"""
trader_client.py — 트레이더 클라이언트

[1단계] UDP 멀티캐스트 시세 수신 + 손실률/지연/순서 뒤바뀜 측정
[2단계] TCP 주문 전송 — 다음 단계에서 추가 예정

실행:  python trader_client.py --name alice
       python trader_client.py --name bob --drop 0.1        (수신 패킷의 10%를 일부러 버림)
       python trader_client.py --name carol --reorder 0.05  (5% 확률로 패킷 순서를 일부러 뒤바꿈)
"""

import argparse
import random
import socket
import struct
import sys
import time

import protocol


def make_multicast_receiver(iface: str) -> socket.socket:
    """멀티캐스트 '수신용' UDP 소켓을 만들고 그룹에 가입한다."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # ── 포트 재사용 옵션 ─────────────────────────────────────────
    # 한 컴퓨터에서 트레이더 여러 명이 '같은 포트 5007'을 동시에 bind 해야 한다.
    # 보통은 한 포트를 한 소켓만 쓸 수 있으므로 "같이 써도 된다"는 옵션을 켠다.
    #  - SO_REUSEADDR : Windows / Linux / Mac 공통
    #  - SO_REUSEPORT : Mac(BSD 계열)에서 여러 소켓이 같은 포트를 공유하려면 필요.
    #                   Windows 에는 이 옵션 자체가 없어서 hasattr 로 확인 후 켠다.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except OSError:
            pass  # 일부 OS 는 상수는 있지만 지원하지 않을 수 있다

    # ── bind ────────────────────────────────────────────────────
    # 포트 5007 로 들어오는 UDP 데이터그램을 받겠다고 OS 에 등록한다.
    # Windows 는 멀티캐스트 주소(224.1.1.1)로 bind 하면 에러가 나므로
    # 모든 OS 에서 동작하도록 "" (= 0.0.0.0, 모든 주소) 로 bind 한다.
    sock.bind(("", protocol.MCAST_PORT))

    # ── 그룹 가입 (IGMP Join) ───────────────────────────────────
    # IP_ADD_MEMBERSHIP 을 하면 OS 가 네트워크에 IGMP Membership Report 를 보낸다.
    # "이 인터페이스에 224.1.1.1 그룹 패킷을 원하는 사람이 있어요"라고 라우터/스위치에 알리는 것.
    # 이걸 안 하면 bind 만 해도 멀티캐스트 패킷이 내 소켓까지 오지 않는다.
    # mreq 구조체 = [그룹 주소 4바이트][내 인터페이스 주소 4바이트]
    mreq = struct.pack("4s4s", socket.inet_aton(protocol.MCAST_GROUP), socket.inet_aton(iface))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    # recvfrom 이 영원히 막혀 있으면 Windows 에서 Ctrl+C 가 안 먹으므로 1초마다 깨어나게 한다.
    sock.settimeout(1.0)
    return sock


class QuoteStats:
    """UDP 시세 수신 통계 (손실률, 지연, 순서 뒤바뀜).

    UDP 는 패킷이 사라져도, 순서가 바뀌어도 아무것도 알려주지 않는다.
    그래서 서버가 붙인 seq(일련번호)와 ts(보낸 시각)로 우리가 직접 계산한다.
    """

    def __init__(self):
        self.first_seq = None    # 처음 받은 seq (클라이언트가 중간에 접속해도 그 이전은 손실로 안 셈)
        self.highest_seq = 0     # 지금까지 받은 가장 큰 seq
        self.received = 0        # 실제로 받은 패킷 수
        self.reordered = 0       # 순서가 뒤바뀌어 도착한 패킷 수
        self.latency_sum = 0.0   # 지연 합계 (평균 계산용)
        self.latency_max = 0.0
        self.last_latency = 0.0

    def on_packet(self, seq: int, ts: float):
        """패킷 하나를 받았을 때 호출. 손실/역순 이벤트가 있으면 설명 문자열을 돌려준다."""
        event = None
        if self.first_seq is None:
            self.first_seq = seq
            self.highest_seq = seq - 1

        if seq > self.highest_seq + 1:
            # 기대한 번호(highest+1)보다 큰 번호가 왔다 → 그 사이 번호들이 빠졌다(손실 또는 아직 안 옴)
            event = f"[손실 감지] seq {self.highest_seq + 1}~{seq - 1} 이(가) 오지 않음"
        if seq < self.highest_seq:
            # 이미 더 큰 번호를 받았는데 작은 번호가 늦게 왔다 → 순서 뒤바뀜
            # (앞에서 '손실'로 셌던 번호가 늦게 도착한 것이므로 손실 수는 자동으로 줄어든다)
            self.reordered += 1
            event = f"[순서 뒤바뀜] seq {seq} 이(가) seq {self.highest_seq} 보다 늦게 도착"

        self.highest_seq = max(self.highest_seq, seq)
        self.received += 1

        # 지연 = 받은 시각 - 서버가 보낸 시각
        latency = protocol.now_ms() - ts
        self.last_latency = latency
        self.latency_sum += latency
        self.latency_max = max(self.latency_max, latency)
        return event

    @property
    def expected(self) -> int:
        """받았어야 하는 패킷 수 = 첫 seq 부터 가장 큰 seq 까지의 개수"""
        if self.first_seq is None:
            return 0
        return self.highest_seq - self.first_seq + 1

    @property
    def lost(self) -> int:
        return max(self.expected - self.received, 0)

    @property
    def loss_rate(self) -> float:
        return self.lost / self.expected if self.expected else 0.0

    @property
    def avg_latency(self) -> float:
        return self.latency_sum / self.received if self.received else 0.0

    def summary(self) -> str:
        return (f"수신 {self.received}/{self.expected}  "
                f"손실 {self.lost}개({self.loss_rate * 100:.1f}%)  "
                f"역순 {self.reordered}개  "
                f"지연 평균 {self.avg_latency:.2f}ms / 최대 {self.latency_max:.2f}ms")


def main():
    parser = argparse.ArgumentParser(description="Mini Exchange 트레이더 클라이언트")
    parser.add_argument("--name", default="trader", help="트레이더 이름")
    parser.add_argument("--iface", default=protocol.MCAST_IFACE,
                        help="멀티캐스트를 받을 인터페이스 IP (서버와 같게 맞추기)")
    parser.add_argument("--drop", type=float, default=0.0,
                        help="받은 패킷을 일부러 버릴 확률 (예: 0.1 = 10%%). 네트워크 손실 흉내")
    # [선택 옵션, 시연용] 같은 컴퓨터 안에서는 패킷 순서가 거의 바뀌지 않아서
    # '순서 뒤바뀜 감지' 기능이 실제로 동작하는 걸 보여주기 어렵다. 그래서 일부러 순서를 바꿔보는 옵션.
    parser.add_argument("--reorder", type=float, default=0.0,
                        help="[선택, 시연용] 패킷을 한 박자 늦게 처리할 확률 (예: 0.05). 순서 뒤바뀜 흉내")
    parser.add_argument("--duration", type=float, default=0,
                        help="이 시간(초)만큼 받고 자동 종료 (0 = 무한, 테스트용)")
    parser.add_argument("--quiet", action="store_true", help="손실/역순 이벤트 출력 끄기")
    args = parser.parse_args()

    sock = make_multicast_receiver(args.iface)
    stats = QuoteStats()
    latest_prices = {}
    held_packet = None        # --reorder 용: 잠시 붙잡아 둔 패킷
    dropped_by_option = 0     # --drop 으로 일부러 버린 개수 (검증용)

    print(f"[{args.name}] {protocol.MCAST_GROUP}:{protocol.MCAST_PORT} 그룹 가입 완료, 시세 수신 대기 "
          f"(drop={args.drop}, reorder={args.reorder})")

    start = time.time()
    last_print = start

    def handle(message):
        nonlocal latest_prices
        event = stats.on_packet(message["seq"], message["ts"])
        latest_prices = message["prices"]
        if event and not args.quiet:
            print(f"[{args.name}] {event}")

    try:
        while True:
            if args.duration and time.time() - start >= args.duration:
                break

            try:
                # recvfrom: 데이터그램 '한 개'를 통째로 받는다.
                # UDP 는 메시지 경계가 보존되므로 한 번의 recvfrom = 서버의 한 번의 sendto.
                # (TCP 는 스트림이라 이렇게 딱 떨어지지 않는다 → 2단계에서 \n 으로 구분할 예정)
                data, sender_addr = sock.recvfrom(protocol.UDP_BUFFER_SIZE)
            except socket.timeout:
                data = None

            if data:
                message = protocol.decode(data)
                if message.get("type") == "QUOTE":
                    # --drop: 네트워크에서 패킷이 사라진 것처럼 그냥 버린다.
                    # 서버는 이 사실을 전혀 모른다 → UDP 에는 재전송(ACK)이 없기 때문.
                    if random.random() < args.drop:
                        dropped_by_option += 1
                    # --reorder: 이번 패킷을 붙잡아 두고, 다음 패킷을 먼저 처리한 뒤 처리한다.
                    elif held_packet is None and random.random() < args.reorder:
                        held_packet = message
                    else:
                        handle(message)
                        if held_packet is not None:
                            handle(held_packet)
                            held_packet = None

            # 1초마다 현재 시세와 통계를 한 줄로 출력
            if time.time() - last_print >= 1.0:
                last_print = time.time()
                price_text = "  ".join(f"{s} {p:,}" for s, p in latest_prices.items())
                print(f"[{args.name}] {price_text} | {stats.summary()}")

    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n[{args.name}] === 최종 결과 ===")
        print(f"[{args.name}] {stats.summary()}")
        if args.drop:
            print(f"[{args.name}] (--drop 으로 일부러 버린 패킷: {dropped_by_option}개)")
        # 그룹 탈퇴(IGMP Leave). 소켓을 닫아도 OS 가 알아서 하지만 명시적으로 보여준다.
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP,
                            struct.pack("4s4s", socket.inet_aton(protocol.MCAST_GROUP),
                                        socket.inet_aton(args.iface)))
        except OSError:
            pass
        sock.close()
        sys.stdout.flush()


if __name__ == "__main__":
    main()
