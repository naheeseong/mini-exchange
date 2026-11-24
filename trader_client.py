"""
trader_client.py — 트레이더 클라이언트

[1단계] UDP 멀티캐스트 시세 수신 + 손실률/지연/순서 뒤바뀜 측정
[2단계] TCP 주문 전송 + 체결(FILL) 수신 + 주문 왕복 시간(RTT) 측정

실행 (거래 모드: 시세는 뒤에서 받고, 키보드로 주문)
       python trader_client.py --name alice
       python trader_client.py --name bob --drop 0.1        (수신 시세의 10%를 일부러 버림)
       python trader_client.py --name carol --reorder 0.05  (5% 확률로 시세 순서를 일부러 뒤바꿈)

실행 (시세 관찰 모드: 1단계처럼 1초마다 시세와 UDP 통계만 출력, TCP 접속 안 함)
       python trader_client.py --name dave --watch --drop 0.1

거래 모드 명령어
       buy SJU 10      SJU 10주 매수
       sell KBK 5      KBK 5주 매도
       balance         잔고 조회 (TCP)
       price           최근 시세 + UDP 통계
       stats           UDP 통계와 TCP RTT 통계 비교
       wait 2          2초 쉬기 (명령을 파일로 넣어 자동 테스트할 때 사용)
       quit            종료

스레드 구성
  - UDP 수신 스레드 : 시세를 계속 받으며 손실/지연/역순 통계를 갱신
  - 메인 스레드     : 키보드 입력을 읽어 TCP 로 주문을 보내고 응답을 기다림
"""

import argparse
import random
import socket
import struct
import sys
import threading
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


class RttStats:
    """TCP 주문 왕복 시간(RTT) 통계.

    RTT = 주문을 보낸 순간 ~ 체결 응답을 받은 순간.
    이 안에는 '요청이 서버까지 가는 시간 + 서버 처리 시간 + 응답이 돌아오는 시간'이 모두 들어 있다.
    UDP 지연(편도)과 달리 내 컴퓨터 시계 하나로만 재므로 서버와 시계가 달라도 정확하다.
    """

    def __init__(self):
        self.rtts = []
        self.fills = 0
        self.rejects = 0

    def add(self, rtt_ms: float):
        self.rtts.append(rtt_ms)

    def summary(self) -> str:
        if not self.rtts:
            return "주문 없음"
        avg = sum(self.rtts) / len(self.rtts)
        return (f"주문 {len(self.rtts)}건 (체결 {self.fills}, 거부 {self.rejects}, 손실 0)  "
                f"RTT 평균 {avg:.2f}ms / 최소 {min(self.rtts):.2f}ms / 최대 {max(self.rtts):.2f}ms")


def format_prices(prices: dict) -> str:
    return "  ".join(f"{s} {p:,}" for s, p in prices.items())


def format_account(cash: int, holdings: dict) -> str:
    held = ", ".join(f"{s} {q}주" for s, q in holdings.items()) or "없음"
    return f"현금 {cash:,}원 | 보유 {held}"


def udp_receive_loop(sock, stats: QuoteStats, shared: dict, args, stop_event: threading.Event,
                     watch_mode: bool):
    """UDP 시세를 계속 받는 루프.

    watch_mode=True  : 1단계처럼 1초마다 시세/통계를 화면에 출력 (메인 스레드에서 실행)
    watch_mode=False : 화면 출력 없이 통계만 갱신 (거래 모드에서 백그라운드 스레드로 실행)
    """
    held_packet = None        # --reorder 용: 잠시 붙잡아 둔 패킷
    start = time.time()
    last_print = start

    def handle(message):
        event = stats.on_packet(message["seq"], message["ts"])
        shared["prices"] = message["prices"]
        if event and watch_mode and not args.quiet:
            print(f"[{args.name}] {event}")

    while not stop_event.is_set():
        if watch_mode and args.duration and time.time() - start >= args.duration:
            break

        try:
            # recvfrom: 데이터그램 '한 개'를 통째로 받는다.
            # UDP 는 메시지 경계가 보존되므로 한 번의 recvfrom = 서버의 한 번의 sendto.
            # (TCP 는 스트림이라 이렇게 딱 떨어지지 않아서 protocol.LineReader 로 \n 마다 자른다)
            data, sender_addr = sock.recvfrom(protocol.UDP_BUFFER_SIZE)
        except socket.timeout:
            data = None
        except OSError:
            break   # 종료 중 소켓이 닫힌 경우

        if data:
            message = protocol.decode(data)
            if message.get("type") == "QUOTE":
                # --drop: 네트워크에서 패킷이 사라진 것처럼 그냥 버린다.
                # 서버는 이 사실을 전혀 모른다 → UDP 에는 재전송(ACK)이 없기 때문.
                if random.random() < args.drop:
                    shared["dropped"] += 1
                # --reorder: 이번 패킷을 붙잡아 두고, 다음 패킷을 먼저 처리한 뒤 처리한다.
                elif held_packet is None and random.random() < args.reorder:
                    held_packet = message
                else:
                    handle(message)
                    if held_packet is not None:
                        handle(held_packet)
                        held_packet = None

        # 시세 관찰 모드에서는 1초마다 현재 시세와 통계를 한 줄로 출력
        if watch_mode and time.time() - last_print >= 1.0:
            last_print = time.time()
            print(f"[{args.name}] {format_prices(shared['prices'])} | {stats.summary()}")


def connect_to_exchange(name: str):
    """거래소에 TCP 로 접속하고 HELLO/WELCOME 을 주고받는다."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # connect: 여기서 OS 가 서버와 3-way handshake(SYN → SYN+ACK → ACK)를 한다.
    # handshake 가 끝나야 connect 가 돌아오므로, 이후에는 '서버가 살아 있고 연결됐다'는 것이 보장된다.
    # (UDP 는 이런 과정이 없어서 서버가 꺼져 있어도 sendto 가 성공해 버린다)
    sock.connect((protocol.TCP_HOST, protocol.TCP_PORT))

    reader = protocol.LineReader(sock)
    protocol.send_message(sock, {"type": "HELLO", "name": name})
    welcome = reader.read_message()
    if welcome is None or welcome.get("type") != "WELCOME":
        raise ConnectionError(f"접속 실패: {welcome}")
    return sock, reader, welcome


def parse_order(words):
    """'buy SJU 10' → ("BUY", "SJU", 10). 형식이 틀리면 None."""
    if len(words) != 3:
        return None
    side, symbol, qty_text = words
    if not qty_text.isdigit():
        return None
    return side.upper(), symbol.upper(), int(qty_text)


def run_trading(args, stats: QuoteStats, shared: dict):
    """거래 모드: 키보드 명령을 읽어 TCP 로 주문하고 결과를 출력한다."""
    tcp_sock, reader, welcome = connect_to_exchange(args.name)
    local_addr = tcp_sock.getsockname()
    print(f"[{args.name}] TCP 접속 완료 {local_addr[0]}:{local_addr[1]} → "
          f"{protocol.TCP_HOST}:{protocol.TCP_PORT}  ({format_account(welcome['cash'], welcome['holdings'])})")
    print(f"[{args.name}] 명령: buy SJU 10 / sell KBK 5 / balance / price / stats / quit")

    rtt_stats = RttStats()
    order_id = 0

    try:
        for line in sys.stdin:
            words = line.strip().split()
            if not words:
                continue
            command = words[0].lower()

            if command in ("quit", "exit", "q"):
                break

            elif command == "wait" and len(words) == 2:
                time.sleep(float(words[1]))

            elif command == "price":
                print(f"[{args.name}] 시세 {format_prices(shared['prices'])}")
                print(f"[{args.name}] UDP  {stats.summary()}")

            elif command == "stats":
                print(f"[{args.name}] UDP 시세 : {stats.summary()}")
                print(f"[{args.name}] TCP 주문 : {rtt_stats.summary()}")

            elif command == "balance":
                protocol.send_message(tcp_sock, {"type": "BALANCE"})
                reply = reader.read_message()
                if reply is None:
                    print(f"[{args.name}] 서버 연결이 끊겼습니다.")
                    break
                print(f"[{args.name}] 잔고 {format_account(reply['cash'], reply['holdings'])}")

            elif command in ("buy", "sell"):
                order = parse_order(words)
                if order is None:
                    print(f"[{args.name}] 형식: buy SJU 10")
                    continue
                side, symbol, qty = order
                order_id += 1
                seen_price = shared["prices"].get(symbol)   # 내가 UDP 로 마지막에 본 가격

                # RTT 측정: 보내기 직전 시각 ~ 응답을 다 받은 시각.
                # perf_counter 는 시간 '간격'을 재는 데 가장 정밀한 시계다.
                sent_at = time.perf_counter()
                protocol.send_message(tcp_sock, {"type": "ORDER", "order_id": order_id,
                                                 "side": side, "symbol": symbol, "qty": qty})
                reply = reader.read_message()   # 응답(\n 까지)이 올 때까지 기다린다
                rtt_ms = (time.perf_counter() - sent_at) * 1000

                if reply is None:
                    print(f"[{args.name}] 서버 연결이 끊겼습니다.")
                    break
                rtt_stats.add(rtt_ms)

                if reply["type"] == "FILL":
                    rtt_stats.fills += 1
                    seen = f"{seen_price:,}" if seen_price else "-"
                    print(f"[{args.name}] FILL #{reply['order_id']} {reply['side']} {reply['symbol']} "
                          f"{reply['qty']}주 @ {reply['price']:,}원 (내가 본 시세 {seen}) "
                          f"RTT {rtt_ms:.2f}ms | {format_account(reply['cash'], reply['holdings'])}")
                else:
                    rtt_stats.rejects += 1
                    print(f"[{args.name}] REJECT #{reply.get('order_id')} {reply.get('reason')} "
                          f"RTT {rtt_ms:.2f}ms")

            else:
                print(f"[{args.name}] 알 수 없는 명령: {line.strip()}")

    except KeyboardInterrupt:
        pass
    finally:
        # close: OS 가 FIN 을 보내 연결을 정상 종료한다 → 서버의 recv 가 b"" 를 받고 끝난다.
        tcp_sock.close()

    return rtt_stats


def main():
    parser = argparse.ArgumentParser(description="Mini Exchange 트레이더 클라이언트")
    parser.add_argument("--name", default="trader", help="트레이더 이름 (같은 이름으로 다시 접속하면 계좌 유지)")
    parser.add_argument("--iface", default=protocol.MCAST_IFACE,
                        help="멀티캐스트를 받을 인터페이스 IP (서버와 같게 맞추기)")
    parser.add_argument("--drop", type=float, default=0.0,
                        help="받은 시세 패킷을 일부러 버릴 확률 (예: 0.1 = 10%%). 네트워크 손실 흉내")
    # [선택 옵션, 시연용] 같은 컴퓨터 안에서는 패킷 순서가 거의 바뀌지 않아서
    # '순서 뒤바뀜 감지' 기능이 실제로 동작하는 걸 보여주기 어렵다. 그래서 일부러 순서를 바꿔보는 옵션.
    parser.add_argument("--reorder", type=float, default=0.0,
                        help="[선택, 시연용] 패킷을 한 박자 늦게 처리할 확률 (예: 0.05). 순서 뒤바뀜 흉내")
    parser.add_argument("--watch", action="store_true",
                        help="시세 관찰 모드: TCP 주문 없이 UDP 시세와 통계만 1초마다 출력")
    parser.add_argument("--duration", type=float, default=0,
                        help="[--watch 전용] 이 시간(초)만큼 받고 자동 종료 (0 = 무한, 테스트용)")
    parser.add_argument("--quiet", action="store_true", help="[--watch 전용] 손실/역순 이벤트 출력 끄기")
    args = parser.parse_args()

    udp_sock = make_multicast_receiver(args.iface)
    stats = QuoteStats()
    shared = {"prices": {}, "dropped": 0}   # UDP 스레드와 메인 스레드가 함께 보는 값
    stop_event = threading.Event()
    rtt_stats = None

    print(f"[{args.name}] {protocol.MCAST_GROUP}:{protocol.MCAST_PORT} 그룹 가입 완료, 시세 수신 시작 "
          f"(drop={args.drop}, reorder={args.reorder})")

    try:
        if args.watch:
            # 1단계와 같은 시세 관찰 모드: 메인 스레드가 직접 UDP 를 받는다.
            udp_receive_loop(udp_sock, stats, shared, args, stop_event, watch_mode=True)
        else:
            # 거래 모드: UDP 는 백그라운드 스레드가 받고, 메인 스레드는 키보드 입력과 TCP 를 맡는다.
            # 두 일을 한 스레드에서 하면 input() 이나 recv() 에서 기다리는 동안 다른 쪽이 멈춰버린다.
            udp_thread = threading.Thread(target=udp_receive_loop, daemon=True,
                                          args=(udp_sock, stats, shared, args, stop_event, False))
            udp_thread.start()
            rtt_stats = run_trading(args, stats, shared)
    except KeyboardInterrupt:
        pass
    except ConnectionRefusedError:
        # TCP 는 connect 할 때 서버가 없으면 바로 알 수 있다 (상대 OS 가 RST 로 거절).
        print(f"[{args.name}] 거래소 서버({protocol.TCP_HOST}:{protocol.TCP_PORT})에 접속할 수 없습니다. "
              f"서버를 먼저 실행하세요.")
    finally:
        stop_event.set()
        print(f"\n[{args.name}] === 최종 결과 ===")
        print(f"[{args.name}] UDP 시세 : {stats.summary()}")
        if args.drop:
            print(f"[{args.name}]            (--drop 으로 일부러 버린 패킷: {shared['dropped']}개)")
        if rtt_stats is not None:
            print(f"[{args.name}] TCP 주문 : {rtt_stats.summary()}")
        # 그룹 탈퇴(IGMP Leave). 소켓을 닫아도 OS 가 알아서 하지만 명시적으로 보여준다.
        try:
            udp_sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP,
                                struct.pack("4s4s", socket.inet_aton(protocol.MCAST_GROUP),
                                            socket.inet_aton(args.iface)))
        except OSError:
            pass
        udp_sock.close()
        sys.stdout.flush()


if __name__ == "__main__":
    main()
