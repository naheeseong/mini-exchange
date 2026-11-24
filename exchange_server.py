"""
exchange_server.py — 거래소 서버

[1단계] 시세 생성 + UDP 멀티캐스트 송신
[2단계] TCP 주문 서버 (멀티스레드) — 다음 단계에서 추가 예정

실행:  python exchange_server.py
       python exchange_server.py --iface 127.0.0.1   (와이파이 없이 내 컴퓨터 안에서만 테스트할 때)
"""

import argparse
import random
import socket
import threading
import time

import protocol

# ---------------------------------------------------------------------------
# 공유 데이터: 현재 시세
# ---------------------------------------------------------------------------
# 시세 스레드가 가격을 바꾸고, 2단계의 TCP 주문 스레드들이 이 가격으로 체결한다.
# 여러 스레드가 같은 dict 를 동시에 읽고 쓰면 꼬일 수 있으므로 Lock(자물쇠)으로 보호한다.
prices = dict(protocol.INITIAL_PRICES)
prices_lock = threading.Lock()


def next_price(price: int) -> int:
    """랜덤 워크: 직전 가격에서 아주 조금(평균 0, 표준편차 0.2%) 랜덤하게 움직인다.

    실제 주식처럼 '이전 가격'을 기준으로 오르락내리락하므로 그래프가 자연스럽다.
    가격은 10원 단위로 맞추고, 0원 이하로 떨어지지 않게 한다.
    """
    change_rate = random.gauss(0, 0.002)
    new_price = price * (1 + change_rate)
    new_price = round(new_price / 10) * 10
    return max(new_price, 10)


def make_multicast_sender(iface: str) -> socket.socket:
    """멀티캐스트 '송신용' UDP 소켓을 만든다."""

    # SOCK_DGRAM = UDP. 연결(handshake) 없이 데이터그램(독립된 편지 한 통)을 바로 던진다.
    # TCP 처럼 connect/accept 과정이 없으므로 받는 사람이 몇 명이든, 0명이든 상관없이 보낼 수 있다.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # TTL = 1: 라우터를 넘지 않고 내 LAN 안에서만 퍼지게 한다.
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, protocol.MCAST_TTL)

    # LOOP = 1: 내가 보낸 멀티캐스트를 '같은 컴퓨터'의 수신자도 받을 수 있게 한다.
    # 서버와 클라이언트를 한 노트북에서 돌리므로 꼭 켜야 한다. (Mac/Windows 기본값이 다를 수 있어 명시)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

    # 어떤 네트워크 인터페이스로 내보낼지 지정. 0.0.0.0 이면 OS 가 고른다.
    # Mac 에서 와이파이가 꺼져 있으면 기본 경로가 없어 "No route to host" 가 날 수 있는데,
    # 이때 --iface 127.0.0.1 로 루프백(내 컴퓨터 내부)을 지정하면 된다.
    if iface != "0.0.0.0":
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(iface))

    return sock


def quote_broadcast_loop(sock: socket.socket, interval: float, stop_event: threading.Event):
    """0.1초마다 가격을 바꾸고, 그 결과를 멀티캐스트 그룹으로 방송한다."""
    seq = 0
    destination = (protocol.MCAST_GROUP, protocol.MCAST_PORT)

    while not stop_event.is_set():
        # 1) 가격 갱신 (Lock 으로 보호)
        with prices_lock:
            for symbol in prices:
                prices[symbol] = next_price(prices[symbol])
            snapshot = dict(prices)

        # 2) 메시지 만들기: seq(일련번호)와 ts(보낸 시각)를 붙인다.
        #    UDP 는 "몇 번째 패킷인지", "언제 보냈는지"를 알려주지 않기 때문에 응용 계층에서 직접 붙인다.
        seq += 1
        message = {
            "type": "QUOTE",
            "seq": seq,
            "ts": protocol.now_ms(),
            "prices": snapshot,
        }

        # 3) sendto: 연결이 없으므로 매번 '받는 주소'를 같이 적어서 보낸다.
        #    목적지가 그룹 주소(224.1.1.1)이므로 이 한 번의 호출로 가입자 전원에게 전달된다.
        #    받는 쪽이 없거나 패킷이 사라져도 sendto 는 에러 없이 성공한다 → UDP 는 '보내고 잊는다'.
        sock.sendto(protocol.encode(message), destination)

        if seq % 50 == 0:  # 5초에 한 번 서버 화면에 현재 상태 출력
            price_text = "  ".join(f"{s} {p:,}" for s, p in snapshot.items())
            print(f"[UDP 송신] seq={seq:<6} {price_text}")

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="Mini Exchange 거래소 서버")
    parser.add_argument("--iface", default=protocol.MCAST_IFACE,
                        help="멀티캐스트를 내보낼 인터페이스 IP (기본: OS 자동 선택)")
    parser.add_argument("--interval", type=float, default=protocol.TICK_INTERVAL,
                        help="시세 방송 간격(초), 기본 0.1")
    args = parser.parse_args()

    stop_event = threading.Event()
    sender = make_multicast_sender(args.iface)

    print(f"[거래소] UDP 시세 멀티캐스트 시작 → {protocol.MCAST_GROUP}:{protocol.MCAST_PORT} "
          f"(간격 {args.interval}초)")
    print("[거래소] 종료하려면 Ctrl+C")

    # 시세 방송은 별도 스레드에서 돌린다.
    # 2단계에서 메인 스레드는 TCP accept() 로 트레이더 접속을 기다리게 될 것이다.
    quote_thread = threading.Thread(
        target=quote_broadcast_loop, args=(sender, args.interval, stop_event), daemon=True
    )
    quote_thread.start()

    try:
        # 1단계에서는 메인 스레드가 할 일이 없으므로 그냥 기다린다.
        # (time.sleep 을 짧게 반복해야 Windows 에서도 Ctrl+C 가 바로 먹는다)
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[거래소] 종료합니다.")
    finally:
        stop_event.set()
        quote_thread.join(timeout=1)
        sender.close()


if __name__ == "__main__":
    main()
