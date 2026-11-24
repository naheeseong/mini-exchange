"""
exchange_server.py — 거래소 서버

[1단계] 시세 생성 + UDP 멀티캐스트 송신
[2단계] TCP 주문 서버 (멀티스레드) + 트레이더별 잔고/보유 주식 관리

실행:  python exchange_server.py
       python exchange_server.py --iface 127.0.0.1   (와이파이 없이 내 컴퓨터 안에서만 테스트할 때)

스레드 구성
  - 시세 스레드 1개      : 0.1초마다 가격 변경 + UDP 멀티캐스트 송신
  - 메인 스레드          : TCP accept() 로 새 트레이더 접속을 기다림
  - 트레이더 스레드 N개  : 접속한 트레이더마다 1개씩, 그 트레이더의 주문만 처리
"""

import argparse
import random
import socket
import threading
import time
from collections import deque

import protocol

# ---------------------------------------------------------------------------
# 공유 데이터: 현재 시세
# ---------------------------------------------------------------------------
# 시세 스레드가 가격을 바꾸고, 2단계의 TCP 주문 스레드들이 이 가격으로 체결한다.
# 여러 스레드가 같은 dict 를 동시에 읽고 쓰면 꼬일 수 있으므로 Lock(자물쇠)으로 보호한다.
prices = dict(protocol.INITIAL_PRICES)
prices_lock = threading.Lock()

# 트레이더 계좌: {"alice": {"cash": 10000000, "holdings": {"SJU": 10}}, ...}
# 여러 트레이더 스레드가 동시에 접근하므로 역시 Lock 으로 보호한다.
# 같은 이름으로 다시 접속하면 예전 계좌를 그대로 이어서 쓴다.
accounts = {}
accounts_lock = threading.Lock()

# 최근 체결 내역 (대시보드에서 STATUS 요청으로 가져감). 최근 50건만 보관한다.
recent_fills = deque(maxlen=50)
# 거래소 전체 TCP 통계
tcp_stats = {"connected": 0, "orders": 0, "fills": 0, "rejects": 0}

# 여러 스레드가 동시에 print 하면 한 줄에 두 메시지가 섞여 찍힐 수 있다.
# (print 는 '내용'과 '줄바꿈'을 따로 쓰기 때문) → 출력도 Lock 으로 한 줄씩 순서를 정한다.
print_lock = threading.Lock()


def log(text: str):
    with print_lock:
        print(text, flush=True)


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
            log(f"[UDP 송신] seq={seq:<6} {price_text}")

        time.sleep(interval)


# ===========================================================================
# 2단계: TCP 주문 서버
# ===========================================================================

def make_tcp_server() -> socket.socket:
    """주문을 받을 TCP '듣기(listening)' 소켓을 만든다."""

    # SOCK_STREAM = TCP. 연결 지향, 신뢰성(재전송), 순서 보장, 바이트 스트림.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 서버를 껐다 바로 켜면 이전 연결이 TIME_WAIT 상태로 남아 있어 bind 가 실패할 수 있다.
    # SO_REUSEADDR 로 "그래도 이 포트를 다시 쓰겠다"고 알려준다.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    sock.bind((protocol.TCP_HOST, protocol.TCP_PORT))

    # listen: 이제부터 연결 요청을 받겠다. 클라이언트가 connect() 하면 OS 가 대신
    # 3-way handshake(SYN → SYN+ACK → ACK)를 끝내고, 완성된 연결을 대기열(backlog)에 넣어둔다.
    sock.listen(protocol.TCP_BACKLOG)

    # accept() 가 영원히 막혀 있으면 Windows 에서 Ctrl+C 가 안 먹으므로 1초마다 깨어나게 한다.
    sock.settimeout(1.0)
    return sock


def account_view(name: str) -> dict:
    """계좌 정보를 응답에 넣기 좋은 형태로 복사해서 돌려준다. (accounts_lock 을 잡은 상태에서 호출)"""
    account = accounts[name]
    return {"cash": account["cash"], "holdings": dict(account["holdings"])}


def execute_order(name: str, order: dict) -> dict:
    """주문을 '현재 시세'로 즉시 체결하고 FILL 또는 REJECT 메시지를 만든다."""
    order_id = order.get("order_id")
    side = str(order.get("side", "")).upper()
    symbol = str(order.get("symbol", "")).upper()
    qty = order.get("qty")

    # 1) 주문 내용 검사
    if side not in ("BUY", "SELL"):
        return {"type": "REJECT", "order_id": order_id, "reason": "side 는 BUY 또는 SELL"}
    if symbol not in protocol.INITIAL_PRICES:
        return {"type": "REJECT", "order_id": order_id, "reason": f"없는 종목: {symbol}"}
    if not isinstance(qty, int) or qty <= 0:
        return {"type": "REJECT", "order_id": order_id, "reason": "수량은 1 이상의 정수"}

    # 2) 체결 가격 = 지금 이 순간의 시세
    with prices_lock:
        price = prices[symbol]
    amount = price * qty

    # 3) 잔고 확인 후 계좌 반영. 확인과 반영을 한 Lock 안에서 해야
    #    두 스레드가 동시에 같은 돈을 쓰는 일이 생기지 않는다.
    with accounts_lock:
        account = accounts[name]
        holdings = account["holdings"]
        if side == "BUY":
            if account["cash"] < amount:
                return {"type": "REJECT", "order_id": order_id,
                        "reason": f"현금 부족 (필요 {amount:,}원, 보유 {account['cash']:,}원)"}
            account["cash"] -= amount
            holdings[symbol] = holdings.get(symbol, 0) + qty
        else:
            if holdings.get(symbol, 0) < qty:
                return {"type": "REJECT", "order_id": order_id,
                        "reason": f"보유 주식 부족 ({symbol} {holdings.get(symbol, 0)}주 보유)"}
            account["cash"] += amount
            holdings[symbol] -= qty
            if holdings[symbol] == 0:
                del holdings[symbol]

        fill = {"type": "FILL", "order_id": order_id, "side": side, "symbol": symbol,
                "qty": qty, "price": price}
        fill.update(account_view(name))
    return fill


def handle_trader(conn: socket.socket, addr, stop_event: threading.Event):
    """트레이더 한 명과의 TCP 연결을 처리하는 스레드 함수."""
    reader = protocol.LineReader(conn)
    name = None
    connected = False
    peer = f"{addr[0]}:{addr[1]}"   # 클라이언트 IP:포트 (포트는 OS 가 임시로 고른 번호)

    try:
        # 첫 메시지는 반드시 HELLO (내 이름). 이름으로 계좌를 찾는다.
        hello = reader.read_message()
        if hello is None or hello.get("type") != "HELLO" or not hello.get("name"):
            protocol.send_message(conn, {"type": "ERROR", "reason": "첫 메시지는 HELLO 여야 합니다"})
            return
        name = str(hello["name"])
        with accounts_lock:
            tcp_stats["connected"] += 1
        connected = True

        with accounts_lock:
            if name not in accounts:
                accounts[name] = {"cash": protocol.INITIAL_CASH, "holdings": {}}
            welcome = {"type": "WELCOME", "name": name}
            welcome.update(account_view(name))
        protocol.send_message(conn, welcome)
        log(f"[TCP 접속] {name} ({peer})  현금 {welcome['cash']:,}원")

        # 이후로는 주문이 올 때마다 처리하고 바로 응답한다.
        while not stop_event.is_set():
            message = reader.read_message()
            if message is None:
                break   # 클라이언트가 연결을 닫음

            msg_type = message.get("type")
            if msg_type == "ORDER":
                reply = execute_order(name, message)
                with accounts_lock:
                    tcp_stats["orders"] += 1
                    if reply["type"] == "FILL":
                        tcp_stats["fills"] += 1
                        recent_fills.append({"time": time.strftime("%H:%M:%S"), "trader": name,
                                             "side": reply["side"], "symbol": reply["symbol"],
                                             "qty": reply["qty"], "price": reply["price"]})
                    else:
                        tcp_stats["rejects"] += 1
                if reply["type"] == "FILL":
                    log(f"[체결] {name:<8} {reply['side']:<4} {reply['symbol']} {reply['qty']:>4}주 "
                          f"@ {reply['price']:,}원  (주문번호 {reply['order_id']})")
                else:
                    log(f"[거부] {name:<8} 주문번호 {reply['order_id']}: {reply['reason']}")
            elif msg_type == "BALANCE":
                with accounts_lock:
                    reply = {"type": "BALANCE"}
                    reply.update(account_view(name))
            elif msg_type == "STATUS":
                # 대시보드용: 거래소 전체 현황 (최근 체결, 접속자 수, 트레이더별 잔고)
                with accounts_lock:
                    reply = {"type": "STATUS",
                             "fills": list(recent_fills),
                             "stats": dict(tcp_stats),
                             "accounts": {n: account_view(n) for n in accounts}}
            else:
                reply = {"type": "ERROR", "reason": f"알 수 없는 메시지: {msg_type}"}

            # TCP 는 신뢰성을 보장하므로, 여기서 보낸 응답은 연결이 살아 있는 한
            # 반드시 '보낸 순서대로' 클라이언트에 도착한다. (빠진 조각은 OS 가 알아서 재전송)
            protocol.send_message(conn, reply)

    except (ConnectionError, OSError) as e:
        # 클라이언트 프로그램이 강제 종료되면 RST 를 받아 ConnectionResetError 가 날 수 있다.
        log(f"[TCP 오류] {name or peer}: {e}")
    except ValueError as e:
        log(f"[TCP 오류] {name or peer}: 잘못된 메시지 형식 ({e})")
    finally:
        conn.close()
        if connected:
            with accounts_lock:
                tcp_stats["connected"] -= 1
        log(f"[TCP 종료] {name or peer} 연결 끊김")


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

    # 시세 방송은 별도 스레드에서 돌린다. 메인 스레드는 아래에서 TCP accept() 를 담당한다.
    quote_thread = threading.Thread(
        target=quote_broadcast_loop, args=(sender, args.interval, stop_event), daemon=True
    )
    quote_thread.start()

    server = make_tcp_server()
    print(f"[거래소] TCP 주문 서버 대기 중 → {protocol.TCP_HOST}:{protocol.TCP_PORT}")
    print("[거래소] 종료하려면 Ctrl+C")

    try:
        while True:
            try:
                # accept: 대기열에서 handshake 가 끝난 연결 하나를 꺼낸다.
                # 돌려받는 conn 은 '이 트레이더 전용' 새 소켓이고,
                # server 소켓은 계속 다음 접속을 기다리는 데만 쓰인다.
                conn, addr = server.accept()
            except socket.timeout:
                continue

            # accept 로 받은 소켓은 OS 에 따라 server 의 timeout 을 물려받을 수 있으므로 blocking 으로 되돌린다.
            conn.settimeout(None)

            # 트레이더마다 스레드를 하나씩 만든다.
            # 한 트레이더가 recv() 에서 기다리고 있어도 다른 트레이더의 주문은 다른 스레드가 처리한다.
            thread = threading.Thread(target=handle_trader, args=(conn, addr, stop_event), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\n[거래소] 종료합니다.")
    finally:
        stop_event.set()
        server.close()
        quote_thread.join(timeout=1)
        sender.close()


if __name__ == "__main__":
    main()
