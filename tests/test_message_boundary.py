"""
tests/test_message_boundary.py — TCP 메시지 경계 실험

TCP 는 '바이트 스트림'이라서 send 한 번이 recv 한 번과 짝이 맞는다는 보장이 없다.
이 스크립트는 일부러 두 가지 극단적인 상황을 만들어서,
protocol.LineReader 의 '\n 으로 자르기'가 그래도 메시지를 정확히 복원하는지 확인한다.

  1) 주문 두 개를 '한 번의 sendall'로 붙여서 보냄   → 서버가 두 주문을 따로 처리해야 함
  2) 주문 하나를 '1바이트씩' 나눠서 보냄            → 서버가 다 모은 뒤 한 주문으로 처리해야 함

실행: 거래소 서버를 먼저 켠 뒤  python tests/test_message_boundary.py
"""

import os
import socket
import sys
import time

# 상위 폴더의 protocol.py 를 import 하기 위해 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import protocol  # noqa: E402


def order(order_id, side, symbol, qty):
    return {"type": "ORDER", "order_id": order_id, "side": side, "symbol": symbol, "qty": qty}


def main():
    sock = socket.create_connection((protocol.TCP_HOST, protocol.TCP_PORT))
    reader = protocol.LineReader(sock)
    protocol.send_message(sock, {"type": "HELLO", "name": "boundary"})
    print("WELCOME:", reader.read_message())

    # 1) 두 메시지를 붙여서 한 번에 보낸다
    two_messages = protocol.encode_line(order(1, "BUY", "SJU", 1)) + protocol.encode_line(order(2, "BUY", "KBK", 1))
    sock.sendall(two_messages)
    first = reader.read_message()
    second = reader.read_message()
    print(f"[붙여 보내기] 응답 2개 수신: 주문번호 {first['order_id']}, {second['order_id']}")

    # 2) 메시지 하나를 1바이트씩 쪼개서 보낸다
    one_message = protocol.encode_line(order(3, "SELL", "SJU", 1))
    for byte in one_message:
        sock.sendall(bytes([byte]))
        time.sleep(0.002)
    reply = reader.read_message()
    print(f"[쪼개 보내기] {len(one_message)}바이트를 {len(one_message)}번 나눠 보냄 → 응답: {reply}")

    sock.close()


if __name__ == "__main__":
    main()
