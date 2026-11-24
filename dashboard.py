"""
dashboard.py — Mini Exchange 실시간 대시보드 (Streamlit)

실행:  (거래소 서버를 먼저 켠 뒤)
       streamlit run dashboard.py
       → 브라우저에서 http://localhost:8501 이 열린다.

대시보드 자신도 하나의 '트레이더'처럼 동작한다.
  - UDP : 멀티캐스트 그룹에 가입해서 시세를 직접 받는다 → 가격 차트, 손실률, 지연
  - TCP : 거래소에 접속해서 1초마다 STATUS(현황)를 요청하고, 사이드바에서 주문도 보낸다
          → 요청마다 RTT 를 재고, 거래소 전체의 최근 체결 내역을 받아온다
그래서 화면에 보이는 UDP/TCP 숫자는 모두 '이 대시보드가 실제로 주고받은 패킷'으로 계산한 값이다.

Streamlit 은 화면을 그릴 때마다 이 파일을 위에서부터 다시 실행한다.
소켓과 수신 스레드가 매번 새로 만들어지면 안 되므로 @st.cache_resource 로 딱 한 번만 만든다.
"""

import random
import socket
import threading
import time
from collections import deque
from datetime import datetime

import altair as alt          # Streamlit 설치 시 함께 설치되는 차트 라이브러리
import pandas as pd
import streamlit as st

import protocol
from trader_client import QuoteStats, make_multicast_receiver

HISTORY_SIZE = 300        # 차트에 보여줄 시세 개수 (0.1초 × 300 = 최근 30초)
RECENT_WINDOW = 100       # '최근 손실률'을 계산할 seq 범위 (최근 100개 ≈ 10초)


class ExchangeMonitor:
    """대시보드 뒤에서 계속 돌아가는 네트워크 담당 객체.

    - UDP 수신 스레드가 시세를 받아 history 와 stats 를 갱신한다.
    - TCP 요청(request)은 화면 쪽에서 호출하며, 요청마다 RTT 를 기록한다.
    """

    def __init__(self, iface: str = protocol.MCAST_IFACE):
        self.lock = threading.Lock()

        # ---- UDP 쪽 상태 ----
        self.udp_sock = make_multicast_receiver(iface)
        self.stats = QuoteStats()
        self.drop = 0.0                                  # 사이드바 슬라이더로 바꾸는 손실 시뮬레이션 확률
        self.dropped = 0
        self.history = deque(maxlen=HISTORY_SIZE)        # 가격/지연 기록
        self.recent_seqs = deque(maxlen=RECENT_WINDOW)   # 최근 받은 seq (최근 손실률 계산용)

        # ---- TCP 쪽 상태 ----
        self.tcp_sock = None
        self.reader = None
        self.tcp_lock = threading.Lock()   # TCP 는 요청 → 응답 순서가 섞이면 안 되므로 한 번에 하나씩
        self.rtt_history = deque(maxlen=HISTORY_SIZE)
        self.order_id = 0

        thread = threading.Thread(target=self._udp_loop, daemon=True)
        thread.start()

    # ------------------------------------------------------------------
    # UDP: 시세 수신 (백그라운드 스레드)
    # ------------------------------------------------------------------
    def _udp_loop(self):
        while True:
            try:
                data, _ = self.udp_sock.recvfrom(protocol.UDP_BUFFER_SIZE)
            except socket.timeout:
                continue
            message = protocol.decode(data)
            if message.get("type") != "QUOTE":
                continue

            # 손실 시뮬레이션: 받은 데이터그램을 그냥 버린다. 서버는 이 사실을 모른다.
            if random.random() < self.drop:
                with self.lock:
                    self.dropped += 1
                continue

            with self.lock:
                self.stats.on_packet(message["seq"], message["ts"])
                self.recent_seqs.append(message["seq"])
                row = {"time": datetime.now(), "seq": message["seq"],
                       "latency_ms": self.stats.last_latency}
                row.update(message["prices"])
                self.history.append(row)

    def recent_loss_rate(self) -> float:
        """최근 RECENT_WINDOW 개 범위에서의 손실률 (누적 손실률보다 슬라이더 변화가 빨리 보인다)"""
        if len(self.recent_seqs) < 2:
            return 0.0
        span = max(self.recent_seqs) - min(self.recent_seqs) + 1
        return 1 - len(self.recent_seqs) / span

    # ------------------------------------------------------------------
    # TCP: 요청 1개 보내고 응답 1개 받기 (RTT 측정)
    # ------------------------------------------------------------------
    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3.0)                 # 서버가 응답이 없을 때 화면이 멈추지 않도록
        sock.connect((protocol.TCP_HOST, protocol.TCP_PORT))   # 3-way handshake
        reader = protocol.LineReader(sock)
        protocol.send_message(sock, {"type": "HELLO", "name": "dashboard"})
        reader.read_message()                # WELCOME
        self.tcp_sock, self.reader = sock, reader

    def request(self, message: dict, kind: str):
        """TCP 로 요청을 보내고 응답을 돌려준다. 실패하면 None. (kind: 'STATUS' 또는 'ORDER')"""
        with self.tcp_lock:
            try:
                if self.tcp_sock is None:
                    self._connect()
                sent_at = time.perf_counter()
                protocol.send_message(self.tcp_sock, message)
                reply = self.reader.read_message()
                rtt_ms = (time.perf_counter() - sent_at) * 1000
                if reply is None:
                    raise ConnectionError("서버가 연결을 닫음")
            except (OSError, ConnectionError, ValueError):
                # 서버가 꺼졌거나 재시작됨 → 다음 요청 때 다시 접속한다
                if self.tcp_sock is not None:
                    self.tcp_sock.close()
                self.tcp_sock = None
                return None
            self.rtt_history.append({"time": datetime.now(), "rtt_ms": rtt_ms, "kind": kind})
            return reply

    def send_order(self, side: str, symbol: str, qty: int):
        self.order_id += 1
        return self.request({"type": "ORDER", "order_id": self.order_id,
                             "side": side, "symbol": symbol, "qty": qty}, kind="ORDER")


def line_chart(data: pd.DataFrame, y: str, title: str, height: int = 200):
    """y 축이 0부터 시작하지 않는 선 그래프 (가격 변화가 납작하게 보이지 않도록)"""
    chart = (alt.Chart(data, height=height, title=title)
             .mark_line()
             .encode(x=alt.X("time:T", title=None, axis=alt.Axis(format="%H:%M:%S")),
                     y=alt.Y(f"{y}:Q", title=None, scale=alt.Scale(zero=False))))
    st.altair_chart(chart, use_container_width=True)


@st.cache_resource
def get_monitor() -> ExchangeMonitor:
    """브라우저 새로고침/재실행에도 소켓과 스레드는 딱 한 번만 만든다."""
    return ExchangeMonitor()


# ======================================================================
# 화면 구성
# ======================================================================
st.set_page_config(page_title="Mini Exchange", layout="wide")
monitor = get_monitor()

st.title("Mini Exchange — TCP/UDP 실시간 거래 대시보드")
st.caption(f"시세: UDP 멀티캐스트 {protocol.MCAST_GROUP}:{protocol.MCAST_PORT}  ·  "
           f"주문/체결: TCP {protocol.TCP_HOST}:{protocol.TCP_PORT}")

# ---------------- 사이드바: 손실 시뮬레이션 + 주문 ----------------
with st.sidebar:
    st.header("UDP 손실 시뮬레이션")
    monitor.drop = st.slider("받은 시세를 일부러 버릴 확률", 0.0, 0.5, monitor.drop, 0.05,
                             help="트레이더의 --drop 옵션과 같은 기능. 오른쪽 UDP 손실률이 따라 변한다.")
    st.caption("UDP 는 재전송이 없어서 버린 시세는 영원히 사라진다. "
               "TCP 주문은 이 슬라이더와 상관없이 항상 도착한다.")

    st.header("TCP 주문")
    symbol = st.selectbox("종목", list(protocol.INITIAL_PRICES))
    qty = st.number_input("수량", min_value=1, value=10, step=1)
    buy_col, sell_col = st.columns(2)
    side = None
    if buy_col.button("매수 (buy)", use_container_width=True):
        side = "BUY"
    if sell_col.button("매도 (sell)", use_container_width=True):
        side = "SELL"
    if side:
        reply = monitor.send_order(side, symbol, int(qty))
        if reply is None:
            st.error("거래소 서버에 접속할 수 없습니다.")
        elif reply["type"] == "FILL":
            st.success(f"FILL {reply['side']} {reply['symbol']} {reply['qty']}주 @ {reply['price']:,}원 "
                       f"(RTT {monitor.rtt_history[-1]['rtt_ms']:.2f}ms)")
        else:
            st.warning(f"REJECT: {reply['reason']}")


# ---------------- 1초마다 다시 그리는 영역 ----------------
@st.fragment(run_every=1.0)
def live_view():
    # TCP 로 거래소 현황 요청 (이 요청의 RTT 도 기록된다)
    status = monitor.request({"type": "STATUS"}, kind="STATUS")

    with monitor.lock:
        history = pd.DataFrame(list(monitor.history))
        stats = monitor.stats
        udp = {
            "received": stats.received, "expected": stats.expected, "lost": stats.lost,
            "loss_rate": stats.loss_rate, "recent_loss": monitor.recent_loss_rate(),
            "reordered": stats.reordered, "avg_latency": stats.avg_latency,
            "max_latency": stats.latency_max, "dropped": monitor.dropped,
        }
    rtt = pd.DataFrame(list(monitor.rtt_history))

    if history.empty:
        st.warning("아직 UDP 시세를 받지 못했습니다. 거래소 서버(exchange_server.py)가 실행 중인지 확인하세요.")
        return
    if status is None:
        st.error("TCP 로 거래소에 접속할 수 없습니다. 서버가 켜져 있는지 확인하세요.")

    # ---- 1. 현재가 ----
    latest = history.iloc[-1]
    cols = st.columns(len(protocol.INITIAL_PRICES))
    for col, (sym, start_price) in zip(cols, protocol.INITIAL_PRICES.items()):
        change = (latest[sym] - start_price) / start_price * 100
        col.metric(sym, f"{int(latest[sym]):,}원", f"{change:+.2f}% (시작가 대비)")

    # ---- 2. 실시간 가격 차트 (UDP 로 받은 시세) ----
    st.subheader("실시간 가격 (최근 30초, UDP 시세)")
    cols = st.columns(len(protocol.INITIAL_PRICES))
    for col, sym in zip(cols, protocol.INITIAL_PRICES):
        with col:
            line_chart(history, sym, f"{sym} (원)")

    # ---- 3. UDP vs TCP 나란히 비교 ----
    st.subheader("UDP 시세 vs TCP 주문 — 실제 측정값 비교")
    udp_col, tcp_col = st.columns(2)

    with udp_col:
        st.markdown("#### UDP (시세, 1:N 멀티캐스트)")
        a, b, c = st.columns(3)
        a.metric("누적 손실률", f"{udp['loss_rate'] * 100:.1f}%")
        b.metric("최근 10초 손실률", f"{udp['recent_loss'] * 100:.1f}%")
        c.metric("평균 지연(편도)", f"{udp['avg_latency']:.2f}ms")
        st.caption(f"수신 {udp['received']}/{udp['expected']} · 손실 {udp['lost']}개 · "
                   f"순서 뒤바뀜 {udp['reordered']}개 · 최대 지연 {udp['max_latency']:.2f}ms · "
                   f"시뮬레이션으로 버린 패킷 {udp['dropped']}개")
        line_chart(history, "latency_ms", "시세 패킷별 지연 (ms)", height=180)

    with tcp_col:
        st.markdown("#### TCP (주문/조회, 1:1 연결)")
        a, b, c = st.columns(3)
        if rtt.empty:
            a.metric("평균 RTT(왕복)", "-")
            b.metric("최근 RTT", "-")
            c.metric("요청 수", "0")
        else:
            a.metric("평균 RTT(왕복)", f"{rtt['rtt_ms'].mean():.2f}ms")
            b.metric("최근 RTT", f"{rtt['rtt_ms'].iloc[-1]:.2f}ms")
            c.metric("요청 수 / 손실", f"{len(rtt)} / 0")
            orders = int((rtt["kind"] == "ORDER").sum())
            st.caption(f"STATUS 조회 {len(rtt) - orders}건 + 주문 {orders}건 · 최대 RTT {rtt['rtt_ms'].max():.2f}ms · "
                       "TCP 는 OS 가 ACK 와 재전송으로 빠진 조각을 다시 보내므로 응용 프로그램이 보는 손실은 항상 0")
            line_chart(rtt, "rtt_ms", "TCP 요청별 왕복 시간 RTT (ms)", height=180)

    comparison = pd.DataFrame({
        "항목": ["연결 설정", "전달 보장", "순서 보장", "메시지 경계", "전송 대상",
               "측정한 손실", "측정한 지연"],
        "UDP 시세": ["없음 (바로 sendto)", "없음 (재전송 X)", "없음 (seq 로 직접 감지)",
                   "있음 (데이터그램 1개 = 메시지 1개)", "1:N (멀티캐스트 그룹)",
                   f"{udp['loss_rate'] * 100:.1f}%", f"편도 평균 {udp['avg_latency']:.2f}ms"],
        "TCP 주문": ["3-way handshake 후 통신", "있음 (ACK + 재전송)", "있음 (순서 번호로 OS 가 정렬)",
                   "없음 (스트림 → \\n 으로 구분)", "1:1 (트레이더마다 연결 1개)", "0%",
                   "-" if rtt.empty else f"왕복 평균 {rtt['rtt_ms'].mean():.2f}ms"],
    })
    st.dataframe(comparison, hide_index=True, use_container_width=True)

    # ---- 4. 최근 체결 내역 / 트레이더 잔고 (TCP STATUS 응답) ----
    if status is not None:
        fills_col, acct_col = st.columns([3, 2])
        with fills_col:
            st.subheader("최근 체결 내역 (거래소 전체)")
            fills = pd.DataFrame(list(reversed(status["fills"])))
            if fills.empty:
                st.info("아직 체결이 없습니다. 사이드바나 trader_client.py 로 주문해 보세요.")
            else:
                fills.columns = ["시각", "트레이더", "매수/매도", "종목", "수량", "체결가"]
                st.dataframe(fills.head(15), hide_index=True, use_container_width=True)
        with acct_col:
            st.subheader("트레이더 잔고")
            s = status["stats"]
            st.caption(f"접속 중 {s['connected']}명 · 주문 {s['orders']}건 "
                       f"(체결 {s['fills']}, 거부 {s['rejects']})")
            rows = []
            for name, acct in status["accounts"].items():
                held = ", ".join(f"{sym} {q}주" for sym, q in acct["holdings"].items()) or "-"
                rows.append({"트레이더": name, "현금": f"{acct['cash']:,}원", "보유 주식": held})
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


live_view()
