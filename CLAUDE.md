# CLAUDE.md — Mini Exchange (Real-Time Stock Trading Simulator over TCP/UDP)

**English** | [한국어](CLAUDE.ko.md)

Computer Networks course project. Due **2026-11-30**.
This file lists the requirements and rules that the AI (Claude) must always follow when working on this project.

## Mandatory Assignment Requirements
1. The backend uses both TCP and UDP.
2. Demonstrate how the protocols behave in a real application.
3. Use Agentic AI in the development process and record that process in `AI_LOG.md`.
- **Forbidden:** copying or downloading code from the internet. All code is written from scratch.

## Design (modeled on a real stock exchange)
| Channel | Address | Direction | Purpose | Reason |
|---|---|---|---|---|
| UDP multicast | 224.1.1.1:5007 | Exchange → all traders | Market data (quotes) | Loss-tolerant, 1:N, low latency |
| TCP | 127.0.0.1:6000 | Trader ↔ exchange | Orders / fills | Reliability, ordered delivery |

- Three fictional stocks: SJU 50,000 KRW, KBK 72,000 KRW, NET 13,500 KRW
- Prices change every 0.1 s by random walk

## Files
- `protocol.py` — shared settings, JSON message encoding/decoding
- `exchange_server.py` — quote generation + UDP multicast sender + TCP order server (multithreaded)
- `trader_client.py` — UDP quote receiver + TCP order sender
- `dashboard.py` — Streamlit dashboard
- `README.md`, `AI_LOG.md`, `CLAUDE.md` (English) + their Korean versions `README.ko.md`, `AI_LOG.ko.md`, `CLAUDE.ko.md`
- `requirements.txt`

## Functional Requirements
### 1. UDP Market Data
- Every message carries a sequence number `seq` and a send timestamp `ts`.
- The client detects the loss rate from gaps in `seq`, latency from `ts`, and reordering from `seq` going backwards.
- The client's `--drop` option (e.g. 0.1) simulates packet loss.

### 2. TCP Orders / Fills
- The server handles multiple traders connected at the same time using threads.
- TCP is a stream with no message boundaries, so messages are delimited by a newline (`\n`).
- The client types `buy SJU 10` / `sell KBK 5` → the order is sent → filled immediately at the current price → `FILL` reply.
- Measure the round-trip time (RTT) from order to fill.
- Manage each trader's cash balance and stock holdings.

### 3. Dashboard (Streamlit)
- Real-time price chart, UDP loss rate, average latency, TCP order RTT, recent fills
- Show TCP and UDP statistics side by side for comparison.

## Coding Rules
- Python 3.10+. Sockets use only the standard library `socket` module (socket/bind/listen/accept/connect/send/recv).
- External libraries are allowed only for the dashboard (Streamlit, etc.).
- Must work on both Windows and Mac (be careful with multicast socket options).
- Code comments are written in Korean. They explain **why** network concepts such as the handshake, datagrams, streams, message boundaries, and IGMP behave the way they do.
- The user must be able to explain the code themselves, so readable code takes priority over complex abstractions.

## Workflow
- Proceed step by step: (1) UDP market data → (2) TCP orders/fills → (3) dashboard → (4) README
- At the end of each step, run a real test with 1 server + 2 or more clients → report the results → move to the next step **only after the user confirms**.
- For each step, record in `AI_LOG.md` the date, the user's request, what the AI did, and what the user decided.
- In each step, explain in simple terms how TCP and UDP behave differently and why this protocol was chosen for this part.
- Markdown documents are kept in two versions, English (`*.md`) and Korean (`*.ko.md`). When one is changed, update the other as well.
