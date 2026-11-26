# AI_LOG.md — Record of Using Agentic AI

**English** | [한국어](AI_LOG.ko.md)

This project was developed step by step together with Claude (an AI agent).
For each step I record the date, my request, what the AI did, and what I decided.

---

## Step 0 — Organizing the Requirements (2026-11-24)

**My request**
- Gave the AI the assignment requirements (use TCP/UDP, demonstrate real behavior, record AI usage, no copying code), the design (UDP multicast quotes + TCP orders/fills), the file layout, and the coding rules.
- Asked it to first organize these in CLAUDE.md, then proceed in the order (1) UDP market data → (2) TCP orders/fills → (3) dashboard → (4) README, testing each step and getting my confirmation.

**What the AI did**
- Organized the requirements, design, coding rules, and workflow in `CLAUDE.md`.

**What I decided**
- Quotes over UDP multicast (224.1.1.1:5007), orders over TCP (127.0.0.1:6000).
- Three stocks (SJU 50,000 / KBK 72,000 / NET 13,500), random walk every 0.1 s.
- Standard `socket` module only, comments in Korean, readable code first.

---

## Step 1 — UDP Multicast Market Data (2026-11-24)

**My request**
- Implement the UDP market-data feature: attach seq/ts, compute loss rate, latency, and reordering on the client, and add a `--drop` option.

**What the AI did**
- `protocol.py`: shared settings such as addresses/ports, symbols, and tick interval + JSON `encode`/`decode` + `now_ms()`.
- `exchange_server.py`: random-walk price generation (mean 0, standard deviation 0.2%, in 10 KRW steps) + a thread that sends a multicast message every 0.1 s. The price dict will be shared with the TCP threads in step 2, so it is protected by a Lock.
- `trader_client.py`: joins the multicast group (IGMP Join) and receives quotes; the `QuoteStats` class computes loss, reordering, and latency.
  - Loss = (number of seqs from the first seq to the highest seq) − number received. Counting starts from the "first seq received" so that a client that joins midway does not count earlier numbers as lost.
  - Reordering = a seq arrives that is smaller than the highest seq already received.
  - Latency = receive time − ts.
- Windows/Mac compatibility:
  - The receiving socket binds to `""` instead of the multicast address (Windows cannot bind to a multicast address).
  - `SO_REUSEADDR` + `SO_REUSEPORT` (where available) → several clients on one machine can use the same port (Mac needs REUSEPORT; Windows does not have it).
  - `IP_MULTICAST_LOOP=1` → receivers on the same machine also get the packets.
  - `settimeout(1.0)` → Ctrl+C can terminate the program on Windows.
  - `--iface` option → can be set to `127.0.0.1`, e.g. on a Mac with Wi-Fi turned off.
- Test: ran 1 server + 3 clients (alice default / bob `--drop 0.1` / carol `--reorder 0.05`) for 10 seconds. Logs are in `test_logs/step1_*.log`.

**Test results (10 s, about 100 packets)**
| Client | Option | Received | Loss rate | Out of order | Avg. latency |
|---|---|---|---|---|---|
| alice | none | 101/101 | 0.0% | 0 | 0.34ms |
| bob | `--drop 0.1` | 85/100 | 15.0% | 0 | 0.31ms |
| carol | `--reorder 0.05` | 101/101 | 0.0% | 3 | 3.27ms (held-back packets arrive ~100 ms late, raising the average) |

- All three clients received the same server packets at once → confirmed that a single send from the server is delivered 1:N.
- Also confirmed correct operation with `--iface 127.0.0.1`.

**What the AI additionally proposed**
- On a single machine packets are almost never reordered, so the reordering-detection feature could not be demonstrated. The AI therefore added a `--reorder` option that works like `--drop` (a feature not in the requirements → needed my confirmation).
- Added `--duration` (auto exit) and `--quiet` (turn off event output) options to automate testing.

**What I decided**
- Left it to the AI whether to keep the `--reorder` option → the AI decided to keep it because it is needed to demonstrate reordering detection, and to mark it as an "optional option (for demonstration)" in the code comments and README.
- Decided to upload the code to my GitHub repository.

---

## Step 2 — TCP Orders / Fills (2026-11-24)

**My request**
- After checking step 1, said "go ahead". Implement TCP orders/fills: multithreaded server, `\n` message delimiting, `buy SJU 10` → FILL, RTT measurement, per-trader cash and holdings.
- Midway I said "finish everything and tell me when it's done" → steps 2–4 continued without stopping at each step.

**What the AI did**
- `protocol.py`: `encode_line`, `send_message` (uses `sendall`), `LineReader` (accumulates received bytes and splits at every `\n`). Defined the TCP message types (HELLO/WELCOME/ORDER/FILL/REJECT/BALANCE/STATUS).
- `exchange_server.py`: the main thread runs `accept()`, with one thread per trader (`handle_trader`). Orders are filled immediately at the current price; checking and updating the balance happen inside a single Lock (prevents two threads from spending the same money). REJECT if cash or holdings are insufficient.
- `trader_client.py`: moved UDP reception to a background thread; the main thread handles keyboard input → TCP order → waits for the reply. RTT is measured with `perf_counter`. The FILL output also shows "the price I saw over UDP" so the difference between the quote and the fill price is visible. The step 1 behavior is kept as `--watch` mode.
- `tests/test_message_boundary.py`: sends two messages coalesced in one write, and one message split into single bytes → experiment to check whether the server splits them correctly.

**Test (1 server + 3 trading clients + 1 watching client + boundary experiment)** — logs: `test_logs/step2_*.log`
- alice: 3 fills, 2 rejects (insufficient holdings, insufficient cash), average RTT 0.41ms
- bob (`--drop 0.1`): 13.3% UDP loss, yet all 3 TCP orders filled → confirmed the contrast between UDP loss and TCP reliability
- carol: 60 consecutive buy/sell orders, all 60 filled, average RTT 0.09ms
- dave (`--watch`): prints only quotes as in step 1, 0% loss
- Boundary experiment: coalesced send → 2 replies (order numbers 1, 2); split send → 1 FILL, correct
- Confirmed that if the server is stopped first, the client immediately prints "서버 연결이 끊겼습니다" (connection to the server was lost) on the next order

**Found and fixed during testing**
- Noticed two fill messages printed on one line in the server log (`[체결] bob ...[체결] carol ...`).
  Cause: when several threads `print` at the same time, the 'content' and the 'newline' are written separately and get interleaved. → Fixed with a `log()` function that uses a `print_lock`; resolved in the re-test.

**What I decided**
- Approved step 2, and delegated the remaining steps to be finished without intermediate confirmation.

---

## Step 3 — Streamlit Dashboard (2026-11-24)

**What the AI did**
- `dashboard.py`: designed so that the dashboard itself behaves like a trader.
  - UDP: joins the multicast group directly and receives quotes → per-stock price charts, loss rate (cumulative / last 10 s), one-way latency chart.
  - TCP: sends a `STATUS` request to the exchange every second → recent fills (all traders), trader balances. Records the RTT of every request for an RTT chart.
  - Sidebar: UDP loss simulation slider (same function as `--drop`), buy/sell buttons (TCP orders).
  - Shows a UDP vs TCP comparison table (connection, delivery guarantee, ordering, message boundaries, recipients, measured loss/latency) with live numbers.
  - `@st.cache_resource` creates the sockets/threads only once, and `@st.fragment(run_every=1.0)` refreshes every second.
- Added a `STATUS` message to the server and storage of the 50 most recent fills (`recent_fills`).

**Test (1 server + 2 traders + dashboard, checked with browser automation)** — screenshot: `docs/dashboard.png`, logs: `test_logs/step3_*.log`
- Buy button on the dashboard → FILL, RTT 2.08ms displayed. Fills from alice/bob/dashboard appear together in the fill history.
- Slider at 0.2 → cumulative UDP loss rate 10.3%, last-10-s loss rate 16.0%. Over the same period, 22 TCP requests with 0 lost, average RTT 0.74ms.
- Final trader results: alice UDP 558/558, all 80 orders filled / bob (`--drop 0.1`) UDP loss 8.5%, all 40 orders filled.

**Found and fixed during testing**
- The price chart's y-axis started at 0, making the graph look flat → used Altair to fit the y-axis to the data range.
- The arrow (↑) under metrics was also attached to descriptions such as "0 lost", which could be misleading → moved the descriptions to captions.

**What the AI judged (for reference)**
- The dashboard's UDP latency chart shows occasional 50–100 ms spikes. This is not the network but a processing delay: the receiver thread pauses briefly due to Python's GIL while Streamlit renders the page. Terminal traders were stable at an average of 0.3 ms over the same period, so this was left as is and explained in the README.

---

## Step 4 — README (2026-11-24)

**What the AI did**
- `README.md`: architecture diagram, file descriptions, how to run, commands/options (`--reorder` marked as "optional, for demonstration"), demo scenario, TCP vs UDP comparison, how multicast works, Windows/Mac compatibility, troubleshooting, test results summary.
- `requirements.txt`: `streamlit>=1.37` (because of `st.fragment(run_every=...)`).

**Not yet verified**
- Development and testing were done on Linux. The Windows/Mac compatibility options are in the code, but running it on actual Windows/Mac is something I need to do myself.

---

## Addendum — English Translation of the Documents (2026-11-26)

**My request**
- My professor is not Korean, so I asked for the README in English. Then: "make every markdown document in both English and Korean".

**What I decided**
- English is the main file (`README.md`, `AI_LOG.md`, `CLAUDE.md`), and the original Korean content is kept as `*.ko.md` (`README.ko.md`, `AI_LOG.ko.md`, `CLAUDE.ko.md`). Each file links to the other version at the top.

**What the AI did**
- Translated the three documents into English. Code blocks, commands, and the `docs/dashboard.png` path were kept unchanged, and the README architecture diagram was realigned to fit the English labels.
- The README example output was left in Korean, exactly as the program prints it, with an English gloss of the key words (주, 원, 손실, 체결, etc.) above it.
- Since Claude reads `CLAUDE.md` automatically by its file name, the name was kept and the file was translated into English with its rules unchanged. The "comments in Korean" rule is also unchanged, so code comments stay in Korean.
- Added a rule to the `CLAUDE.md` workflow: "keep markdown documents in both English and Korean and update them together".
