# Multipath Transport — Testing Methodology

This document describes the test environment, procedure, and results used to validate the multipath transport feature (`PathSendMode`) added to this WTelegramClient fork.

## Environment

### Network Configuration

Three physical VLANs provide independent TCP paths to the Telegram network, each with a dedicated source IP:

| VLAN | Source IP | Interface | Steady-state RTT to Telegram |
|------|-----------|-----------|-------------------------------|
| vlan50 | 10.0.5.30 | vlan50@eno1 | ~43 ms |
| vlan80 | 10.0.8.30 | vlan80@eno1 | ~70 ms |
| vlan90 | 10.0.9.30 | vlan90@eno1 | ~43 ms |

Policy routing tables 100, 800, and 900 ensure that TCP sockets bound to each source IP egress via the correct VLAN gateway, regardless of the default route.  Traffic to Telegram from `10.0.5.30` always leaves via `10.0.5.1 dev vlan50`, etc.

### Application Under Test

`makefoxsrv` — a Telegram-bot server for the MakeFox image-generation service.  It maintains three WTelegramClient instances at steady state:

- **DC 1** — primary Telegram DC (command/response traffic)
- **DC −1** — media DC (file upload; image results sent here)
- **DC 5** — auxiliary DC (some users routed here by Telegram)

Each client instance is configured with `LocalEndPoints = [10.0.5.30, 10.0.8.30, 10.0.9.30]`, giving it three parallel TCP paths to its assigned Telegram data-center endpoint.

### Settings

```ini
# ~/makefoxbot/conf/settings.ini (relevant multipath lines)
address = 10.0.5.30, 10.0.8.30, 10.0.9.30
```

The `SendMode` property on each `WTelegram.Client` instance is set in `FoxTelegram.cs` and `FoxTelegramBot.cs`.

---

## Failure Injection Method

Path failures are simulated with `iptables OUTPUT DROP` rules on the relevant VLAN interface:

```bash
# Block outbound traffic on a VLAN (simulates link failure / ISP outage)
sudo iptables -A OUTPUT -o vlanXX -j DROP

# Restore the VLAN
sudo iptables -D OUTPUT -o vlanXX -j DROP
```

Because the policy routing sends source-IP-bound traffic out the corresponding VLAN, a DROP rule on `vlanXX` silently discards all TCP data for paths using that source address.  From the application's perspective, the TCP socket stalls (kernel send-buffer fills, no ACKs received), then the Telegram server resets the connection after keepalive or retransmit timeout.

> **Safety constraint:** Only `vlan50`, `vlan80`, and `vlan90` are touched.
> `eno1`, `eno2`, `vlan20`, and any other interfaces are never modified.

---

## Path Index Notes

### PreferredOrder mode

In `PreferredOrder` mode, `DoConnectAsync` connects `LocalEndPoints[0]` (vlan50) first.  If it succeeds, it becomes **Path 0**.  Secondary endpoints are attempted in declared order.  Path indices always match the `LocalEndPoints` array order.

### Other modes (RoundRobin, StickyFailover, LowestLatency)

In all other modes, `DoConnectAsync` races all `LocalEndPoints` in **parallel**; the first TCP connection to succeed becomes the primary path (Path 0), regardless of its position in `LocalEndPoints`.  Secondary paths receive indices 1, 2, ... in the order the secondary-connection loop iterates the *remaining* endpoints.

As a result, which VLAN ends up as Path 0 is non-deterministic and depends on connection timing.  During testing we confirmed path assignments by:

1. Observing `"Path N connected from X.X.X.X"` log lines on startup.
2. Correlating `[path N]` probe-send logs with `ss -tnp` to match path indices to source IPs.

---

## Pass/Fail Criterion

Each scenario is **PASS** if an image is generated and delivered end-to-end via `@makefoxtestbot` within a reasonable timeout (≤ 90 seconds from request to Telegram delivery confirmation).

Test command sent via Telegram:
```
/generate anthro male arctic fox, thick white fluffy fur, winter sweater, fully clothed, young, cub, chubby
```

Success is confirmed by receiving both the image message and the `✅ Complete!` notification from the bot.

---

## Test Matrix

The same 8-scenario matrix was run for each `PathSendMode`.  Each scenario uses live image generation as the end-to-end signal.

| # | Scenario | Alive VLANs during test |
|---|----------|------------------------|
| 1 | Baseline — all paths up | vlan50, vlan80, vlan90 |
| 2 | Single failure — block vlan50 | vlan80, vlan90 |
| 3 | Single failure — block vlan80 | vlan50, vlan90 |
| 4 | Single failure — block vlan90 | vlan50, vlan80 |
| 5 | Dual failure — block vlan50 + vlan80 | vlan90 only |
| 6 | Dual failure — block vlan50 + vlan90 | vlan80 only |
| 7 | Dual failure — block vlan80 + vlan90 | vlan50 only |
| 8 | Total outage → recovery | all blocked → all restored |

Scenario 8 additionally verifies:
- The `PathHealthMonitor` detects global unresponsiveness and triggers `PerformFullReconnectAsync`.
- After unblocking, `ConnectAsync` re-establishes all three paths (`"Multipath transport: 3/3 paths alive"` log line).
- Image delivery succeeds immediately after reconnect.

---

## Results

All four modes were tested.  **32/32 scenarios passed.**

### PreferredOrder (8/8)

| Scenario | Result | Delivery time |
|----------|--------|---------------|
| 1 Baseline | PASS | ~20 s |
| 2 Block vlan50 | PASS | ~25 s |
| 3 Block vlan80 | PASS | ~18 s |
| 4 Block vlan90 | PASS | ~19 s |
| 5 Block vlan50+80 | PASS | ~22 s |
| 6 Block vlan50+90 | PASS | ~21 s |
| 7 Block vlan80+90 | PASS | ~24 s |
| 8 Total outage + recovery | PASS | ~15 s |

**Observed behavior:** After recovery from path failure, traffic returned to vlan50 (Path 0 / LocalEndPoints[0]) as expected — the defining characteristic of this mode.

### RoundRobin (8/8)

All 8 scenarios passed.  Sends distributed across alive paths in rotation; delivery times were comparable to `PreferredOrder`.

### StickyFailover (8/8)

All 8 scenarios passed.  After failover to a secondary path, traffic remained on the new path even after the original recovered — correct sticky behavior.

### LowestLatency (8/8)

| Scenario | Result | Delivery time | Observed routing |
|----------|--------|---------------|-----------------|
| 1 Baseline | PASS | 22.7 s | [P0] ~42 ms EWMA |
| 2 Block one VLAN | PASS | 19.8 s | [P0] or [P2] (lowest alive) |
| 3 Block another VLAN | PASS | 13.4 s | remaining low-latency path |
| 4 Block third VLAN | PASS | 24.7 s | remaining low-latency path |
| 5 Block two VLANs (vlan90 alive) | PASS | 24.7 s | [P0] vlan90 |
| 6 Block two VLANs (vlan80 alive) | PASS | 13.3 s | [P0] vlan80 |
| 7 Block two VLANs (vlan50 alive) | PASS | 10.7 s | [P1] vlan50 |
| 8 Total outage + recovery | PASS | 10.3 s | 3/3 paths alive |

**Observed RTT measurements at steady state:**
```
DC 1 > Path 0 RTT 42ms (EWMA 42ms)   ← vlan80 (won parallel race)
DC 1 > Path 1 RTT 43ms (EWMA 43ms)   ← vlan50
DC 1 > Path 2 RTT 44ms (EWMA 44ms)   ← vlan90
DC-1 > Path 0 RTT 43ms (EWMA 42ms)
DC-1 > Path 1 RTT 78ms (EWMA 78ms)   ← (path order varies per DC instance)
DC-1 > Path 2 RTT 43ms (EWMA 42ms)
```

The EWMA (α = 0.2) converges toward the true RTT within ~5–10 probe cycles.  In all scenarios, `GetPrimaryAlivePath()` selected the alive path with the minimum EWMA, and resumed the optimal path automatically when lower-latency paths recovered.

---

## Log Artifacts Used for Verification

All test activity was verified against two log sinks:

- `/tmp/makefoxsrv.log` — stdout (INFO level and above)
- `~/makefoxbot/logs/output.txt` — DEBUG level (includes per-path RTT, `[path N]` send tags, `[PN]` receive tags, reconnect events)

Key log patterns:

| Pattern | Meaning |
|---------|---------|
| `1>Path N RTT Xms (EWMA Yms)` | DC 1, path N received a Pong; RTT and EWMA updated |
| `Sending ... [path N]` | Outbound RPC sent via path N |
| `Receiving ... [PN]` | Response received on path N |
| `Path N unresponsive (Xs silent, probe unanswered). Force-closing.` | Per-path failure detected |
| `All N path(s) globally unresponsive. Triggering full reconnect.` | Total outage detected |
| `Multipath transport: X/Y paths alive.` | Post-reconnect path count |
| `Path N reconnected and registered successfully.` | Per-path recovery |

---

## Procedure (per mode)

1. Edit `FoxTelegram.cs` and `FoxTelegramBot.cs` to set the desired `SendMode`.
2. Kill the test instance: `kill -9 $(cat ~/makefoxbot/data/makefoxsrv.pid)`
3. Build: `cd ~/makefoxbot/src/makefoxsrv && dotnet publish -c Release -o publish`
4. Deploy: `cp -a publish/* ~/makefoxbot/bin/`
5. Start: `cd ~/makefoxbot/bin && nohup ./makefoxsrv > /tmp/makefoxsrv.log 2>&1 &`
6. Wait for `"Added # saved tasks to queue."` in `/tmp/makefoxsrv.log` + 3 s.
7. For each scenario:
   a. Apply `iptables` DROP rules if applicable.
   b. Send `/generate <prompt>` to `@makefoxtestbot`.
   c. Wait for image delivery confirmation (up to 90 s).
   d. Remove DROP rules; allow 10–20 s for path recovery before next scenario.
8. After all scenarios, restore `SendMode = PathSendMode.PreferredOrder` (production default).
