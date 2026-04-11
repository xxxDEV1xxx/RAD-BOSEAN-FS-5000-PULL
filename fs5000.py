#!/usr/bin/env python3
"""
fs5000.py  —  Bosean FS-5000 All-In-One
============================================================
Single script. Does everything:

  1. EXTRACT   — pull all data from device over USB
  2. DECODE    — parse binary protocol, reconstruct real timestamps
  3. REPORT    — write interactive HTML graph of historical data
  4. MONITOR   — live-stream to a second HTML graph (auto-refreshes in browser)
                 with spike detection and event logging

Usage:
  python fs5000.py                        # auto-detect port, full run
  python fs5000.py --port COM3            # specify port
  python fs5000.py --load-dir .           # skip extraction, load existing .bin files
  python fs5000.py --no-extract           # skip extraction, go straight to monitor
  python fs5000.py --report-only          # historical report only, no live
  python fs5000.py --spike-threshold 1.0  # alert above N µSv/h (default 1.0)
  python fs5000.py --out C:\\logs         # output directory

Requirements:
  pip install pyserial

Files written to --out directory:
  fs5000_report_TIMESTAMP.html     interactive historical charts
  fs5000_live_TIMESTAMP.html       live-updating chart (open in browser)
  live_TIMESTAMP.csv               every live reading
  dose_curve_TIMESTAMP.csv         decoded dose history
  rate_curve_TIMESTAMP.csv         decoded rate history
  spikes_TIMESTAMP.txt             spike event log
  *.bin                            raw device data (kept for re-analysis)
"""

import argparse
import csv
import datetime
import glob
import http.server
import json
import os
import struct
import sys
import threading
import time
import webbrowser
from collections import deque
from pathlib import Path

# ── Serial import (optional — only needed for extraction / live stream) ──────
try:
    import serial
    import serial.tools.list_ports
    HAS_SERIAL = True
except ImportError:
    HAS_SERIAL = False

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

CH340_VID  = 0x1A86
CH340_PID  = 0x7523
BAUD       = 115200

# Known timeline anchors
FIRST_POWER   = datetime.datetime(2024, 12,  9, tzinfo=datetime.timezone.utc)
DOSE_RESET    = datetime.datetime(2026,  3,  2, tzinfo=datetime.timezone.utc)
EXTRACTION    = datetime.datetime(2026,  3, 24, 21, 24, 59, tzinfo=datetime.timezone.utc)
EXTRACT_EPOCH = int(EXTRACTION.timestamp())

BG_LOW   = 0.05   # µSv/h natural background lower
BG_HIGH  = 0.35   # µSv/h natural background upper
DEFAULT_SPIKE_THRESHOLD = 1.0
DEFAULT_SPIKE_END       = 0.35
PRE_CONTEXT_S  = 30
POST_CONTEXT_S = 60

STAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# ═══════════════════════════════════════════════════════════════════════════
# FRAMING
# ═══════════════════════════════════════════════════════════════════════════

def _cs(data):
    return sum(data) % 256

def make_packet(payload: bytes) -> bytes:
    hdr  = bytes([0xAA, len(payload) + 3])
    body = hdr + payload
    return body + bytes([_cs(body)]) + bytes([0x55])

def recv_frame(port, timeout_s=3.0):
    deadline = time.monotonic() + timeout_s
    while True:
        if time.monotonic() > deadline:
            return None
        b = port.read(1)
        if not b:
            continue
        if b[0] == 0xAA:
            break
    lb = port.read(1)
    if not lb:
        return None
    length = lb[0]
    rest = port.read(length)
    if len(rest) < length or rest[-1] != 0x55:
        return None
    payload = rest[:-2]
    if _cs(bytes([0xAA, length]) + payload) != rest[-2]:
        return None
    return payload

# ═══════════════════════════════════════════════════════════════════════════
# PORT
# ═══════════════════════════════════════════════════════════════════════════

def find_port(forced=None):
    if not HAS_SERIAL:
        print("ERROR: pyserial not installed.  pip install pyserial")
        sys.exit(1)
    if forced:
        return forced
    for p in serial.tools.list_ports.comports():
        if p.vid == CH340_VID and p.pid == CH340_PID:
            print(f"[AUTO] Found FS-5000 on {p.device} [{p.description}]")
            return p.device
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        print("ERROR: No serial ports. Is CH340 driver installed?")
        sys.exit(1)
    print("CH340 not detected. Available ports:")
    for p in ports:
        print(f"  {p.device}  {p.description}")
    print("Use --port COMx")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════
# EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

def drain_bytes(port, wait=4.0, silence=2.0):
    buf, last_rx = bytearray(), time.monotonic()
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        avail = port.in_waiting
        if avail:
            buf.extend(port.read(avail))
            last_rx = time.monotonic()
            sys.stdout.write(f"\r  {len(buf)} bytes...")
            sys.stdout.flush()
        elif buf and (time.monotonic() - last_rx) > silence:
            break
        else:
            time.sleep(0.05)
    print()
    return bytes(buf)

def extract_all(port_name, out_dir):
    print(f"\n{'='*60}")
    print(f"EXTRACTING from {port_name}")
    print('='*60)
    results = {}
    with serial.Serial(port_name, BAUD, timeout=3) as port:
        port.reset_input_buffer()
        time.sleep(0.1)

        # Stop any active stream
        port.write(make_packet(bytes([0x0e, 0x00])))
        time.sleep(0.4)
        port.reset_input_buffer()

        for name, payload, wait in [
            ("get_version",     bytes([0x06]),       3),
            ("get_dose",        bytes([0x07]),       3),
            ("dose_curve",      bytes([0x03]),      15),
            ("rate_curve",      bytes([0x0f]),      15),
            ("read_alarms",     bytes([0x10]),       5),
        ]:
            print(f"\n[{name}]  sending {make_packet(payload).hex()}")
            port.reset_input_buffer()
            time.sleep(0.1)
            port.write(make_packet(payload))
            raw = drain_bytes(port, wait=wait, silence=2.0)
            if raw:
                path = os.path.join(out_dir, f"{name}_{STAMP}.bin")
                with open(path, 'wb') as f:
                    f.write(raw)
                print(f"  Saved {len(raw)} bytes → {path}")
                results[name] = raw
            else:
                print(f"  NO RESPONSE")

        # Try live stream
        port.reset_input_buffer()
        port.write(make_packet(bytes([0x0e, 0x01])))
        time.sleep(0.3)
        peek = bytearray(port.read(port.in_waiting or 0))
        if peek and peek[0] == 0xAA and len(peek) > 1:
            peek = peek[2 + peek[1]:]  # skip ACK frame
        # Grab a few seconds to validate
        buf = bytearray(peek)
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            avail = port.in_waiting
            if avail:
                buf.extend(port.read(avail))
            time.sleep(0.05)
        # Stop
        port.write(make_packet(bytes([0x0e, 0x00])))
        time.sleep(0.2)
        if buf:
            path = os.path.join(out_dir, f"live_sample_{STAMP}.bin")
            with open(path, 'wb') as f:
                f.write(bytes(buf))
            print(f"\n[live_sample]  {len(buf)} bytes saved")
            results['live_sample'] = bytes(buf)

    return results

# ═══════════════════════════════════════════════════════════════════════════
# DECODE
# ═══════════════════════════════════════════════════════════════════════════

def pattern_scan(raw):
    """Scan for 8-byte records: >I 00 00 H (timestamp, pad, value)."""
    out = []
    i = 0
    while i + 8 <= len(raw):
        ts  = struct.unpack_from('>I', raw, i)[0]
        pad = raw[i+4:i+6]
        val = struct.unpack_from('>H', raw, i+6)[0]
        if ts > 0x30000000 and pad == b'\x00\x00':
            out.append((ts, val))
            i += 8
        else:
            i += 1
    return out

def split_sessions(records, gap=10):
    if not records:
        return []
    sessions, cur = [], [records[0]]
    for ts, val in records[1:]:
        if ts < cur[-1][0] - gap:
            sessions.append(cur)
            cur = [(ts, val)]
        else:
            cur.append((ts, val))
    sessions.append(cur)
    return sessions

def fmt_utc(epoch_s):
    return datetime.datetime.fromtimestamp(
        int(epoch_s), tz=datetime.timezone.utc).isoformat()

def decode_dose(raw):
    """870 records @ 455s. Anchor last record = EXTRACT_EPOCH."""
    recs = pattern_scan(raw)
    if not recs:
        return []
    sess = max(split_sessions(recs), key=len)
    offset = EXTRACT_EPOCH - sess[-1][0]
    out = []
    for ts, val in sess:
        rt = ts + offset
        out.append({
            'ts_ms':     rt * 1000,
            'timestamp': fmt_utc(rt),
            'dose_uSv':  round(val * 0.001, 3),
            'raw': val,
        })
    return out

def decode_rate_hist(raw):
    """Historical rate sessions — anchor last historical record just before live window."""
    recs = pattern_scan(raw)
    if not recs:
        return []
    sessions = split_sessions(recs)
    # Session -1 = live 1s buffer. Sessions 0..-2 = historical.
    if len(sessions) < 2:
        return []
    hist_sessions = sessions[:-1]
    live_sess = sessions[-1]
    RATE_INTERVAL = 600
    # Live window start
    live_end_device = live_sess[-1][0]
    live_offset     = EXTRACT_EPOCH - live_end_device
    live_start_real = live_sess[0][0] + live_offset
    # Flatten historical sessions in order
    all_hist = []
    for s in hist_sessions:
        all_hist.extend(s)
    # Assign timestamps backwards from live start
    hist_end_ts = live_start_real - RATE_INTERVAL
    out = []
    for i, (_, val) in enumerate(reversed(all_hist)):
        rt = hist_end_ts - i * RATE_INTERVAL
        out.insert(0, {
            'ts_ms':          rt * 1000,
            'timestamp':      fmt_utc(rt),
            'dose_rate_uSvh': round(val * 0.01, 4),
            'raw': val,
        })
    return out

def decode_rate_live(raw):
    """Live 1s rate buffer. Anchor last record = EXTRACT_EPOCH."""
    recs = pattern_scan(raw)
    if not recs:
        return []
    sessions = split_sessions(recs)
    sess = sessions[-1]
    offset = EXTRACT_EPOCH - sess[-1][0]
    out = []
    t0 = sess[0][0]
    for ts, val in sess:
        rt = ts + offset
        out.append({
            'ts_ms':          rt * 1000,
            'timestamp':      fmt_utc(rt),
            'elapsed_s':      ts - t0,
            'dose_rate_uSvh': round(val * 0.01, 4),
            'raw': val,
        })
    return out

def decode_version(raw):
    i = 0
    while i < len(raw):
        if raw[i] != 0xAA:
            i += 1
            continue
        if i + 1 >= len(raw):
            break
        length = raw[i+1]
        end = i + 2 + length
        if end > len(raw):
            break
        payload = raw[i+2:end-2]
        if len(payload) >= 2 and payload[1] == 0x06:
            try:
                s = payload[2:].decode('ascii', errors='replace')
                return s.split('\x00')[0].strip()
            except Exception:
                pass
        i = end
    return ''

def load_bins(load_dir):
    """Auto-detect the most recent set of .bin files in a directory."""
    def latest(pattern):
        matches = sorted(glob.glob(os.path.join(load_dir, pattern)))
        return matches[-1] if matches else None

    raw = {}
    for key, pat in [
        ('dose_curve',  'dose_curve_*.bin'),
        ('rate_curve',  'rate_curve_*.bin'),
        ('read_alarms', 'read_alarms_*.bin'),
        ('get_version', 'get_version_*.bin'),
        ('get_dose',    'get_dose_*.bin'),
    ]:
        p = latest(pat)
        if p and os.path.exists(p):
            raw[key] = open(p, 'rb').read()
            print(f"  Loaded {key}: {p} ({len(raw[key])} bytes)")
        else:
            print(f"  Not found: {pat}")
    return raw

# ═══════════════════════════════════════════════════════════════════════════
# HISTORICAL HTML REPORT
# ═══════════════════════════════════════════════════════════════════════════

REPORT_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>FS-5000 Historical Report — {title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',system-ui,monospace;padding:24px}}
h1{{color:#58a6ff;font-size:1.35rem;margin-bottom:3px}}
.sub{{color:#8b949e;font-size:.8rem;margin-bottom:20px;line-height:1.7}}
.sub b{{color:#e6edf3}}
.stats{{display:flex;flex-wrap:wrap;gap:11px;margin-bottom:22px}}
.st{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:11px 16px;min-width:140px}}
.stl{{color:#8b949e;font-size:.68rem;text-transform:uppercase;letter-spacing:.06em;margin-bottom:3px}}
.stv{{font-size:1.4rem;font-weight:700;line-height:1.1}}
.stu{{color:#8b949e;font-size:.72rem;margin-top:2px}}
.blue{{color:#58a6ff}}.green{{color:#3fb950}}.orange{{color:#f0883e}}.red{{color:#f85149}}
.panel{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:18px;margin-bottom:18px}}
.ph{{color:#58a6ff;font-size:.92rem;margin-bottom:13px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}}
.badge{{font-size:.68rem;padding:2px 7px;border-radius:9px;font-weight:700}}
.b-ok{{background:#1a3a2a;color:#3fb950}}
.b-warn{{background:#3d2a0a;color:#f0883e}}
.b-alert{{background:#3a1a1a;color:#f85149}}
.b-info{{background:#1a2a3a;color:#58a6ff}}
.b-grey{{background:#21262d;color:#8b949e}}
.cbox{{position:relative;height:260px}}
.note{{color:#8b949e;font-size:.74rem;margin-top:10px;padding:9px 13px;background:#0d1117;border-radius:5px;border-left:3px solid #30363d;line-height:1.5}}
.note b{{color:#e6edf3}}.note .hi{{color:#f0883e}}.note .ok{{color:#3fb950}}
.tl{{display:flex;flex-direction:column;gap:0;font-size:.78rem}}
.tlr{{display:flex;gap:0}}
.tld{{width:14px;display:flex;flex-direction:column;align-items:center;flex-shrink:0}}
.tlc{{width:10px;height:10px;border-radius:50%;border:2px solid #58a6ff;background:#0d1117;flex-shrink:0;margin-top:3px}}
.tll{{width:2px;background:#30363d;flex-grow:1}}
.tlco{{padding:2px 0 13px 11px;flex:1}}
.tldt{{color:#58a6ff;font-weight:600;font-size:.76rem}}
.tltx{{color:#8b949e;margin-top:2px;line-height:1.4}}
footer{{color:#484f58;font-size:.7rem;text-align:center;margin-top:20px}}
</style>
</head>
<body>
<h1>&#9762; Bosean FS-5000 — Historical Radiation Report</h1>
<div class="sub">
  Device since <b>2024-12-09</b> &nbsp;·&nbsp;
  Dose reset <b>2026-03-02</b> &nbsp;·&nbsp;
  Extracted <b>2026-03-24 21:24:59 UTC</b> &nbsp;·&nbsp;
  Location: <b>Perris, CA</b>
  {ver_line}
</div>

<div class="stats">
  <div class="st"><div class="stl">Session Dose</div><div class="stv blue">{final_dose}</div><div class="stu">µSv (since 2026-03-02)</div></div>
  <div class="st"><div class="stl">Buffer Window</div><div class="stv blue">{span_days}d</div><div class="stu">recoverable history</div></div>
  <div class="st"><div class="stl">Live Avg Rate</div><div class="stv {live_cls}">{avg_live}</div><div class="stu">µSv/h (last 6 min)</div></div>
  <div class="st"><div class="stl">Live Peak</div><div class="stv {peak_live_cls}">{peak_live}</div><div class="stu">µSv/h</div></div>
  <div class="st"><div class="stl">Dose Records</div><div class="stv blue">{n_dose}</div><div class="stu">@ 455s interval</div></div>
  <div class="st"><div class="stl">Rate Records</div><div class="stv blue">{n_live}</div><div class="stu">@ ~1.7s live</div></div>
</div>

<div class="panel">
  <div class="ph">&#128336; Ownership Timeline</div>
  <div class="tl">
    <div class="tlr"><div class="tld"><div class="tlc"></div><div class="tll"></div></div><div class="tlco"><div class="tldt">2024-12-09</div><div class="tltx">Device first powered. RTC never synced to wall time.</div></div></div>
    <div class="tlr"><div class="tld"><div class="tlc" style="border-color:#f0883e"></div><div class="tll"></div></div><div class="tlco"><div class="tldt" style="color:#f0883e">2026-03-02</div><div class="tltx">Total dose reset. Counter restarted. Pre-reset history not recoverable from ring buffer.</div></div></div>
    <div class="tlr"><div class="tld"><div class="tlc"></div><div class="tll"></div></div><div class="tlco"><div class="tldt">2026-03-20 07:29 UTC</div><div class="tltx">Earliest recoverable dose record (ring buffer tail, 870 records).</div></div></div>
    <div class="tlr"><div class="tld"><div class="tlc" style="border-color:#f85149"></div><div class="tll"></div></div><div class="tlco"><div class="tldt" style="color:#f85149">Pre-2026-03-20</div><div class="tltx">2.0 µSv/h spike events observed on camera — in overwritten ring buffer. Not recoverable from this extraction. Use live monitor for future capture.</div></div></div>
    <div class="tlr"><div class="tld"><div class="tlc"></div><div class="tll"></div></div><div class="tlco"><div class="tldt">2026-03-24 21:19 UTC</div><div class="tltx">Live 1-second rate capture begins (210 samples, ~6 min).</div></div></div>
    <div class="tlr"><div class="tld"><div class="tlc" style="border-color:#3fb950"></div><div class="tll" style="background:transparent"></div></div><div class="tlco"><div class="tldt" style="color:#3fb950">2026-03-24 21:24:59 UTC</div><div class="tltx">USB extraction. Live reading: D=58.1 µSv total, DR=0.18 µSv/h, CPM=21.</div></div></div>
  </div>
</div>

<div class="panel">
  <div class="ph">&#128200; Cumulative Dose — Ring Buffer Tail
    <span class="badge b-info">{n_dose} records · 455s</span>
    <span class="badge b-ok">NORMAL</span>
  </div>
  <div class="cbox"><canvas id="doseChart"></canvas></div>
  <div class="note">
    <b>{n_dose} records</b> · 2026-03-20 07:29 → 2026-03-24 21:24 UTC · {span_days} days.
    Dose: <b>{dose_start} → {final_dose} µSv</b>.
    Device total at extraction: <b class="ok">58.1 µSv</b>.
    Pre-2026-03-20 data overwritten by ring buffer — not recoverable.
  </div>
</div>

<div class="panel">
  <div class="ph">&#9889; Dose Rate — Live Capture (1-second resolution)
    <span class="badge b-info">{n_live} records · ~1.7s</span>
    <span class="badge {live_badge_cls}">{live_badge}</span>
  </div>
  <div class="cbox"><canvas id="rateLiveChart"></canvas></div>
  <div class="note">
    <b>{n_live} records</b> · 2026-03-24 21:19 → 21:24 UTC.
    Range: <b class="ok">{min_live:.4f} – {max_live:.4f} µSv/h</b>.
    Avg: <b class="ok">{avg_live} µSv/h</b>. CPM=21 at extraction.
    Natural background reference band shown (0.05 – 0.35 µSv/h).
  </div>
</div>

<div class="panel">
  <div class="ph">&#9888; Note on Historical Rate Data
    <span class="badge b-grey">ARTIFACT — NOT PLOTTED</span>
  </div>
  <div class="note" style="margin-top:0">
    The historical 10-minute rate curve (690 records) contains values of <b class="hi">10–67 µSv/h</b>
    which are inconsistent with the live capture showing <b class="ok">0.11 µSv/h average</b> minutes later.
    Values are quantized in exact multiples of 2.56 — a signature of integer overflow / bit-shift corruption
    from multiple device clock resets, not real radiation. They are excluded from charting.
    The <b>spike events you observed on camera (1.3–2.0 µSv/h)</b> are real transients that occurred before
    2026-03-20 and are not recoverable from this extraction.
    <b>Run the live monitor (see terminal) to capture future events at 1-second resolution.</b>
  </div>
</div>

<footer>
  FS-5000 stock firmware · J321 GM tube · CH340 USB · 8-byte BE record format (uint32 ts + 2-byte pad + uint16 val) ·
  Timestamps reconstructed: first power 2024-12-09, dose reset 2026-03-02, extraction 2026-03-24T21:24:59Z
</footer>

<script>
const DOSE  = {dose_json};
const RLIVE = {rlive_json};

Chart.defaults.color = '#8b949e';
Chart.defaults.borderColor = '#1e2630';

const tip = {{
  backgroundColor:'#161b22',borderColor:'#30363d',borderWidth:1,
  titleColor:'#58a6ff',bodyColor:'#e6edf3',padding:10
}};
const xAx = () => ({{
  type:'time',
  time:{{ tooltipFormat:'yyyy-MM-dd HH:mm:ss',
         displayFormats:{{second:'HH:mm:ss',minute:'HH:mm',hour:'MM/dd HH:mm',day:'MMM dd'}} }},
  grid:{{color:'#1e2630'}},ticks:{{color:'#8b949e',maxRotation:20}}
}});
const yAx = (lbl,extra={{}}) => ({{
  title:{{display:true,text:lbl,color:'#8b949e'}},
  grid:{{color:'#1e2630'}},ticks:{{color:'#8b949e'}},
  beginAtZero:false,...extra
}});

new Chart(document.getElementById('doseChart'),{{
  type:'line',
  data:{{datasets:[{{
    label:'Cumulative Dose (µSv)',
    data:DOSE.map(r=>( {{x:r.ts_ms,y:r.dose_uSv}} )),
    borderColor:'#58a6ff',backgroundColor:'rgba(88,166,255,0.07)',
    borderWidth:1.5,pointRadius:0,pointHoverRadius:4,fill:true,tension:0.3
  }}]}},
  options:{{responsive:true,maintainAspectRatio:false,
    interaction:{{mode:'index',intersect:false}},
    plugins:{{legend:{{display:false}},tooltip:tip}},
    scales:{{x:xAx(),y:yAx('Dose (µSv)')}}
  }}
}});

const bgH = RLIVE.map(r=>( {{x:r.ts_ms,y:0.35}} ));
const bgL = RLIVE.map(r=>( {{x:r.ts_ms,y:0.05}} ));
new Chart(document.getElementById('rateLiveChart'),{{
  type:'line',
  data:{{datasets:[
    {{label:'Dose Rate (µSv/h)',data:RLIVE.map(r=>( {{x:r.ts_ms,y:r.dose_rate_uSvh}} )),
      borderColor:'#3fb950',backgroundColor:'rgba(63,185,80,0.08)',
      borderWidth:1.5,pointRadius:1.5,pointHoverRadius:5,fill:false,tension:0.2,order:1}},
    {{label:'BG High 0.35',data:bgH,borderColor:'rgba(240,136,62,0.4)',borderWidth:1,
      borderDash:[5,4],pointRadius:0,fill:'+1',backgroundColor:'rgba(240,136,62,0.05)',order:2}},
    {{label:'BG Low 0.05',data:bgL,borderColor:'rgba(63,185,80,0.2)',borderWidth:1,
      borderDash:[5,4],pointRadius:0,fill:false,order:3}}
  ]}},
  options:{{responsive:true,maintainAspectRatio:false,
    interaction:{{mode:'index',intersect:false}},
    plugins:{{legend:{{display:true,labels:{{color:'#8b949e',boxWidth:10,font:{{size:10}}}}}},tooltip:tip}},
    scales:{{x:xAx(),y:yAx('Dose Rate (µSv/h)',{{suggestedMax:0.5}})}}
  }}
}});
</script>
</body>
</html>"""

def build_report_html(dose_records, rlive_records, version_str, out_dir):
    if not dose_records and not rlive_records:
        print("  No decoded records — skipping report.")
        return None

    dose_records  = dose_records  or []
    rlive_records = rlive_records or []

    final_dose  = dose_records[-1]['dose_uSv']  if dose_records  else 0
    dose_start  = dose_records[0]['dose_uSv']   if dose_records  else 0
    span_ms     = (dose_records[-1]['ts_ms'] - dose_records[0]['ts_ms']) if len(dose_records) > 1 else 0
    span_days   = round(span_ms / 86400000, 1)
    n_dose      = len(dose_records)

    live_vals   = [r['dose_rate_uSvh'] for r in rlive_records] or [0]
    avg_live    = round(sum(live_vals)/len(live_vals), 4)
    peak_live   = round(max(live_vals), 4)
    min_live    = round(min(live_vals), 4)
    max_live    = round(max(live_vals), 4)
    n_live      = len(rlive_records)

    live_cls      = 'green' if avg_live  < BG_HIGH else 'orange'
    peak_live_cls = 'green' if peak_live < BG_HIGH else ('orange' if peak_live < 1.0 else 'red')
    live_badge    = 'NORMAL BACKGROUND' if avg_live < BG_HIGH else 'ABOVE BACKGROUND'
    live_badge_cls= 'b-ok' if avg_live < BG_HIGH else 'b-warn'
    ver_line      = f'&nbsp;·&nbsp; Firmware: <b>{version_str}</b>' if version_str else ''

    html = REPORT_TEMPLATE.format(
        title          = STAMP,
        ver_line       = ver_line,
        final_dose     = final_dose,
        dose_start     = dose_start,
        span_days      = span_days,
        avg_live       = avg_live,
        peak_live      = peak_live,
        min_live       = min_live,
        max_live       = max_live,
        n_dose         = n_dose,
        n_live         = n_live,
        live_cls       = live_cls,
        peak_live_cls  = peak_live_cls,
        live_badge     = live_badge,
        live_badge_cls = live_badge_cls,
        dose_json      = json.dumps(dose_records),
        rlive_json     = json.dumps(rlive_records),
    )
    path = os.path.join(out_dir, f"fs5000_report_{STAMP}.html")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  Report → {path}")
    return path

# ═══════════════════════════════════════════════════════════════════════════
# LIVE MONITOR + LIVE HTML GRAPH
# ═══════════════════════════════════════════════════════════════════════════

LIVE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>FS-5000 Live Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',system-ui,monospace;padding:20px;height:100vh;display:flex;flex-direction:column}
h1{color:#58a6ff;font-size:1.1rem;margin-bottom:10px;display:flex;align-items:center;gap:10px}
.dot{width:10px;height:10px;border-radius:50%;background:#3fb950;animation:pulse 1.2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}
.stats{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}
.st{background:#161b22;border:1px solid #30363d;border-radius:7px;padding:9px 14px;min-width:120px}
.stl{color:#8b949e;font-size:.66rem;text-transform:uppercase;letter-spacing:.05em;margin-bottom:2px}
.stv{font-size:1.3rem;font-weight:700}
.stu{color:#8b949e;font-size:.68rem;margin-top:1px}
.blue{color:#58a6ff}.green{color:#3fb950}.orange{color:#f0883e}.red{color:#f85149}
.panels{display:flex;flex-direction:column;gap:14px;flex:1}
.panel{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;flex:1}
.ph{color:#58a6ff;font-size:.85rem;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between}
.ph-right{color:#8b949e;font-size:.72rem}
.cbox{position:relative;height:100%;min-height:180px}
.spike-banner{display:none;background:#3a1a1a;border:1px solid #f85149;border-radius:6px;
  padding:10px 16px;margin-bottom:12px;color:#f85149;font-weight:600;font-size:.9rem}
.spike-banner.show{display:block;animation:flash 0.5s ease-in-out}
@keyframes flash{0%,100%{opacity:1}50%{opacity:0.5}}
#spikes-log{margin-top:10px;font-size:.72rem;color:#8b949e;max-height:80px;overflow-y:auto}
#spikes-log .spike-entry{padding:2px 0;border-bottom:1px solid #21262d}
#spikes-log .spike-entry b{color:#f0883e}
</style>
</head>
<body>
<h1><span class="dot"></span> FS-5000 Live Monitor &nbsp;<span id="conn-status" style="font-size:.75rem;color:#8b949e">connecting...</span></h1>

<div class="spike-banner" id="spikeBanner">&#9888; SPIKE DETECTED &mdash; <span id="spikeVal"></span></div>

<div class="stats">
  <div class="st"><div class="stl">Current Rate</div><div class="stv" id="s-rate">—</div><div class="stu">µSv/h</div></div>
  <div class="st"><div class="stl">Session Peak</div><div class="stv" id="s-peak">—</div><div class="stu">µSv/h</div></div>
  <div class="st"><div class="stl">Session Avg</div><div class="stv" id="s-avg">—</div><div class="stu">µSv/h</div></div>
  <div class="st"><div class="stl">CPM</div><div class="stv blue" id="s-cpm">—</div><div class="stu">counts/min</div></div>
  <div class="st"><div class="stl">Dose</div><div class="stv blue" id="s-dose">—</div><div class="stu">µSv session</div></div>
  <div class="st"><div class="stl">Spike Events</div><div class="stv red" id="s-spikes">0</div><div class="stu">this session</div></div>
  <div class="st"><div class="stl">Records</div><div class="stv blue" id="s-count">0</div><div class="stu">total</div></div>
</div>

<div class="panels">
  <div class="panel">
    <div class="ph">
      <span>&#9889; Dose Rate — Rolling 10 Minutes</span>
      <span class="ph-right" id="ts-display">—</span>
    </div>
    <div class="cbox"><canvas id="rateChart"></canvas></div>
  </div>
  <div class="panel" style="flex:0.4">
    <div class="ph"><span>&#128680; Spike Events</span></div>
    <div id="spikes-log"><span style="color:#484f58">No spikes detected yet.</span></div>
  </div>
</div>

<script>
const MAX_POINTS   = 600;    // 10 minutes at 1s
const SPIKE_THRESH = {spike_threshold};
const BG_HIGH      = 0.35;
const BG_LOW       = 0.05;

let allRates = [], sessionPeak = 0, sessionSum = 0, sessionCount = 0;
let spikeCount = 0, inSpike = false;

Chart.defaults.color = '#8b949e';
Chart.defaults.borderColor = '#1e2630';

const rateDataset = {{
  label:'Dose Rate (µSv/h)',
  data:[],
  borderColor:'#3fb950',
  backgroundColor:'rgba(63,185,80,0.08)',
  borderWidth:1.5,
  pointRadius:0,
  pointHoverRadius:4,
  fill:false,
  tension:0.2,
  order:1
}};
const spikeDataset = {{
  label:'Spike threshold',
  data:[],
  borderColor:'rgba(248,81,73,0.5)',
  borderWidth:1,
  borderDash:[5,4],
  pointRadius:0,
  fill:false,
  order:2
}};
const bgHDataset = {{
  label:'BG High (0.35)',
  data:[],
  borderColor:'rgba(240,136,62,0.35)',
  borderWidth:1,
  borderDash:[4,4],
  pointRadius:0,
  fill:'+1',
  backgroundColor:'rgba(240,136,62,0.04)',
  order:3
}};
const bgLDataset = {{
  label:'BG Low (0.05)',
  data:[],
  borderColor:'rgba(63,185,80,0.2)',
  borderWidth:1,
  borderDash:[4,4],
  pointRadius:0,
  fill:false,
  order:4
}};

const chart = new Chart(document.getElementById('rateChart'), {{
  type:'line',
  data:{{ datasets:[rateDataset, spikeDataset, bgHDataset, bgLDataset] }},
  options:{{
    responsive:true, maintainAspectRatio:false,
    animation:{{ duration:0 }},
    interaction:{{ mode:'index', intersect:false }},
    plugins:{{
      legend:{{ display:true, labels:{{ color:'#8b949e', boxWidth:10, font:{{size:10}} }} }},
      tooltip:{{ backgroundColor:'#161b22', borderColor:'#30363d', borderWidth:1,
                 titleColor:'#58a6ff', bodyColor:'#e6edf3' }}
    }},
    scales:{{
      x:{{ type:'time',
           time:{{ tooltipFormat:'HH:mm:ss', displayFormats:{{second:'HH:mm:ss',minute:'HH:mm'}} }},
           grid:{{ color:'#1e2630' }}, ticks:{{ color:'#8b949e', maxRotation:0 }} }},
      y:{{ title:{{ display:true, text:'µSv/h', color:'#8b949e' }},
           grid:{{ color:'#1e2630' }}, ticks:{{ color:'#8b949e' }},
           beginAtZero:true, suggestedMax:0.5 }}
    }}
  }}
}});

function addPoint(ts_ms, dr, cpm, dose) {{
  const now = ts_ms || Date.now();
  rateDataset.data.push({{ x:now, y:dr }});
  spikeDataset.data.push({{ x:now, y:SPIKE_THRESH }});
  bgHDataset.data.push({{ x:now, y:BG_HIGH }});
  bgLDataset.data.push({{ x:now, y:BG_LOW }});

  // Trim to MAX_POINTS
  if (rateDataset.data.length > MAX_POINTS) {{
    rateDataset.data.shift();
    spikeDataset.data.shift();
    bgHDataset.data.shift();
    bgLDataset.data.shift();
  }}

  // Dynamically adjust y-axis max
  const visRates = rateDataset.data.map(p=>p.y);
  const maxVis = Math.max(...visRates);
  chart.options.scales.y.suggestedMax = Math.max(0.5, maxVis * 1.2);
  chart.update('none');

  // Stats
  sessionPeak = Math.max(sessionPeak, dr);
  sessionSum += dr;
  sessionCount++;

  document.getElementById('s-rate').textContent  = dr.toFixed(4);
  document.getElementById('s-rate').className    = 'stv ' + (dr>=SPIKE_THRESH?'red':dr>=BG_HIGH?'orange':'green');
  document.getElementById('s-peak').textContent  = sessionPeak.toFixed(4);
  document.getElementById('s-peak').className    = 'stv ' + (sessionPeak>=SPIKE_THRESH?'red':sessionPeak>=BG_HIGH?'orange':'green');
  document.getElementById('s-avg').textContent   = (sessionSum/sessionCount).toFixed(4);
  document.getElementById('s-cpm').textContent   = cpm||'—';
  document.getElementById('s-dose').textContent  = dose||'—';
  document.getElementById('s-count').textContent = sessionCount;
  document.getElementById('ts-display').textContent = new Date(now).toISOString().replace('T',' ').slice(0,19)+' UTC';

  // Spike detection
  if (dr >= SPIKE_THRESH && !inSpike) {{
    inSpike = true;
    spikeCount++;
    document.getElementById('s-spikes').textContent = spikeCount;
    const banner = document.getElementById('spikeBanner');
    document.getElementById('spikeVal').textContent = dr.toFixed(4) + ' µSv/h at ' + new Date(now).toISOString().slice(11,19);
    banner.classList.add('show');
    // Add to log
    const log = document.getElementById('spikes-log');
    const entry = document.createElement('div');
    entry.className = 'spike-entry';
    entry.innerHTML = `<b>SPIKE #${{spikeCount}}</b> &nbsp; ${{new Date(now).toISOString().replace('T',' ').slice(0,19)}} UTC &nbsp; peak=${{dr.toFixed(4)}} µSv/h`;
    if (log.firstChild && log.firstChild.style && log.firstChild.color === '#484f58') log.innerHTML='';
    log.prepend(entry);
  }} else if (dr < BG_HIGH && inSpike) {{
    inSpike = false;
    document.getElementById('spikeBanner').classList.remove('show');
  }}
}}

// Poll the server for new readings
let lastIdx = 0;
async function poll() {{
  try {{
    const r = await fetch('/live?from=' + lastIdx);
    if (!r.ok) throw new Error(r.status);
    const data = await r.json();
    document.getElementById('conn-status').textContent = 'live';
    document.getElementById('conn-status').style.color = '#3fb950';
    for (const rec of data.records) {{
      addPoint(rec.ts_ms, rec.dose_rate_uSvh, rec.cpm, rec.dose_uSv);
      lastIdx++;
    }}
  }} catch(e) {{
    document.getElementById('conn-status').textContent = 'reconnecting...';
    document.getElementById('conn-status').style.color = '#f0883e';
  }}
  setTimeout(poll, 800);
}}
poll();
</script>
</body>
</html>
""".replace('{spike_threshold}', str(DEFAULT_SPIKE_THRESHOLD))


class LiveDataStore:
    """Thread-safe store for live readings."""
    def __init__(self):
        self._records = []
        self._lock    = threading.Lock()

    def append(self, rec):
        with self._lock:
            self._records.append(rec)

    def since(self, idx):
        with self._lock:
            return self._records[idx:]

    def all(self):
        with self._lock:
            return list(self._records)


class LiveHTTPHandler(http.server.BaseHTTPRequestHandler):
    store = None  # set before serving

    def log_message(self, fmt, *args):
        pass  # suppress access logs

    def do_GET(self):
        if self.path == '/':
            body = LIVE_HTML.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', len(body))
            self.end_headers()
            self.wfile.write(body)

        elif self.path.startswith('/live'):
            from urllib.parse import urlparse, parse_qs
            qs   = parse_qs(urlparse(self.path).query)
            idx  = int(qs.get('from', ['0'])[0])
            recs = self.store.since(idx)
            body = json.dumps({'records': recs}).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()


def parse_live_line(line):
    rec = {}
    for part in line.split(';'):
        part = part.strip()
        if not part:
            continue
        if 'T' in part and part[0].isdigit():
            rec['device_ts'] = part
        elif ':' in part:
            k, _, v = part.partition(':')
            rec[k.strip()] = v.strip()
    if 'DR' not in rec:
        return None
    try:
        dr_str = rec['DR'].replace('uSv/h', '').replace('mSv/h', '').strip()
        dr = float(dr_str)
        if 'mSv/h' in rec['DR']:
            dr *= 1000
        rec['dose_rate_uSvh'] = dr
    except ValueError:
        return None
    try:
        rec['dose_uSv'] = float(rec.get('D', '0').replace('uSv', '').replace('mSv', ''))
    except ValueError:
        rec['dose_uSv'] = 0.0
    try:
        rec['cpm'] = int(rec.get('CPM', '0'))
    except ValueError:
        rec['cpm'] = 0
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    rec['ts_ms']      = int(now.timestamp() * 1000)
    rec['wall_time']  = now.isoformat()
    return rec


# ── Spike detector ────────────────────────────────────────────────────────────

class SpikeDetector:
    IDLE = 0; ACTIVE = 1; COOLDOWN = 2

    def __init__(self, threshold, end_thresh, pre_s, post_s, on_spike):
        self.threshold  = threshold
        self.end_thresh = end_thresh
        self.pre_s      = pre_s
        self.post_s     = post_s
        self.on_spike   = on_spike
        self.state      = self.IDLE
        self.pre_buf    = deque()
        self.event_buf  = []
        self.peak       = 0.0
        self.spike_start= None
        self.cd_time    = None

    def feed(self, rec):
        dr  = rec['dose_rate_uSvh']
        now = time.monotonic()

        if self.state == self.IDLE:
            self.pre_buf.append(rec)
            while len(self.pre_buf) > self.pre_s + 5:
                self.pre_buf.popleft()
            if dr >= self.threshold:
                self.state       = self.ACTIVE
                self.event_buf   = list(self.pre_buf)
                self.peak        = dr
                self.spike_start = rec['wall_time']

        elif self.state == self.ACTIVE:
            self.event_buf.append(rec)
            self.peak = max(self.peak, dr)
            if dr < self.end_thresh:
                self.state   = self.COOLDOWN
                self.cd_time = now

        elif self.state == self.COOLDOWN:
            self.event_buf.append(rec)
            self.peak = max(self.peak, dr)
            if dr >= self.threshold:
                self.state   = self.ACTIVE
                self.cd_time = None
            elif now - self.cd_time >= self.post_s:
                self._emit()
                self.state = self.IDLE
                self.pre_buf.clear()
                self.event_buf = []

    def _emit(self):
        if not self.event_buf:
            return
        spike_recs = [r for r in self.event_buf if r['dose_rate_uSvh'] >= self.threshold]
        dur = 0.0
        if len(spike_recs) >= 2:
            try:
                a = datetime.datetime.fromisoformat(spike_recs[0]['wall_time'])
                b = datetime.datetime.fromisoformat(spike_recs[-1]['wall_time'])
                dur = abs((b - a).total_seconds())
            except Exception:
                pass
        self.on_spike({
            'spike_start':   self.spike_start,
            'peak_uSvh':     self.peak,
            'duration_s':    round(dur, 1),
            'spike_records': len(spike_recs),
            'all_records':   self.event_buf[:],
        })


def run_live_monitor(port_name, out_dir, spike_threshold, spike_end,
                     pre_s, post_s, live_store, quiet=False):
    """Runs in its own thread. Feeds live_store and spike detector."""
    csv_path  = os.path.join(out_dir, f"live_{STAMP}.csv")
    spk_path  = os.path.join(out_dir, f"spikes_{STAMP}.txt")
    spike_events = []

    def on_spike(ev):
        spike_events.append(ev)
        with open(spk_path, 'w', encoding='utf-8') as f:
            f.write(f"FS-5000 SPIKE EVENT LOG\n")
            f.write(f"Generated: {datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}\n")
            f.write(f"Threshold: {spike_threshold} µSv/h\n")
            f.write(f"Events: {len(spike_events)}\n")
            f.write("="*70 + "\n\n")
            for i, e in enumerate(spike_events):
                f.write(f"EVENT #{i+1}\n")
                f.write(f"  Start:    {e['spike_start']}\n")
                f.write(f"  Peak:     {e['peak_uSvh']:.4f} µSv/h\n")
                f.write(f"  Duration: {e['duration_s']:.1f}s\n")
                f.write(f"  Records:  {e['spike_records']} (in spike) / {len(e['all_records'])} total\n\n")
                f.write("  Timeline:\n")
                for r in e['all_records']:
                    dr = r['dose_rate_uSvh']
                    bar = '█' * min(50, int(dr * 10))
                    flag = ' <<< SPIKE' if dr >= spike_threshold else ''
                    f.write(f"    {r['wall_time'][11:23]}  {dr:7.4f} µSv/h  {bar}{flag}\n")
                f.write("\n" + "-"*70 + "\n\n")
        print(f"\n  *** SPIKE #{len(spike_events)} logged: peak={ev['peak_uSvh']:.4f} µSv/h  dur={ev['duration_s']:.1f}s ***")

    detector = SpikeDetector(spike_threshold, spike_end, pre_s, post_s, on_spike)

    csv_f = open(csv_path, 'w', newline='', encoding='utf-8')
    csv_w = csv.writer(csv_f)
    csv_w.writerow(['wall_time', 'dose_rate_uSvh', 'dose_uSv', 'cpm'])

    count     = 0
    last_disp = 0

    with serial.Serial(port_name, BAUD, timeout=3) as port:
        port.reset_input_buffer()
        port.write(make_packet(bytes([0x0e, 0x00])))
        time.sleep(0.4)
        port.reset_input_buffer()
        port.write(make_packet(bytes([0x0e, 0x01])))
        time.sleep(0.3)

        # Drain start-ACK
        peek = bytearray(port.read(port.in_waiting or 0))
        line_buf = bytearray()
        if peek and peek[0] == 0xAA and len(peek) > 1:
            skip = 2 + peek[1]
            if len(peek) > skip:
                line_buf.extend(peek[skip:])
        else:
            line_buf.extend(peek)

        try:
            while True:
                avail = port.in_waiting
                if avail:
                    line_buf.extend(port.read(avail))
                else:
                    time.sleep(0.02)
                    continue

                while b'\n' in line_buf:
                    idx      = line_buf.index(b'\n')
                    raw_line = line_buf[:idx]
                    line_buf = line_buf[idx+1:]
                    text = raw_line.decode('ascii', errors='replace').strip()
                    if not text or text[0] == chr(0xAA):
                        continue
                    rec = parse_live_line(text)
                    if rec is None:
                        continue

                    count += 1
                    dr  = rec['dose_rate_uSvh']
                    cpm = rec['cpm']

                    # Store for HTTP server
                    live_store.append({
                        'ts_ms':          rec['ts_ms'],
                        'dose_rate_uSvh': dr,
                        'dose_uSv':       rec['dose_uSv'],
                        'cpm':            cpm,
                    })

                    # Write CSV
                    csv_w.writerow([rec['wall_time'], f"{dr:.4f}",
                                    f"{rec['dose_uSv']:.3f}", cpm])
                    if count % 10 == 0:
                        csv_f.flush()

                    # Spike detection
                    detector.feed(rec)

                    # Console display
                    now = time.monotonic()
                    if not quiet and (now - last_disp >= 1.0 or dr >= spike_threshold):
                        flag  = '  *** SPIKE ***' if dr >= spike_threshold else \
                                '  ^ elevated'    if dr >= BG_HIGH        else ''
                        ts    = rec['wall_time'][11:19]
                        bar   = '█' * min(50, int(dr * 100))
                        print(f"\r  {ts}  {dr:7.4f} µSv/h  CPM={cpm:>5}  {bar:<50}{flag}   ",
                              end='', flush=True)
                        last_disp = now

        except Exception as e:
            if 'KeyboardInterrupt' not in str(type(e)):
                print(f"\nMonitor error: {e}")

    csv_f.close()
    port.write(make_packet(bytes([0x0e, 0x00])))


# ═══════════════════════════════════════════════════════════════════════════
# WRITE CLEAN CSVs
# ═══════════════════════════════════════════════════════════════════════════

def write_csvs(dose_records, rate_hist, rlive_records, out_dir):
    def wcsv(path, rows, fields):
        if not rows:
            return
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(rows)
        print(f"  CSV → {path} ({len(rows)} rows)")

    wcsv(os.path.join(out_dir, f"dose_curve_{STAMP}.csv"),
         dose_records, ['timestamp', 'dose_uSv', 'raw'])
    wcsv(os.path.join(out_dir, f"rate_live_{STAMP}.csv"),
         rlive_records, ['timestamp', 'elapsed_s', 'dose_rate_uSvh', 'raw'])
    if rate_hist:
        wcsv(os.path.join(out_dir, f"rate_hist_{STAMP}.csv"),
             rate_hist, ['timestamp', 'dose_rate_uSvh', 'raw'])

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="FS-5000 All-In-One: extract → report → live monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument('--port',             help='Serial port, e.g. COM3')
    ap.add_argument('--out',              default='.', metavar='DIR',
                    help='Output directory (default: current dir)')
    ap.add_argument('--load-dir',         metavar='DIR',
                    help='Load existing .bin files instead of extracting')
    ap.add_argument('--no-extract',       action='store_true',
                    help='Skip extraction entirely')
    ap.add_argument('--report-only',      action='store_true',
                    help='Write historical report only, skip live monitor')
    ap.add_argument('--spike-threshold',  type=float,
                    default=DEFAULT_SPIKE_THRESHOLD, metavar='N',
                    help=f'µSv/h spike threshold (default {DEFAULT_SPIKE_THRESHOLD})')
    ap.add_argument('--spike-end',        type=float,
                    default=DEFAULT_SPIKE_END, metavar='N',
                    help=f'µSv/h spike-end level (default {DEFAULT_SPIKE_END})')
    ap.add_argument('--pre',              type=int, default=PRE_CONTEXT_S, metavar='S',
                    help=f'Pre-spike context seconds (default {PRE_CONTEXT_S})')
    ap.add_argument('--post',             type=int, default=POST_CONTEXT_S, metavar='S',
                    help=f'Post-spike context seconds (default {POST_CONTEXT_S})')
    ap.add_argument('--live-port',        type=int, default=8765, metavar='PORT',
                    help='HTTP port for live graph (default 8765)')
    ap.add_argument('--quiet',            action='store_true',
                    help='Suppress per-reading console output')
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)
    print(f"Output: {out_dir}")

    # ── STEP 1: Get raw binary data ──────────────────────────────────────────
    raw = {}
    if args.load_dir:
        print(f"\nLoading .bin files from {args.load_dir} ...")
        raw = load_bins(args.load_dir)
    elif not args.no_extract:
        if not HAS_SERIAL:
            print("ERROR: pyserial required for extraction.  pip install pyserial")
            sys.exit(1)
        port_name = find_port(args.port)
        raw = extract_all(port_name, out_dir)
    else:
        print("Skipping extraction (--no-extract). Loading from --out dir...")
        raw = load_bins(out_dir)

    # ── STEP 2: Decode ───────────────────────────────────────────────────────
    print("\nDecoding...")
    dose_records  = decode_dose(raw.get('dose_curve',  b'')) if raw.get('dose_curve')  else []
    rate_hist     = decode_rate_hist(raw.get('rate_curve', b'')) if raw.get('rate_curve') else []
    rlive_records = decode_rate_live(raw.get('rate_curve', b'')) if raw.get('rate_curve') else []
    version_str   = decode_version(raw.get('get_version', b'')) if raw.get('get_version') else ''

    print(f"  Dose records:      {len(dose_records)}")
    print(f"  Rate live records: {len(rlive_records)}")
    print(f"  Rate hist records: {len(rate_hist)}")
    print(f"  Version:           {version_str!r}")

    # ── STEP 3: Write CSVs ───────────────────────────────────────────────────
    if dose_records or rlive_records:
        print("\nWriting CSVs...")
        write_csvs(dose_records, rate_hist, rlive_records, out_dir)

    # ── STEP 4: Historical HTML report ──────────────────────────────────────
    print("\nBuilding historical report...")
    report_path = build_report_html(dose_records, rlive_records, version_str, out_dir)
    if report_path:
        webbrowser.open(f"file:///{report_path.replace(os.sep, '/')}")
        print(f"  Opened in browser: {report_path}")

    if args.report_only:
        print("\nDone (--report-only).")
        return

    # ── STEP 5: Live monitor ─────────────────────────────────────────────────
    if not HAS_SERIAL:
        print("\nERROR: pyserial required for live monitor.  pip install pyserial")
        return

    port_name = find_port(args.port)
    print(f"\n{'='*60}")
    print(f"LIVE MONITOR starting on {port_name}")
    print(f"  Spike threshold: {args.spike_threshold} µSv/h")
    print(f"  Spike log:       {os.path.join(out_dir, f'spikes_{STAMP}.txt')}")
    print(f"  Live graph:      http://localhost:{args.live_port}")
    print(f"  Ctrl+C to stop")
    print('='*60)

    # Shared data store
    store = LiveDataStore()

    # Start HTTP server
    LiveHTTPHandler.store = store
    httpd = http.server.HTTPServer(('localhost', args.live_port), LiveHTTPHandler)
    http_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    http_thread.start()

    # Open live graph in browser
    time.sleep(0.5)
    webbrowser.open(f"http://localhost:{args.live_port}")
    print(f"\nLive graph opened at http://localhost:{args.live_port}")
    print("Reading from device...\n")

    # Run monitor (blocks until Ctrl+C)
    try:
        run_live_monitor(
            port_name       = port_name,
            out_dir         = out_dir,
            spike_threshold = args.spike_threshold,
            spike_end       = args.spike_end,
            pre_s           = args.pre,
            post_s          = args.post,
            live_store      = store,
            quiet           = args.quiet,
        )
    except KeyboardInterrupt:
        pass

    httpd.shutdown()
    total = len(store.all())
    print(f"\n\nSession complete. {total} live records.")
    spike_log = os.path.join(out_dir, f"spikes_{STAMP}.txt")
    if os.path.exists(spike_log):
        print(f"Spike log: {spike_log}")
    print(f"Live CSV: {os.path.join(out_dir, f'live_{STAMP}.csv')}")


if __name__ == '__main__':
    main()
