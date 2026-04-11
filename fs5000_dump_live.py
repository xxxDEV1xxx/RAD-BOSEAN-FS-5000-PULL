#!/usr/bin/env python3
"""
fs5000.py — Unified Bosean FS-5000 extractor + real-time monitor

Modes:
  # Full pipeline: extract curves + alarms, then start live monitor + HTML
  python fs5000.py --out C:\radiation_logs

  # Historical only (no live monitor)
  python fs5000.py --report-only --out C:\radiation_logs

  # Live monitor only (no curve/alarms pull)
  python fs5000.py --monitor-only --out C:\radiation_logs

  # Tune spike thresholds (absolute µSv/h)
  python fs5000.py --monitor-only --spike-threshold 1.0 --spike-end 0.35 --pre 30 --post 60

Made by: Christopher T. Williams (base dump) + extended monitor
"""

import argparse
import csv
import datetime
import json
import os
import struct
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import serial
import serial.tools.list_ports

# ---------------------------------------------------------------------------
# Constants / IDs
# ---------------------------------------------------------------------------

CH340_VID = 0x1A86
CH340_PID = 0x7523
BAUD      = 115200
STAMP     = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

CMDS = {
    "get_version":     bytes([0x06]),
    "get_dose":        bytes([0x07]),
    "read_dose_curve": bytes([0x03]),
    "read_rate_curve": bytes([0x0f]),
    "read_alarms":     bytes([0x10]),
    "live_start":      bytes([0x0e, 0x01]),
    "live_stop":       bytes([0x0e, 0x00]),
}

# ---------------------------------------------------------------------------
# Framing / checksum
# ---------------------------------------------------------------------------

def checksum(data: bytes) -> int:
    return sum(data) % 256

def make_packet(payload: bytes) -> bytes:
    hdr  = bytes([0xAA, len(payload) + 3])
    body = hdr + payload
    cs   = bytes([checksum(body)])
    return body + cs + bytes([0x55])

# ---------------------------------------------------------------------------
# Port discovery
# ---------------------------------------------------------------------------

def find_port() -> str:
    for p in serial.tools.list_ports.comports():
        if p.vid == CH340_VID and p.pid == CH340_PID:
            print(f"[AUTO] Found FS-5000 on {p.device} [{p.description}]")
            return p.device
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        print("ERROR: No serial ports found. Install CH340 driver.")
        sys.exit(1)
    print("CH340 not auto-detected. Available ports:")
    for p in ports:
        print(f"  {p.device}  {p.description}")
    print("Use --port to specify.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Raw drain
# ---------------------------------------------------------------------------

def drain(port: serial.Serial, wait: float = 3.0) -> bytes:
    buf      = bytearray()
    deadline = time.monotonic() + wait
    last_rx  = time.monotonic()
    while True:
        now = time.monotonic()
        if now > deadline:
            break
        if buf and (now - last_rx) > 1.5:
            break
        avail = port.in_waiting
        if avail:
            chunk = port.read(avail)
            buf.extend(chunk)
            last_rx = time.monotonic()
        else:
            time.sleep(0.05)
    return bytes(buf)

# ---------------------------------------------------------------------------
# Save helpers
# ---------------------------------------------------------------------------

def save_raw(out_dir: str, name: str, data: bytes) -> str:
    path = os.path.join(out_dir, f"{name}_{STAMP}.bin")
    with open(path, "wb") as f:
        f.write(data)
    print(f"  [SAVED] {len(data)} bytes → {path}")
    return path

def save_hex(out_dir: str, name: str, data: bytes) -> str:
    path = os.path.join(out_dir, f"{name}_{STAMP}.hex.txt")
    with open(path, "w") as f:
        f.write(f"# {name}  {STAMP}\n")
        f.write(f"# {len(data)} bytes\n\n")
        for i in range(0, len(data), 16):
            chunk = data[i:i+16]
            hex_part  = " ".join(f"{b:02x}" for b in chunk)
            ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
            f.write(f"{i:06x}  {hex_part:<47}  {ascii_part}\n")
    print(f"  [SAVED] hex dump → {path}")
    return path

def save_text(out_dir: str, name: str, data: bytes) -> str:
    path = os.path.join(out_dir, f"{name}_{STAMP}.txt")
    with open(path, "w", encoding="utf-8", errors="replace") as f:
        f.write(data.decode("ascii", errors="replace"))
    print(f"  [SAVED] text → {path}")
    return path

# ---------------------------------------------------------------------------
# Frame parser (for curves/alarms)
# ---------------------------------------------------------------------------

def parse_frames(raw: bytes) -> list:
    frames = []
    i = 0
    while i < len(raw):
        if raw[i] != 0xAA:
            i += 1
            continue
        if i + 1 >= len(raw):
            break
        length = raw[i+1]
        end    = i + 2 + length
        if end > len(raw):
            break
        frame   = raw[i:end]
        payload = frame[2:-2]
        rx_cs   = frame[-2]
        tr      = frame[-1]
        if tr != 0x55:
            i += 1
            continue
        calc_cs = checksum(frame[:2 + len(payload)])
        if calc_cs != rx_cs:
            i += 1
            continue
        frames.append(payload)
        i = end
    return frames

# ---------------------------------------------------------------------------
# Curve decoder
# ---------------------------------------------------------------------------

def decode_curve_best_effort(raw: bytes, curve_type: str) -> list:
    records = []
    frames = parse_frames(raw)
    if frames:
        print(f"    Found {len(frames)} valid frames in response")
        if frames:
            print(f"    ACK frame: {frames[0].hex()}")
        data_bytes = bytearray()
        for f in frames[1:]:
            if len(f) > 2:
                data_bytes.extend(f[2:])
        raw_data = bytes(data_bytes)
    else:
        raw_data = raw

    print(f"    Curve data bytes available: {len(raw_data)}")
    if len(raw_data) < 6:
        print("    Too few bytes to decode curve records.")
        return records

    for rec_size, fmt in [(8, ">II"), (6, ">IH")]:
        if len(raw_data) < rec_size:
            continue
        n_possible = len(raw_data) // rec_size
        trial = []
        for i in range(n_possible):
            off = i * rec_size
            ts_raw, val_raw = struct.unpack_from(fmt, raw_data, off)
            if ts_raw == 0 and val_raw == 0:
                continue
            if not (1262304000 <= ts_raw <= 2208988800):
                continue
            try:
                dt = datetime.datetime.utcfromtimestamp(ts_raw)
                ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                ts = f"unix:{ts_raw}"

            if curve_type == "rate":
                value = round(val_raw * 0.01, 4)
                trial.append({"timestamp": ts, "unix": ts_raw,
                              "dose_rate_uSvh": value, "raw": val_raw})
            else:
                value = round(val_raw * 0.001, 6)
                trial.append({"timestamp": ts, "unix": ts_raw,
                              "dose_uSv": value, "raw": val_raw})

        if len(trial) > len(records):
            records = trial
            print(f"    Decoded {len(records)} records using {rec_size}-byte format")

    return records

def save_csv(out_dir: str, name: str, records: list) -> None:
    if not records:
        print(f"    No decoded records for {name}")
        return
    path = os.path.join(out_dir, f"{name}_{STAMP}.csv")
    keys = list(records[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(records)
    print(f"  [CSV] {len(records)} records → {path}")

# ---------------------------------------------------------------------------
# Command wrapper
# ---------------------------------------------------------------------------

def pull_command(port: serial.Serial, name: str, payload: bytes,
                 out_dir: str, wait: float = 4.0) -> bytes:
    print(f"\n{'='*60}")
    print(f"CMD: {name}  payload={payload.hex()}")
    pkt = make_packet(payload)
    print(f"  Sending: {pkt.hex()}")

    port.reset_input_buffer()
    time.sleep(0.1)
    port.write(pkt)

    print(f"  Waiting up to {wait}s for response...")
    raw = drain(port, wait=wait)

    print(f"  Response: {len(raw)} bytes")
    if raw:
        preview = raw[:64].hex()
        print(f"  First 64 bytes: {preview}")
        ascii_prev = "".join(chr(b) if 32 <= b < 127 else "." for b in raw[:64])
        print(f"  ASCII preview:  {ascii_prev}")
        save_raw(out_dir, name, raw)
        save_hex(out_dir, name, raw)
    else:
        print("  *** NO RESPONSE ***")

    return raw

# ---------------------------------------------------------------------------
# Background / anomaly engine
# ---------------------------------------------------------------------------

def median(values):
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return 0.5 * (s[mid - 1] + s[mid])

def mad(values, med):
    if not values:
        return 0.0
    dev = [abs(v - med) for v in values]
    return median(dev)

class AnomalyEngine:
    def __init__(self, out_dir, spike_threshold=1.0, spike_end=0.35,
                 pre_seconds=30, post_seconds=60):
        self.out_dir = out_dir
        self.spike_threshold = spike_threshold
        self.spike_end = spike_end
        self.pre_seconds = pre_seconds
        self.post_seconds = post_seconds

        self.live_buffer = deque(maxlen=600)   # last 10 min
        self.bg_buffer   = deque(maxlen=900)   # last 15 min
        self.pre_buffer  = deque(maxlen=pre_seconds)

        self.current_spike = None
        self.current_plateau = None
        self.events_lock = threading.Lock()
        self.spike_events = []
        self.plateau_events = []

        self.live_csv_path   = os.path.join(out_dir, f"live_{STAMP}.csv")
        self.spikes_csv_path = os.path.join(out_dir, f"spikes_{STAMP}.csv")
        self.spikes_txt_path = os.path.join(out_dir, f"spikes_{STAMP}.txt")
        self.plateau_csv_path = os.path.join(out_dir, f"plateaus_{STAMP}.csv")

        with open(self.live_csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp_iso", "unix", "dose_rate_uSvh"])

        with open(self.spikes_csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["event_id", "phase", "timestamp_iso", "unix",
                        "dose_rate_uSvh"])

        with open(self.plateau_csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["event_id", "start_iso", "end_iso", "duration_s",
                        "mean_uSvh", "bg_uSvh", "ratio"])

        with open(self.spikes_txt_path, "w", encoding="utf-8") as f:
            f.write(f"# FS-5000 spike log {STAMP}\n\n")

        self.next_spike_id = 1
        self.next_plateau_id = 1

    def add_sample(self, ts_unix, dose_rate):
        ts_iso = datetime.datetime.fromtimestamp(ts_unix).isoformat()
        self.live_buffer.append((ts_unix, dose_rate))
        self.bg_buffer.append(dose_rate)
        self.pre_buffer.append((ts_unix, dose_rate))

        with open(self.live_csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([ts_iso, ts_unix, dose_rate])

        bg = median(self.bg_buffer) if len(self.bg_buffer) >= 30 else dose_rate
        sigma = mad(self.bg_buffer, bg) * 1.4826 if len(self.bg_buffer) >= 30 else 0.0
        R = dose_rate / bg if bg > 0 else 1.0

        self._update_spike(ts_unix, ts_iso, dose_rate, bg, sigma, R)
        self._update_plateau(ts_unix, ts_iso, dose_rate, bg, sigma, R)

    def _update_spike(self, ts_unix, ts_iso, dose_rate, bg, sigma, R):
        if self.current_spike is None:
            if dose_rate >= self.spike_threshold:
                self.current_spike = {
                    "id": self.next_spike_id,
                    "start": ts_unix,
                    "end": ts_unix,
                    "samples": [],
                    "phase": "spike",
                    "bg": bg,
                }
                self.next_spike_id += 1
                for t0, d0 in self.pre_buffer:
                    self.current_spike["samples"].append(("pre", t0, d0))
                self.current_spike["samples"].append(("spike", ts_unix, dose_rate))
        else:
            self.current_spike["samples"].append(("spike", ts_unix, dose_rate))
            self.current_spike["end"] = ts_unix
            if dose_rate <= self.spike_end:
                self.current_spike["phase"] = "post"
                self.current_spike["post_until"] = ts_unix + self.post_seconds

        if self.current_spike is not None and self.current_spike.get("phase") == "post":
            if ts_unix >= self.current_spike["post_until"]:
                self._finalize_spike()

    def _finalize_spike(self):
        ev = self.current_spike
        self.current_spike = None
        with self.events_lock:
            self.spike_events.append(ev)
        eid = ev["id"]
        with open(self.spikes_csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for phase, ts, d in ev["samples"]:
                ts_iso = datetime.datetime.fromtimestamp(ts).isoformat()
                w.writerow([eid, phase, ts_iso, ts, d])

        with open(self.spikes_txt_path, "a", encoding="utf-8") as f:
            f.write(f"Spike #{eid}\n")
            f.write(f"  BG ~ {ev['bg']:.4f} µSv/h\n")
            t0 = ev["start"]
            for phase, ts, d in ev["samples"]:
                rel = ts - t0
                bar = "#" * max(1, int(d * 10))
                f.write(f"  {rel:6.1f}s  {phase:5s}  {d:7.4f}  {bar}\n")
            f.write("\n")

    def _update_plateau(self, ts_unix, ts_iso, dose_rate, bg, sigma, R):
        if len(self.live_buffer) < 30:
            return

        window = list(self.live_buffer)[-30:]
        vals = [d for _, d in window]
        dmin, dmax = min(vals), max(vals)
        span = dmax - dmin
        duration = window[-1][0] - window[0][0]

        if self.current_plateau is None:
            if R >= 3.0 and span <= 0.03 and duration >= 30.0:
                self.current_plateau = {
                    "id": self.next_plateau_id,
                    "start": window[0][0],
                    "end": window[-1][0],
                    "vals": vals[:],
                    "bg": bg,
                }
                self.next_plateau_id += 1
        else:
            self.current_plateau["end"] = ts_unix
            self.current_plateau["vals"].append(dose_rate)
            if R < 2.0:
                self._finalize_plateau()

    def _finalize_plateau(self):
        ev = self.current_plateau
        self.current_plateau = None
        start = ev["start"]
        end = ev["end"]
        duration = end - start
        mean_val = sum(ev["vals"]) / len(ev["vals"])
        ratio = mean_val / ev["bg"] if ev["bg"] > 0 else 1.0
        eid = ev["id"]
        with self.events_lock:
            self.plateau_events.append({
                "id": eid,
                "start": start,
                "end": end,
                "duration": duration,
                "mean": mean_val,
                "bg": ev["bg"],
                "ratio": ratio,
            })
        with open(self.plateau_csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                eid,
                datetime.datetime.fromtimestamp(start).isoformat(),
                datetime.datetime.fromtimestamp(end).isoformat(),
                duration,
                mean_val,
                ev["bg"],
                ratio,
            ])

    def get_live_snapshot(self):
        with self.events_lock:
            spikes = list(self.spike_events)
            plateaus = list(self.plateau_events)
        samples = list(self.live_buffer)
        return {
            "samples": samples,
            "spikes": [
                {
                    "id": ev["id"],
                    "start": ev["start"],
                    "end": ev["end"],
                } for ev in spikes
            ],
            "plateaus": [
                {
                    "id": ev["id"],
                    "start": ev["start"],
                    "end": ev["end"],
                    "ratio": ev["ratio"],
                } for ev in plateaus
            ],
        }

# ---------------------------------------------------------------------------
# Live ASCII stream parsing
# ---------------------------------------------------------------------------

def parse_live_ascii(buffer: str):
    """
    Extract dose rate values from ASCII stream like:
    DR:0.18uSv/h;D:58.1uSv;CPS:0001;CPM:000021;AVG:0.13uSv/h;...
    Returns (values, remaining_buffer)
    """
    values = []
    while True:
        start = buffer.find("DR:")
        if start == -1:
            break
        end = buffer.find("uSv/h", start)
        if end == -1:
            break
        segment = buffer[start+3:end]
        try:
            val = float(segment)
            values.append(val)
        except ValueError:
            pass
        buffer = buffer[end+5:]
    return values, buffer

# ---------------------------------------------------------------------------
# HTTP server for live view
# ---------------------------------------------------------------------------

class LiveHandler(BaseHTTPRequestHandler):
    engine: AnomalyEngine = None

    def _send_json(self, obj):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, html: str):
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/live.json":
            snap = self.engine.get_live_snapshot()
            self._send_json(snap)
        else:
            html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>FS-5000 Live</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
<h1>FS-5000 Live Dose Rate</h1>
<canvas id="chart" width="800" height="400"></canvas>
<script>
const ctx = document.getElementById('chart').getContext('2d');
const chart = new Chart(ctx, {{
  type: 'line',
  data: {{
    labels: [],
    datasets: [{{
      label: 'Dose rate (µSv/h)',
      data: [],
      borderColor: 'red',
      fill: false,
      tension: 0.1
    }}]
  }},
  options: {{
    animation: false,
    scales: {{
      x: {{
        type: 'time',
        time: {{
          unit: 'second'
        }}
      }},
      y: {{
        beginAtZero: true
      }}
    }}
  }}
}});
async function update() {{
  const resp = await fetch('/live.json');
  const data = await resp.json();
  const samples = data.samples;
  chart.data.labels = samples.map(s => new Date(s[0]*1000));
  chart.data.datasets[0].data = samples.map(s => s[1]);
  chart.update();
}}
setInterval(update, 1000);
</script>
</body>
</html>"""
            self._send_html(html)

def start_http_server(engine: AnomalyEngine, port=8765):
    LiveHandler.engine = engine
    server = HTTPServer(("127.0.0.1", port), LiveHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"[HTTP] Live view at http://127.0.0.1:{port}/")

# ---------------------------------------------------------------------------
# Historical extraction (your dump logic)
# ---------------------------------------------------------------------------

def run_extraction(port: serial.Serial, out_dir: str, skip_curves=False, skip_live=True):
    print(f"\n{'='*60}")
    print("STEP 0: Passive listen (2s) — checking for unsolicited output...")
    passive = drain(port, wait=2.0)
    if passive:
        print(f"  Device is already sending! {len(passive)} bytes:")
        print(f"  {passive[:128].hex()}")
        save_raw(out_dir, "passive_rx", passive)
        save_hex(out_dir, "passive_rx", passive)
        save_text(out_dir, "passive_rx", passive)
    else:
        print("  No unsolicited output (expected for stock firmware).")

    print(f"\n{'='*60}")
    print("STEP 1: Sending live_stop to clear any active stream state...")
    port.write(make_packet(CMDS["live_stop"]))
    time.sleep(0.5)
    leftover = drain(port, wait=1.0)
    if leftover:
        print(f"  Cleared {len(leftover)} bytes: {leftover.hex()}")
    port.reset_input_buffer()

    if not skip_curves:
        raw_ver = pull_command(port, "get_version", CMDS["get_version"],
                               out_dir, wait=3.0)
        if raw_ver:
            frames = parse_frames(raw_ver)
            if frames:
                try:
                    ver_str = frames[0][2:].decode("ascii",
                                                   errors="replace").strip("\x00 ")
                    print(f"  VERSION: {ver_str!r}")
                except Exception:
                    pass

        pull_command(port, "get_dose", CMDS["get_dose"], out_dir, wait=3.0)

        print(f"\n{'='*60}")
        print("STEP 4: DOSE CURVE PULL (cmd 0x03) — waiting up to 15s...")
        pkt = make_packet(CMDS["read_dose_curve"])
        print(f"  Sending: {pkt.hex()}")
        port.reset_input_buffer()
        time.sleep(0.1)
        port.write(pkt)

        raw_dc = bytearray()
        last_rx = time.monotonic()
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            avail = port.in_waiting
            if avail:
                chunk = port.read(avail)
                raw_dc.extend(chunk)
                last_rx = time.monotonic()
                sys.stdout.write(f"\r  Received: {len(raw_dc)} bytes...")
                sys.stdout.flush()
            elif raw_dc and (time.monotonic() - last_rx) > 2.0:
                break
            else:
                time.sleep(0.05)
        print()
        raw_dc = bytes(raw_dc)
        print(f"  Total received: {len(raw_dc)} bytes")
        if raw_dc:
            save_raw(out_dir, "dose_curve", raw_dc)
            save_hex(out_dir, "dose_curve", raw_dc)
            records = decode_curve_best_effort(raw_dc, "dose")
            save_csv(out_dir, "dose_curve", records)
            if records:
                print(f"\n  *** DOSE CURVE: {len(records)} records ***")
                print(f"  First: {records[0]}")
                print(f"  Last:  {records[-1]}")
        else:
            print("  *** NO RESPONSE TO DOSE CURVE COMMAND ***")

        print(f"\n{'='*60}")
        print("STEP 5: RATE CURVE PULL (cmd 0x0F) — waiting up to 15s...")
        pkt = make_packet(CMDS["read_rate_curve"])
        print(f"  Sending: {pkt.hex()}")
        port.reset_input_buffer()
        time.sleep(0.1)
        port.write(pkt)

        raw_rc = bytearray()
        last_rx = time.monotonic()
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            avail = port.in_waiting
            if avail:
                chunk = port.read(avail)
                raw_rc.extend(chunk)
                last_rx = time.monotonic()
                sys.stdout.write(f"\r  Received: {len(raw_rc)} bytes...")
                sys.stdout.flush()
            elif raw_rc and (time.monotonic() - last_rx) > 2.0:
                break
            else:
                time.sleep(0.05)
        print()
        raw_rc = bytes(raw_rc)
        print(f"  Total received: {len(raw_rc)} bytes")
        if raw_rc:
            save_raw(out_dir, "rate_curve", raw_rc)
            save_hex(out_dir, "rate_curve", raw_rc)
            records = decode_curve_best_effort(raw_rc, "rate")
            save_csv(out_dir, "rate_curve", records)
            if records:
                print(f"\n  *** RATE CURVE: {len(records)} records ***")
                print(f"  First: {records[0]}")
                print(f"  Last:  {records[-1]}")
        else:
            print("  *** NO RESPONSE TO RATE CURVE COMMAND ***")

        pull_command(port, "read_alarms", CMDS["read_alarms"], out_dir, wait=5.0)

    if skip_live:
        print(f"\n{'='*60}")
        print("EXTRACTION COMPLETE.")
        return

# ---------------------------------------------------------------------------
# Live monitor loop
# ---------------------------------------------------------------------------

def run_live_monitor(port: serial.Serial, out_dir: str,
                     spike_threshold: float, spike_end: float,
                     pre: int, post: int, http_port: int):
    engine = AnomalyEngine(out_dir,
                           spike_threshold=spike_threshold,
                           spike_end=spike_end,
                           pre_seconds=pre,
                           post_seconds=post)
    start_http_server(engine, port=http_port)

    print(f"\n{'='*60}")
    print("LIVE MONITOR: starting ASCII stream (0x0E 0x01)...")
    port.reset_input_buffer()
    port.write(make_packet(CMDS["live_start"]))
    time.sleep(0.3)

    buf = ""
    last_tick = time.time()
    print("[MONITOR] Running. Ctrl+C to stop.")
    try:
        while True:
            now = time.time()
            avail = port.in_waiting
            if avail:
                chunk = port.read(avail)
                try:
                    buf += chunk.decode("ascii", errors="ignore")
                except Exception:
                    pass
                vals, buf = parse_live_ascii(buf)
                for v in vals:
                    ts = time.time()
                    engine.add_sample(ts, v)
            if now - last_tick >= 1.0:
                last_tick = now
            time.sleep(0.05)
    except KeyboardInterrupt:
        print("\n[MONITOR] Stopping live stream...")
    finally:
        try:
            port.write(make_packet(CMDS["live_stop"]))
        except Exception:
            pass
        print("[MONITOR] Done.")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="FS-5000 unified extractor + monitor")
    ap.add_argument("--port", help="e.g. COM3")
    ap.add_argument("--out",  default=".", help="Output directory")
    ap.add_argument("--report-only", action="store_true",
                    help="Only pull curves/alarms, no live monitor")
    ap.add_argument("--monitor-only", action="store_true",
                    help="Only run live monitor, no curves/alarms")
    ap.add_argument("--spike-threshold", type=float, default=1.0,
                    help="Absolute spike threshold in µSv/h (default 1.0)")
    ap.add_argument("--spike-end", type=float, default=0.35,
                    help="Spike end threshold in µSv/h (default 0.35)")
    ap.add_argument("--pre", type=int, default=30,
                    help="Pre-spike context seconds (default 30)")
    ap.add_argument("--post", type=int, default=60,
                    help="Post-spike context seconds (default 60)")
    ap.add_argument("--http-port", type=int, default=8765,
                    help="HTTP port for live view (default 8765)")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)
    print(f"Output directory: {out_dir}")

    port_name = args.port or find_port()
    print(f"Opening {port_name} @ {BAUD}...")
    with serial.Serial(port_name, BAUD, timeout=5) as port:
        port.reset_input_buffer()
        time.sleep(0.2)

        if not args.monitor_only:
            run_extraction(port, out_dir,
                           skip_curves=args.report_only,
                           skip_live=True)

        if not args.report_only:
            run_live_monitor(port, out_dir,
                             spike_threshold=args.spike_threshold,
                             spike_end=args.spike_end,
                             pre=args.pre,
                             post=args.post,
                             http_port=args.http_port)

if __name__ == "__main__":
    main()
