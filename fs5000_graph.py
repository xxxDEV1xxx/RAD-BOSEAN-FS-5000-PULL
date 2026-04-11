#!/usr/bin/env python3
"""
fs5000_graph.py — Re-decode FS-5000 binary dumps and produce HTML visualization
Made by : Christopher T. WIlliams

Run in the directory containing your .bin files:
    python fs5000_graph.py
    python fs5000_graph.py --dose  dose_curve_20260324_212459.bin
                           --rate  rate_curve_20260324_212459.bin
                           --out   C:\\fs5000_data

Opens a standalone HTML file with interactive charts (no internet needed).
"""

import argparse
import csv
import datetime
import glob
import json
import os
import struct
import sys


# ---------------------------------------------------------------------------
# Binary frame parser
# ---------------------------------------------------------------------------

def checksum(data: bytes) -> int:
    return sum(data) % 256

def extract_frames(raw: bytes) -> list[bytes]:
    """Pull all valid AA..55 frames from raw bytes, return list of payloads."""
    frames = []
    i = 0
    while i < len(raw):
        if raw[i] != 0xAA:
            i += 1
            continue
        if i + 1 >= len(raw):
            break
        length = raw[i + 1]
        end = i + 2 + length
        if end > len(raw):
            i += 1
            continue
        frame   = raw[i:end]
        payload = frame[2:-2]
        rx_cs   = frame[-2]
        tr      = frame[-1]
        if tr != 0x55:
            i += 1
            continue
        calc = checksum(frame[:2 + len(payload)])
        if calc != rx_cs:
            i += 1
            continue
        frames.append(payload)
        i = end
    return frames


# ---------------------------------------------------------------------------
# Curve decoder — works from frame payloads directly
# ---------------------------------------------------------------------------

def decode_curve_from_frames(frames: list[bytes], curve_type: str,
                              cmd_byte: int) -> list[dict]:
    """
    Frame 0: ACK  →  [cmd][0x06][num_packets:1][num_records:2]
    Frame 1+: data →  [cmd][seq:1][8-byte records...]

    Each 8-byte record: >I I  (uint32 timestamp, uint32 value)
    """
    if not frames:
        return []

    # Parse ACK
    ack = frames[0]
    if len(ack) < 5:
        print(f"  ACK frame too short: {ack.hex()}")
        return []
    if ack[0] != cmd_byte:
        print(f"  ACK cmd mismatch: got 0x{ack[0]:02x}, expected 0x{cmd_byte:02x}")
        return []
    if ack[1] != 0x06:
        print(f"  NACK: {ack.hex()}")
        return []

    num_packets = ack[2]
    num_records = struct.unpack("!H", ack[3:5])[0]
    print(f"  ACK: num_packets={num_packets}, num_records={num_records}")

    # Reassemble data bytes from data frames
    raw_data = bytearray()
    for f in frames[1:]:
        if len(f) < 3:
            continue
        if f[0] != cmd_byte:
            continue
        # f[0]=cmd, f[1]=seq, f[2:]=data
        raw_data.extend(f[2:])

    print(f"  Raw data bytes: {len(raw_data)}")
    return decode_8byte_records(bytes(raw_data), num_records, curve_type)


def decode_8byte_records(raw: bytes, num_records: int,
                          curve_type: str) -> list[dict]:
    """
    Strict 8-byte record decode: >II (uint32 ts, uint32 value).
    Validates timestamps are in plausible range (2020–2035).
    """
    MIN_TS = 1577836800   # 2020-01-01
    MAX_TS = 2051222400   # 2035-01-01

    records = []
    available = len(raw) // 8
    count = min(num_records, available)

    for i in range(count):
        off = i * 8
        ts_raw, val_raw = struct.unpack_from(">II", raw, off)

        if ts_raw == 0 and val_raw == 0:
            continue  # padding

        if not (MIN_TS <= ts_raw <= MAX_TS):
            continue  # bogus timestamp — skip

        try:
            dt = datetime.datetime.fromtimestamp(ts_raw,
                                                  tz=datetime.timezone.utc)
            ts_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_ms  = int(ts_raw * 1000)  # for JS charts
        except (OSError, OverflowError):
            continue

        if curve_type == "rate":
            value = round(val_raw * 0.01, 4)
            records.append({
                "timestamp": ts_iso,
                "unix": ts_raw,
                "ts_ms": ts_ms,
                "dose_rate_uSvh": value,
                "raw": val_raw,
            })
        else:
            value = round(val_raw * 0.001, 6)
            records.append({
                "timestamp": ts_iso,
                "unix": ts_raw,
                "ts_ms": ts_ms,
                "dose_uSv": value,
                "raw": val_raw,
            })

    return records


# ---------------------------------------------------------------------------
# Decode get_dose snapshot
# ---------------------------------------------------------------------------

def decode_dose_snapshot(raw: bytes) -> dict:
    """
    get_dose response: aa 12 07 06 <data...> 55
    After framing strip: [0x07][0x06][fields...]
    Fields are little-endian uint32 values based on observed layout.
    """
    frames = extract_frames(raw)
    if not frames:
        return {}
    payload = frames[0]
    if len(payload) < 2 or payload[0] != 0x07 or payload[1] != 0x06:
        return {}
    data = payload[2:]
    result = {"raw_hex": data.hex()}
    # Best-effort field extraction (4-byte LE chunks)
    for i in range(len(data) // 4):
        val = struct.unpack_from("<I", data, i * 4)[0]
        result[f"field_{i}"] = val
    return result


# ---------------------------------------------------------------------------
# File auto-detection
# ---------------------------------------------------------------------------

def find_latest(pattern: str) -> str | None:
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else None


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(path: str, records: list[dict]) -> None:
    if not records:
        return
    keys = [k for k in records[0].keys() if k != "ts_ms"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)
    print(f"  CSV: {len(records)} records → {path}")


# ---------------------------------------------------------------------------
# HTML chart builder
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>FS-5000 Radiation Data — {title_date}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0d1117;
    color: #e6edf3;
    font-family: 'Segoe UI', system-ui, monospace;
    padding: 24px;
  }
  h1 { color: #58a6ff; font-size: 1.4rem; margin-bottom: 4px; }
  .subtitle { color: #8b949e; font-size: 0.85rem; margin-bottom: 24px; }
  .stats-bar {
    display: flex; gap: 16px; flex-wrap: wrap;
    margin-bottom: 28px;
  }
  .stat {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 12px 18px;
    min-width: 160px;
  }
  .stat-label { color: #8b949e; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }
  .stat-value { color: #58a6ff; font-size: 1.5rem; font-weight: 600; margin-top: 4px; }
  .stat-unit  { color: #8b949e; font-size: 0.8rem; }
  .stat.warn  .stat-value { color: #f0883e; }
  .stat.alert .stat-value { color: #f85149; }
  .chart-section {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 24px;
  }
  .chart-section h2 { color: #58a6ff; font-size: 1rem; margin-bottom: 16px; }
  .chart-wrap { position: relative; height: 300px; }
  .alarm-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.82rem;
  }
  .alarm-table th {
    text-align: left;
    padding: 6px 10px;
    background: #21262d;
    color: #8b949e;
    border-bottom: 1px solid #30363d;
  }
  .alarm-table td {
    padding: 5px 10px;
    border-bottom: 1px solid #21262d;
    color: #e6edf3;
  }
  .alarm-table tr:hover td { background: #21262d; }
  .footer { color: #8b949e; font-size: 0.75rem; margin-top: 24px; text-align: center; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
           font-size: 0.75rem; font-weight: 600; margin-left: 8px; }
  .badge-ok     { background: #1a3a2a; color: #3fb950; }
  .badge-warn   { background: #3a2a1a; color: #f0883e; }
  .badge-alert  { background: #3a1a1a; color: #f85149; }
</style>
</head>
<body>

<h1>&#9762; FS-5000 Radiation Monitor — Data Export</h1>
<div class="subtitle">Bosean FS-5000 · Extracted {title_date} · {total_records} total records</div>

<div class="stats-bar" id="statsBar"></div>

<div class="chart-section">
  <h2>&#128200; Cumulative Dose (µSv) <span id="doseBadge" class="badge"></span></h2>
  <div class="chart-wrap"><canvas id="doseChart"></canvas></div>
</div>

<div class="chart-section">
  <h2>&#9889; Dose Rate (µSv/h) <span id="rateBadge" class="badge"></span></h2>
  <div class="chart-wrap"><canvas id="rateChart"></canvas></div>
</div>

<div class="chart-section" id="alarmSection" style="display:none">
  <h2>&#128680; Alarm Events</h2>
  <table class="alarm-table" id="alarmTable">
    <thead><tr><th>#</th><th>Raw Entry</th></tr></thead>
    <tbody id="alarmBody"></tbody>
  </table>
</div>

<div class="footer">
  FS-5000 stock firmware · Binary protocol reverse-engineered ·
  Background reference: 0.05–0.35 µSv/h natural background
</div>

<script>
const DOSE_DATA  = {dose_json};
const RATE_DATA  = {rate_json};
const ALARM_DATA = {alarm_json};
const SNAPSHOT   = {snapshot_json};
const DEVICE_INFO = {device_json};

// Natural background reference band
const BG_LOW  = 0.05;
const BG_HIGH = 0.35;
// Alert threshold (× background high)
const ALERT_THRESH = 1.0;   // µSv/h — flag if sustained > this

// ---- Stats bar ----
function addStat(label, value, unit, cls='') {
  const bar = document.getElementById('statsBar');
  const d = document.createElement('div');
  d.className = 'stat' + (cls ? ' '+cls : '');
  d.innerHTML = `<div class="stat-label">${label}</div>
                 <div class="stat-value">${value}</div>
                 <div class="stat-unit">${unit}</div>`;
  bar.appendChild(d);
}

// Cumulative dose
if (DOSE_DATA.length) {
  const lastDose = DOSE_DATA[DOSE_DATA.length-1].dose_uSv;
  const cls = lastDose > 1000 ? 'alert' : lastDose > 100 ? 'warn' : '';
  addStat('Cumulative Dose', lastDose.toFixed(3), 'µSv', cls);
  addStat('Dose Records', DOSE_DATA.length, 'data points');
  const span = Math.round((DOSE_DATA[DOSE_DATA.length-1].unix
                           - DOSE_DATA[0].unix) / 86400);
  addStat('Log Span', span, 'days');
}

// Rate stats
if (RATE_DATA.length) {
  const rates = RATE_DATA.map(r => r.dose_rate_uSvh).filter(v => v > 0);
  if (rates.length) {
    const avg  = rates.reduce((a,b)=>a+b,0)/rates.length;
    const max  = Math.max(...rates);
    const cls  = max > ALERT_THRESH ? 'alert' : max > BG_HIGH ? 'warn' : '';
    addStat('Avg Dose Rate', avg.toFixed(3), 'µSv/h');
    addStat('Peak Dose Rate', max.toFixed(3), 'µSv/h', cls);
  }
  addStat('Rate Records', RATE_DATA.length, 'data points');
}

if (DEVICE_INFO.version) addStat('Firmware', DEVICE_INFO.version, '');

// ---- Badge helper ----
function setBadge(id, value, low, high) {
  const el = document.getElementById(id);
  if (!el) return;
  if (value > high) { el.textContent='ELEVATED'; el.className='badge badge-alert'; }
  else if (value > low) { el.textContent='ABOVE BG'; el.className='badge badge-warn'; }
  else { el.textContent='NORMAL'; el.className='badge badge-ok'; }
}

if (DOSE_DATA.length) {
  const lastDose = DOSE_DATA[DOSE_DATA.length-1].dose_uSv;
  setBadge('doseBadge', lastDose, 50, 500);
}
if (RATE_DATA.length) {
  const rates = RATE_DATA.map(r=>r.dose_rate_uSvh).filter(v=>v>0);
  if (rates.length) {
    const avg = rates.reduce((a,b)=>a+b,0)/rates.length;
    setBadge('rateBadge', avg, BG_HIGH*0.5, ALERT_THRESH);
  }
}

// ---- Chart defaults ----
Chart.defaults.color = '#8b949e';
Chart.defaults.borderColor = '#21262d';

const commonOptions = (yLabel) => ({
  responsive: true,
  maintainAspectRatio: false,
  interaction: { mode: 'index', intersect: false },
  plugins: {
    legend: { display: false },
    tooltip: {
      backgroundColor: '#161b22',
      borderColor: '#30363d',
      borderWidth: 1,
      titleColor: '#58a6ff',
      bodyColor: '#e6edf3',
    }
  },
  scales: {
    x: {
      type: 'time',
      time: { tooltipFormat: 'yyyy-MM-dd HH:mm', displayFormats: {
        hour: 'MM/dd HH:mm', day: 'MM/dd', week: 'MM/dd', month: 'MMM yy'
      }},
      grid: { color: '#21262d' },
      ticks: { color: '#8b949e', maxRotation: 30 }
    },
    y: {
      title: { display: true, text: yLabel, color: '#8b949e' },
      grid: { color: '#21262d' },
      ticks: { color: '#8b949e' },
      beginAtZero: true,
    }
  }
});

// ---- Dose curve chart ----
if (DOSE_DATA.length) {
  const dosePoints = DOSE_DATA.map(r => ({ x: r.ts_ms, y: r.dose_uSv }));
  new Chart(document.getElementById('doseChart'), {
    type: 'line',
    data: {
      datasets: [{
        label: 'Cumulative Dose (µSv)',
        data: dosePoints,
        borderColor: '#58a6ff',
        backgroundColor: 'rgba(88,166,255,0.08)',
        borderWidth: 1.5,
        pointRadius: dosePoints.length > 200 ? 0 : 2,
        pointHoverRadius: 4,
        fill: true,
        tension: 0.2,
      }]
    },
    options: commonOptions('Dose (µSv)'),
  });
} else {
  document.getElementById('doseChart').parentElement.innerHTML =
    '<p style="color:#8b949e;padding:20px">No dose curve data decoded.</p>';
}

// ---- Rate curve chart ----
if (RATE_DATA.length) {
  const ratePoints = RATE_DATA.map(r => ({ x: r.ts_ms, y: r.dose_rate_uSvh }));

  // Background reference band as annotation-style dataset
  const bgLow  = RATE_DATA.map(r => ({ x: r.ts_ms, y: BG_LOW  }));
  const bgHigh = RATE_DATA.map(r => ({ x: r.ts_ms, y: BG_HIGH }));

  new Chart(document.getElementById('rateChart'), {
    type: 'line',
    data: {
      datasets: [
        {
          label: 'Dose Rate (µSv/h)',
          data: ratePoints,
          borderColor: '#3fb950',
          backgroundColor: 'rgba(63,185,80,0.08)',
          borderWidth: 1.5,
          pointRadius: ratePoints.length > 200 ? 0 : 2,
          pointHoverRadius: 4,
          fill: false,
          tension: 0.2,
          order: 1,
        },
        {
          label: 'BG High (0.35 µSv/h)',
          data: bgHigh,
          borderColor: 'rgba(240,136,62,0.4)',
          borderWidth: 1,
          borderDash: [4,4],
          pointRadius: 0,
          fill: '+1',
          backgroundColor: 'rgba(240,136,62,0.06)',
          order: 2,
        },
        {
          label: 'BG Low (0.05 µSv/h)',
          data: bgLow,
          borderColor: 'rgba(63,185,80,0.2)',
          borderWidth: 1,
          borderDash: [4,4],
          pointRadius: 0,
          fill: false,
          order: 3,
        },
      ]
    },
    options: {
      ...commonOptions('Dose Rate (µSv/h)'),
      plugins: {
        ...commonOptions('').plugins,
        legend: {
          display: true,
          labels: { color: '#8b949e', boxWidth: 12, font: { size: 11 } }
        }
      }
    },
  });
} else {
  document.getElementById('rateChart').parentElement.innerHTML =
    '<p style="color:#8b949e;padding:20px">No rate curve data decoded.</p>';
}

// ---- Alarm table ----
if (ALARM_DATA.length) {
  document.getElementById('alarmSection').style.display = '';
  const tbody = document.getElementById('alarmBody');
  ALARM_DATA.forEach((a, i) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${i+1}</td><td>${a.entry || JSON.stringify(a)}</td>`;
    tbody.appendChild(tr);
  });
}
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="FS-5000 binary decoder + HTML graph")
    ap.add_argument("--dose",    help="Dose curve .bin file")
    ap.add_argument("--rate",    help="Rate curve .bin file")
    ap.add_argument("--alarms",  help="Alarms .bin file")
    ap.add_argument("--version", help="Version .bin file")
    ap.add_argument("--out",     default=".", help="Output directory")
    ap.add_argument("--no-csv",  action="store_true", help="Skip CSV output")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    # Auto-detect files if not specified
    def latest(pattern):
        matches = sorted(glob.glob(pattern))
        return matches[-1] if matches else None

    dose_file    = args.dose    or latest("dose_curve_*.bin")
    rate_file    = args.rate    or latest("rate_curve_*.bin")
    alarms_file  = args.alarms  or latest("read_alarms_*.bin")
    version_file = args.version or latest("get_version_*.bin")

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    dose_records = []
    rate_records = []
    alarm_records = []
    version_str = ""

    # ---- Version ----
    if version_file and os.path.exists(version_file):
        raw = open(version_file, "rb").read()
        frames = extract_frames(raw)
        if frames and len(frames[0]) > 2:
            try:
                version_str = frames[0][2:].decode("ascii", errors="replace")
                version_str = version_str.split("\x00")[0].strip()
                print(f"Version: {version_str!r}")
            except Exception:
                pass

    # ---- Dose curve ----
    if dose_file and os.path.exists(dose_file):
        print(f"\nDecoding dose curve: {dose_file}")
        raw = open(dose_file, "rb").read()
        frames = extract_frames(raw)
        print(f"  Frames found: {len(frames)}")
        if frames:
            dose_records = decode_curve_from_frames(frames, "dose", 0x03)
            print(f"  Valid records: {len(dose_records)}")
            if dose_records:
                print(f"  First: {dose_records[0]['timestamp']}  "
                      f"{dose_records[0]['dose_uSv']} µSv")
                print(f"  Last:  {dose_records[-1]['timestamp']}  "
                      f"{dose_records[-1]['dose_uSv']} µSv")
                if not args.no_csv:
                    write_csv(os.path.join(out_dir, f"dose_clean_{stamp}.csv"),
                              dose_records)
    else:
        print("No dose curve file found.")

    # ---- Rate curve ----
    if rate_file and os.path.exists(rate_file):
        print(f"\nDecoding rate curve: {rate_file}")
        raw = open(rate_file, "rb").read()
        frames = extract_frames(raw)
        print(f"  Frames found: {len(frames)}")
        if frames:
            rate_records = decode_curve_from_frames(frames, "rate", 0x0f)
            print(f"  Valid records: {len(rate_records)}")
            if rate_records:
                print(f"  First: {rate_records[0]['timestamp']}  "
                      f"{rate_records[0]['dose_rate_uSvh']} µSv/h")
                print(f"  Last:  {rate_records[-1]['timestamp']}  "
                      f"{rate_records[-1]['dose_rate_uSvh']} µSv/h")
                if not args.no_csv:
                    write_csv(os.path.join(out_dir, f"rate_clean_{stamp}.csv"),
                              rate_records)
    else:
        print("No rate curve file found.")

    # ---- Alarms ----
    if alarms_file and os.path.exists(alarms_file):
        print(f"\nDecoding alarms: {alarms_file}")
        raw = open(alarms_file, "rb").read()
        frames = extract_frames(raw)
        # Alarm data is binary records in this device — parse from frame payloads
        # Structure: [0x0f][0x00][num_records? or raw entries]
        # Each alarm entry appears to be ~10 bytes based on 281 bytes / ~28 entries
        alarm_raw = bytearray()
        for f in frames:
            if len(f) > 1:
                alarm_raw.extend(f[1:])  # skip cmd byte
        # Try to extract timestamp+value pairs (10-byte entries observed)
        raw_a = bytes(alarm_raw)
        MIN_TS = 1577836800
        MAX_TS = 2051222400
        i = 0
        while i + 8 <= len(raw_a):
            ts_raw = struct.unpack_from(">I", raw_a, i)[0]
            if MIN_TS <= ts_raw <= MAX_TS:
                try:
                    dt = datetime.datetime.fromtimestamp(
                        ts_raw, tz=datetime.timezone.utc)
                    ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    val = struct.unpack_from(">I", raw_a, i+4)[0]
                    alarm_records.append({"entry": f"{ts}  val={val}  "
                                                   f"raw={raw_a[i:i+8].hex()}"})
                    i += 8
                    continue
                except Exception:
                    pass
            i += 1
        print(f"  Alarm entries decoded: {len(alarm_records)}")

    # ---- HTML graph ----
    if not dose_records and not rate_records:
        print("\nWARNING: No records decoded from binary files.")
        print("Share the .hex.txt files for manual analysis.")
        sys.exit(1)

    title_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    total      = len(dose_records) + len(rate_records)

    html = HTML_TEMPLATE.format(
        title_date    = title_date,
        total_records = total,
        dose_json     = json.dumps(dose_records),
        rate_json     = json.dumps(rate_records),
        alarm_json    = json.dumps(alarm_records),
        snapshot_json = json.dumps({}),
        device_json   = json.dumps({"version": version_str}),
    )

    html_path = os.path.join(out_dir, f"fs5000_report_{stamp}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n{'='*60}")
    print(f"HTML report: {html_path}")
    print(f"  Dose records : {len(dose_records)}")
    print(f"  Rate records : {len(rate_records)}")
    print(f"  Alarm entries: {len(alarm_records)}")
    print(f"\nOpen {html_path} in any browser — no internet required.")
    print("="*60)

    # Try to auto-open
    try:
        import webbrowser
        webbrowser.open(f"file:///{html_path.replace(os.sep, '/')}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
