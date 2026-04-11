#!/usr/bin/env python3
"""
fs5000_monitor.py  —  FS-5000 Live Spike Monitor
Runs continuously. Logs every reading. Captures transient spike events
with pre/post context. Writes CSV + spike event log.
Made by : Christopher T. WIlliams

Designed to catch short-duration spikes (e.g. 1.3-2.0 uSv/h for ~20 seconds)
that a ring buffer or 10-minute average would miss entirely.

Requirements:  pip install pyserial
Usage:
    python fs5000_monitor.py
    python fs5000_monitor.py --port COM3
    python fs5000_monitor.py --port COM3 --spike-threshold 1.0
    python fs5000_monitor.py --port COM3 --out C:\\radiation_logs

Output files (auto-named by date):
    live_YYYYMMDD_HHMMSS.csv       — every reading, full session
    spikes_YYYYMMDD_HHMMSS.csv     — spike events only, with context window
    spikes_YYYYMMDD_HHMMSS.txt     — human-readable spike report
"""

import argparse
import csv
import datetime
import os
import struct
import sys
import time
from collections import deque

import serial
import serial.tools.list_ports

# ── Constants ────────────────────────────────────────────────────────────────
CH340_VID        = 0x1A86
CH340_PID        = 0x7523
BAUD             = 115200
FRAME_HDR        = 0xAA
FRAME_TRL        = 0x55
READ_START       = bytes([0x0e, 0x01])
READ_STOP        = bytes([0x0e, 0x00])

DEFAULT_SPIKE_THRESHOLD   = 1.0    # µSv/h — trigger spike capture
DEFAULT_SPIKE_END_THRESH  = 0.35   # µSv/h — spike considered over when below this
PRE_CONTEXT_SECS          = 30     # seconds of data to keep before spike
POST_CONTEXT_SECS         = 60     # seconds to capture after spike drops below threshold

# ── Framing ──────────────────────────────────────────────────────────────────

def _cs(data):
    return sum(data) % 256

def make_packet(payload):
    hdr  = bytes([FRAME_HDR, len(payload) + 3])
    body = hdr + payload
    return body + bytes([_cs(body)]) + bytes([FRAME_TRL])

# ── Port ─────────────────────────────────────────────────────────────────────

def find_port():
    for p in serial.tools.list_ports.comports():
        if p.vid == CH340_VID and p.pid == CH340_PID:
            return p.device
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        print("ERROR: No serial ports found. Install CH340 driver.")
        sys.exit(1)
    print("CH340 not auto-detected. Ports found:")
    for p in ports:
        print(f"  {p.device}  {p.description}")
    print("Use --port COMx")
    sys.exit(1)

# ── Live stream parser ───────────────────────────────────────────────────────

def parse_live_line(line):
    """
    Parse:  2025-01-18T22:33:26;DR:0.23uSv/h;D:3.70uSv;CPS:0001;CPM:000029;AVG:0.16uSv/h;...
    Returns dict or None.
    """
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
    # Parse dose rate — strip unit suffix
    try:
        dr_str = rec['DR'].replace('uSv/h','').replace('mSv/h','').strip()
        rec['dose_rate_uSvh'] = float(dr_str)
        if 'mSv/h' in rec['DR']:
            rec['dose_rate_uSvh'] *= 1000
    except ValueError:
        return None
    try:
        rec['dose_uSv'] = float(rec.get('D','0').replace('uSv','').replace('mSv',''))
    except ValueError:
        rec['dose_uSv'] = 0.0
    try:
        rec['cpm'] = int(rec.get('CPM','0'))
    except ValueError:
        rec['cpm'] = 0
    rec['wall_time'] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    return rec

# ── Spike detector ───────────────────────────────────────────────────────────

class SpikeDetector:
    """
    State machine:
      IDLE       → watching for DR >= spike_threshold
      ACTIVE     → spike in progress, collecting data
      COOLDOWN   → spike ended, collecting post_context_secs more seconds
    """
    IDLE     = 'IDLE'
    ACTIVE   = 'ACTIVE'
    COOLDOWN = 'COOLDOWN'

    def __init__(self, threshold, end_thresh, pre_secs, post_secs, on_spike):
        self.threshold  = threshold
        self.end_thresh = end_thresh
        self.pre_secs   = pre_secs
        self.post_secs  = post_secs
        self.on_spike   = on_spike   # callback(spike_event_dict)

        self.state      = self.IDLE
        self.pre_buf    = deque()    # rolling pre-spike buffer
        self.event_buf  = []         # records during active spike
        self.cooldown_start = None
        self.spike_peak = 0.0
        self.spike_start_wall = None

    def feed(self, rec):
        dr   = rec['dose_rate_uSvh']
        wall = rec['wall_time']
        now  = time.monotonic()

        if self.state == self.IDLE:
            # Maintain rolling pre-buffer
            self.pre_buf.append(rec)
            # Trim pre-buffer to pre_secs
            while (len(self.pre_buf) > 1 and
                   self._wall_delta(self.pre_buf[0]['wall_time'], wall) > self.pre_secs):
                self.pre_buf.popleft()

            if dr >= self.threshold:
                self.state            = self.ACTIVE
                self.event_buf        = list(self.pre_buf)  # include pre-context
                self.spike_peak       = dr
                self.spike_start_wall = wall
                print(f"\n{'!'*60}")
                print(f"  SPIKE DETECTED  {wall}")
                print(f"  DR = {dr:.4f} µSv/h  (threshold = {self.threshold})")
                print(f"{'!'*60}")

        elif self.state == self.ACTIVE:
            self.event_buf.append(rec)
            self.spike_peak = max(self.spike_peak, dr)

            if dr < self.end_thresh:
                self.state          = self.COOLDOWN
                self.cooldown_start = now
                print(f"  Spike below threshold, collecting {self.post_secs}s context...")

        elif self.state == self.COOLDOWN:
            self.event_buf.append(rec)
            self.spike_peak = max(self.spike_peak, dr)

            if now - self.cooldown_start >= self.post_secs:
                # Emit complete spike event
                self._emit()
                self.state = self.IDLE
                self.pre_buf.clear()
                self.event_buf = []

    def _wall_delta(self, ts_a, ts_b):
        try:
            a = datetime.datetime.fromisoformat(ts_a)
            b = datetime.datetime.fromisoformat(ts_b)
            return abs((b - a).total_seconds())
        except Exception:
            return 0

    def _emit(self):
        if not self.event_buf:
            return
        # Find actual spike window
        spike_recs = [r for r in self.event_buf if r['dose_rate_uSvh'] >= self.threshold]
        duration_s = 0
        if len(spike_recs) >= 2:
            duration_s = self._wall_delta(spike_recs[0]['wall_time'],
                                           spike_recs[-1]['wall_time'])

        event = {
            'spike_start':     self.spike_start_wall,
            'spike_end':       spike_recs[-1]['wall_time'] if spike_recs else '',
            'peak_uSvh':       self.spike_peak,
            'duration_s':      round(duration_s, 1),
            'spike_records':   len(spike_recs),
            'total_records':   len(self.event_buf),
            'pre_context_s':   self.pre_secs,
            'post_context_s':  self.post_secs,
            'all_records':     self.event_buf,
        }
        self.on_spike(event)
        print(f"\n  SPIKE LOGGED: peak={self.spike_peak:.4f} µSv/h  "
              f"duration={duration_s:.1f}s  records={len(spike_recs)}")

# ── Main monitor loop ─────────────────────────────────────────────────────────

def run_monitor(port_name, out_dir, spike_threshold, spike_end_thresh,
                pre_secs, post_secs, quiet=False):

    stamp     = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path  = os.path.join(out_dir, f"live_{stamp}.csv")
    spk_csv   = os.path.join(out_dir, f"spikes_{stamp}.csv")
    spk_txt   = os.path.join(out_dir, f"spikes_{stamp}.txt")

    spike_events = []

    def on_spike(event):
        spike_events.append(event)
        _write_spike_csv(spk_csv, spike_events)
        _write_spike_txt(spk_txt, spike_events)

    detector = SpikeDetector(
        threshold  = spike_threshold,
        end_thresh = spike_end_thresh,
        pre_secs   = pre_secs,
        post_secs  = post_secs,
        on_spike   = on_spike,
    )

    print(f"FS-5000 Live Spike Monitor")
    print(f"  Port:            {port_name}")
    print(f"  Spike threshold: {spike_threshold} µSv/h")
    print(f"  Spike end:       {spike_end_thresh} µSv/h")
    print(f"  Pre-context:     {pre_secs}s")
    print(f"  Post-context:    {post_secs}s")
    print(f"  Live log:        {csv_path}")
    print(f"  Spike log:       {spk_csv}")
    print(f"  Ctrl+C to stop")
    print()

    # Open CSV
    csv_file   = open(csv_path, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['wall_time','device_ts','dose_rate_uSvh','dose_uSv','cpm'])
    csv_file.flush()

    record_count = 0
    last_display = 0

    with serial.Serial(port_name, BAUD, timeout=3) as port:
        port.reset_input_buffer()

        # Send STOP first (clear any stuck state)
        port.write(make_packet(READ_STOP))
        time.sleep(0.4)
        port.reset_input_buffer()

        # Send START
        port.write(make_packet(READ_START))
        time.sleep(0.3)

        # Drain and discard binary start-ACK if present
        peek = bytearray(port.read(port.in_waiting or 0))
        line_buf = bytearray()
        if peek and peek[0] == FRAME_HDR and len(peek) > 1:
            frame_size = 2 + peek[1]
            if len(peek) > frame_size:
                line_buf.extend(peek[frame_size:])
            # else discard entire peek (was just the ACK frame)
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
                    if not text or text[0] == chr(FRAME_HDR):
                        continue

                    rec = parse_live_line(text)
                    if rec is None:
                        continue

                    record_count += 1
                    dr  = rec['dose_rate_uSvh']
                    cpm = rec['cpm']

                    # Write to CSV
                    csv_writer.writerow([
                        rec['wall_time'],
                        rec.get('device_ts',''),
                        f"{dr:.4f}",
                        f"{rec['dose_uSv']:.3f}",
                        cpm,
                    ])
                    if record_count % 10 == 0:
                        csv_file.flush()

                    # Feed spike detector
                    detector.feed(rec)

                    # Display
                    now = time.monotonic()
                    if not quiet and (now - last_display >= 1.0 or dr >= spike_threshold):
                        status = ''
                        if dr >= spike_threshold:
                            status = '  *** SPIKE ***'
                        elif dr >= spike_end_thresh:
                            status = '  ^ above BG'
                        ts_short = rec['wall_time'][11:19]
                        bar_len  = min(60, int(dr * 20))  # 60 chars = 3 uSv/h
                        bar      = '█' * bar_len
                        print(f"\r  {ts_short}  {dr:6.4f} µSv/h  CPM={cpm:>5}  {bar:<60}{status}",
                              end='', flush=True)
                        last_display = now

        except KeyboardInterrupt:
            print(f"\n\nStopping...")

        finally:
            port.write(make_packet(READ_STOP))
            time.sleep(0.3)

    csv_file.close()
    print(f"\nSession complete. {record_count} records.")
    print(f"  Live log:   {csv_path}  ({os.path.getsize(csv_path):,} bytes)")
    if spike_events:
        print(f"  Spike log:  {spk_csv}  ({len(spike_events)} events)")
        print(f"  Spike report: {spk_txt}")
    else:
        print(f"  No spikes above {spike_threshold} µSv/h detected.")

# ── Spike output writers ──────────────────────────────────────────────────────

def _write_spike_csv(path, events):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['event_num','spike_start','spike_end','peak_uSvh',
                    'duration_s','spike_records','record_index',
                    'wall_time','dose_rate_uSvh','dose_uSv','cpm','phase'])
        for ei, ev in enumerate(events):
            spike_times = {r['wall_time'] for r in ev['all_records']
                           if r['dose_rate_uSvh'] >= 1.0}
            for ri, rec in enumerate(ev['all_records']):
                phase = ('pre' if rec['wall_time'] < ev['spike_start']
                         else 'spike' if rec['wall_time'] in spike_times
                         else 'post')
                w.writerow([
                    ei + 1,
                    ev['spike_start'],
                    ev['spike_end'],
                    f"{ev['peak_uSvh']:.4f}",
                    ev['duration_s'],
                    ev['spike_records'],
                    ri,
                    rec['wall_time'],
                    f"{rec['dose_rate_uSvh']:.4f}",
                    f"{rec['dose_uSv']:.3f}",
                    rec['cpm'],
                    phase,
                ])


def _write_spike_txt(path, events):
    with open(path, 'w', encoding='utf-8') as f:
        f.write("FS-5000 SPIKE EVENT REPORT\n")
        f.write(f"Generated: {datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}\n")
        f.write(f"Events: {len(events)}\n")
        f.write("="*70 + "\n\n")
        for ei, ev in enumerate(events):
            f.write(f"EVENT #{ei+1}\n")
            f.write(f"  Start:        {ev['spike_start']}\n")
            f.write(f"  End:          {ev['spike_end']}\n")
            f.write(f"  Peak:         {ev['peak_uSvh']:.4f} µSv/h\n")
            f.write(f"  Duration:     {ev['duration_s']:.1f} seconds\n")
            f.write(f"  Spike records:{ev['spike_records']}\n")
            f.write(f"  Total records:{ev['total_records']} (incl pre/post context)\n")
            f.write(f"\n  Timeline:\n")
            for rec in ev['all_records']:
                dr   = rec['dose_rate_uSvh']
                ts   = rec['wall_time'][11:23]
                bar  = '█' * min(50, int(dr * 10))
                flag = ' <<<' if dr >= 1.0 else ''
                f.write(f"    {ts}  {dr:7.4f} µSv/h  {bar}{flag}\n")
            f.write("\n" + "-"*70 + "\n\n")

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="FS-5000 live spike monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('--port',            help='Serial port (e.g. COM3)')
    ap.add_argument('--out',             default='.', help='Output directory')
    ap.add_argument('--spike-threshold', type=float, default=DEFAULT_SPIKE_THRESHOLD,
                    metavar='N',         help=f'µSv/h to trigger spike capture (default {DEFAULT_SPIKE_THRESHOLD})')
    ap.add_argument('--spike-end',       type=float, default=DEFAULT_SPIKE_END_THRESH,
                    metavar='N',         help=f'µSv/h to end spike (default {DEFAULT_SPIKE_END_THRESH})')
    ap.add_argument('--pre',             type=int,   default=PRE_CONTEXT_SECS,
                    metavar='SECS',      help=f'Seconds of pre-spike context (default {PRE_CONTEXT_SECS})')
    ap.add_argument('--post',            type=int,   default=POST_CONTEXT_SECS,
                    metavar='SECS',      help=f'Seconds of post-spike context (default {POST_CONTEXT_SECS})')
    ap.add_argument('--quiet',           action='store_true',
                    help='Suppress per-reading display (spikes still printed)')
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)

    port = args.port or find_port()

    run_monitor(
        port_name       = port,
        out_dir         = out_dir,
        spike_threshold = args.spike_threshold,
        spike_end_thresh= args.spike_end,
        pre_secs        = args.pre,
        post_secs       = args.post,
        quiet           = args.quiet,
    )

if __name__ == '__main__':
    main()
