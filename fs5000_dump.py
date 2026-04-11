#!/usr/bin/env python3
"""
fs5000_dump.py  —  Bosean FS-5000 EMERGENCY DATA PULL
Tries every known command. Writes ALL raw bytes to disk immediately.
Does not depend on correct framing to save your data.
Made by : Christopher T. WIlliams
Requirements:  pip install pyserial
Usage:         python fs5000_dump.py
               python fs5000_dump.py --port COM3
               python fs5000_dump.py --port COM3 --out C:\\fs5000_data
"""

import argparse
import datetime
import os
import struct
import sys
import time

import serial
import serial.tools.list_ports

CH340_VID = 0x1A86
CH340_PID = 0x7523
BAUD      = 115200
STAMP     = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# ---------------------------------------------------------------------------
# Known command payloads (before framing)
# ---------------------------------------------------------------------------
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
# Framing
# ---------------------------------------------------------------------------

def checksum(data: bytes) -> int:
    return sum(data) % 256

def make_packet(payload: bytes) -> bytes:
    hdr  = bytes([0xAA, len(payload) + 3])
    body = hdr + payload
    cs   = bytes([checksum(body)])
    return body + cs + bytes([0x55])

# ---------------------------------------------------------------------------
# Port
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
# Raw drain — read everything available for up to `wait` seconds
# ---------------------------------------------------------------------------

def drain(port: serial.Serial, wait: float = 3.0) -> bytes:
    """Read all bytes available within `wait` seconds of silence."""
    buf      = bytearray()
    deadline = time.monotonic() + wait
    last_rx  = time.monotonic()
    while True:
        now = time.monotonic()
        if now > deadline:
            break
        # Stop if we've had 1.5s of silence AND got at least something,
        # OR if total wait exceeded
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
# Save raw bytes unconditionally
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
# Frame parser — best-effort, non-fatal
# ---------------------------------------------------------------------------

def parse_frames(raw: bytes) -> list:
    """Extract all valid AA..55 frames from a byte buffer."""
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
# Curve decoder — applied to whatever raw data we got
# ---------------------------------------------------------------------------

def decode_curve_best_effort(raw: bytes, curve_type: str) -> list:
    """
    Try to decode curve records from raw data.
    Handles both 8-byte (uint32+uint32) and 6-byte (uint32+uint16) formats.
    Skips the multi-packet header if present.
    Returns list of dicts.
    """
    records = []

    # If it looks like framed data, extract payload bytes first
    frames = parse_frames(raw)
    if frames:
        print(f"    Found {len(frames)} valid frames in response")
        # First frame is usually ACK with header info
        if frames:
            print(f"    ACK frame: {frames[0].hex()}")
        # Remaining frames are data
        data_bytes = bytearray()
        for f in frames[1:]:
            # Each data frame: [cmd:1][seq:1][data...]
            if len(f) > 2:
                data_bytes.extend(f[2:])
        raw_data = bytes(data_bytes)
    else:
        # No valid frames — try treating the whole buffer as raw curve data
        # Strip leading header bytes if present (first 5-6 bytes may be ACK)
        raw_data = raw

    print(f"    Curve data bytes available: {len(raw_data)}")
    if len(raw_data) < 6:
        print("    Too few bytes to decode curve records.")
        return records

    # Try 8-byte records first
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
            # Sanity check: timestamp should be a plausible Unix time
            # (between 2010-01-01 and 2040-01-01)
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
    import csv
    path = os.path.join(out_dir, f"{name}_{STAMP}.csv")
    keys = list(records[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(records)
    print(f"  [CSV] {len(records)} records → {path}")

# ---------------------------------------------------------------------------
# Main data pull
# ---------------------------------------------------------------------------

def pull_command(port: serial.Serial, name: str, payload: bytes,
                 out_dir: str, wait: float = 4.0) -> bytes:
    """Send a command, drain all response bytes, save everything."""
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
        # Also show ASCII preview
        ascii_prev = "".join(chr(b) if 32 <= b < 127 else "." for b in raw[:64])
        print(f"  ASCII preview:  {ascii_prev}")

        save_raw(out_dir, name, raw)
        save_hex(out_dir, name, raw)
    else:
        print("  *** NO RESPONSE ***")

    return raw

def main():
    ap = argparse.ArgumentParser(description="FS-5000 emergency data dump")
    ap.add_argument("--port", help="e.g. COM3")
    ap.add_argument("--out",  default=".", help="Output directory")
    ap.add_argument("--live-only", action="store_true",
                    help="Only try live stream (skip curves)")
    ap.add_argument("--curves-only", action="store_true",
                    help="Only pull curves (skip live stream)")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out)
    os.makedirs(out_dir, exist_ok=True)
    print(f"Output directory: {out_dir}")

    port_name = args.port or find_port()
    print(f"Opening {port_name} @ {BAUD}...")

    with serial.Serial(port_name, BAUD, timeout=5) as port:
        port.reset_input_buffer()
        time.sleep(0.2)

        # ----------------------------------------------------------------
        # STEP 0: Passive listen — see if device is already sending anything
        # ----------------------------------------------------------------
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

        # ----------------------------------------------------------------
        # STEP 1: Stop any active stream first
        # ----------------------------------------------------------------
        print(f"\n{'='*60}")
        print("STEP 1: Sending live_stop to clear any active stream state...")
        port.write(make_packet(CMDS["live_stop"]))
        time.sleep(0.5)
        leftover = drain(port, wait=1.0)
        if leftover:
            print(f"  Cleared {len(leftover)} bytes: {leftover.hex()}")
        port.reset_input_buffer()

        # ----------------------------------------------------------------
        # STEP 2: Version — confirms comms are working at all
        # ----------------------------------------------------------------
        if not args.live_only:
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

        # ----------------------------------------------------------------
        # STEP 3: Dose snapshot
        # ----------------------------------------------------------------
        if not args.live_only:
            pull_command(port, "get_dose", CMDS["get_dose"], out_dir, wait=3.0)

        # ----------------------------------------------------------------
        # STEP 4: DOSE CURVE  ← the critical data
        # ----------------------------------------------------------------
        if not args.live_only:
            print(f"\n{'='*60}")
            print("STEP 4: DOSE CURVE PULL (cmd 0x03) — waiting up to 15s...")
            pkt = make_packet(CMDS["read_dose_curve"])
            print(f"  Sending: {pkt.hex()}")
            port.reset_input_buffer()
            time.sleep(0.1)
            port.write(pkt)

            # Dose curve can be large — drain aggressively
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
                    break  # 2s silence after data = transfer complete
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

        # ----------------------------------------------------------------
        # STEP 5: RATE CURVE  ← also critical
        # ----------------------------------------------------------------
        if not args.live_only:
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

        # ----------------------------------------------------------------
        # STEP 6: Alarm log
        # ----------------------------------------------------------------
        if not args.live_only:
            pull_command(port, "read_alarms", CMDS["read_alarms"], out_dir, wait=5.0)

        # ----------------------------------------------------------------
        # STEP 7: Live stream attempt (last — device may not return to
        # command mode cleanly after this)
        # ----------------------------------------------------------------
        if not args.curves_only:
            print(f"\n{'='*60}")
            print("STEP 7: LIVE STREAM — trying all known start sequences...")

            # Try A: official 2-byte start  0x0E 0x01
            print("  [A] Sending 0x0E 0x01 start...")
            port.reset_input_buffer()
            port.write(make_packet(bytes([0x0e, 0x01])))
            time.sleep(0.3)
            raw_a = drain(port, wait=4.0)
            if raw_a:
                print(f"  [A] Got {len(raw_a)} bytes: {raw_a[:64].hex()}")
                ascii_a = "".join(chr(b) if 32<=b<127 else "." for b in raw_a[:128])
                print(f"  [A] ASCII: {ascii_a}")
                save_raw(out_dir, "live_attempt_A", raw_a)
                save_hex(out_dir, "live_attempt_A", raw_a)
                save_text(out_dir, "live_attempt_A", raw_a)
            else:
                print("  [A] No response.")

            # Stop
            port.write(make_packet(bytes([0x0e, 0x00])))
            time.sleep(0.3)
            port.reset_input_buffer()

            # Try B: single byte  0x0E  (some firmware variants)
            print("  [B] Sending single byte 0x0E start...")
            port.reset_input_buffer()
            port.write(make_packet(bytes([0x0e])))
            time.sleep(0.3)
            raw_b = drain(port, wait=4.0)
            if raw_b:
                print(f"  [B] Got {len(raw_b)} bytes: {raw_b[:64].hex()}")
                ascii_b = "".join(chr(b) if 32<=b<127 else "." for b in raw_b[:128])
                print(f"  [B] ASCII: {ascii_b}")
                save_raw(out_dir, "live_attempt_B", raw_b)
                save_text(out_dir, "live_attempt_B", raw_b)
            else:
                print("  [B] No response.")

            port.write(make_packet(bytes([0x0e, 0x00])))
            time.sleep(0.3)
            port.reset_input_buffer()

    # ----------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------
    print(f"\n{'='*60}")
    print("COMPLETE. Files written to:", out_dir)
    files = sorted(os.listdir(out_dir))
    for fn in files:
        if STAMP in fn:
            fpath = os.path.join(out_dir, fn)
            size  = os.path.getsize(fpath)
            print(f"  {fn}  ({size} bytes)")
    print("="*60)
    print()
    print("NEXT STEPS:")
    print("  1. If dose_curve_*.csv exists and has data — your data is saved.")
    print("  2. If dose_curve_*.bin is non-empty but CSV is empty,")
    print("     share the .hex.txt file so the binary format can be decoded.")
    print("  3. If ALL .bin files are 0 bytes — device is not responding to")
    print("     commands.  Check: is the FS-5000 screen on? Try unplugging")
    print("     and replugging USB while the device is powered ON.")

if __name__ == "__main__":
    main()
