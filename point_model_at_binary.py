#!/usr/bin/env python3
import sys
import math
from pathlib import Path

CONNECTION_TERMS = ("wizard vision", "wizard_vision")

def entropy(data: bytes) -> float:
    """Shannon entropy — high = packed/encrypted/random"""
    if not data:
        return 0.0
    counts = {}
    for byte in data:
        counts[byte] = counts.get(byte, 0) + 1
    probs = [count / len(data) for count in counts.values()]
    return -sum(p * math.log2(p) for p in probs if p > 0)

def hexdump(data: bytes, length: int = 16) -> str:
    result = []
    for i in range(0, len(data), length):
        chunk = data[i:i + length]
        hex_part = ' '.join(f'{b:02x}' for b in chunk)
        ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in chunk)
        result.append(f'{i:08x}  {hex_part:<{length*3}}  {ascii_part}')
    return '\n'.join(result)

def extract_strings(data: bytes, min_len: int = 4) -> list[str]:
    strings = []
    current = ''
    for b in data:
        if 32 <= b <= 126:
            current += chr(b)
        else:
            if len(current) >= min_len:
                strings.append(current)
            current = ''
    if len(current) >= min_len:
        strings.append(current)
    return strings

def point_model_at_binary(file_path: str):
    path = Path(file_path)
    print(f"🔥 POINTING MODEL AT BINARY: {path.resolve()}")
    print(f"Size: {path.stat().st_size:,} bytes\n")

    with open(path, 'rb') as f:
        header = f.read(4096)          # first 4 KB for analysis
        full_for_strings = f.read(8192) if path.stat().st_size > 4096 else b''

    # Magic + type guess
    magic = ' '.join(f'{b:02x}' for b in header[:16])
    print(f"Magic bytes (first 16): {magic}")

    if header.startswith(b'\x7fELF'):
        print("Type: ELF (Linux/Unix executable/object)")
    elif header.startswith(b'MZ'):
        print("Type: PE (Windows executable)")
    elif header.startswith(b'\xca\xfe\xba\xbe') or header.startswith(b'\xcf\xfa\xed\xfe'):
        print("Type: Mach-O (macOS executable)")
    elif header.startswith(b'PK\x03\x04'):
        print("Type: ZIP / JAR / APK (not a raw binary)")
    else:
        print("Type: Unknown / raw binary / other")

    # Entropy (packed?)
    ent = entropy(header)
    print(f"\nEntropy: {ent:.4f} {'(HIGH → likely packed/encrypted/compressed)' if ent > 6.8 else '(normal text/data)'}")

    # Strings
    print("\n=== TOP 40 INTERESTING STRINGS ===")
    strings = extract_strings(header + full_for_strings)
    for s in strings[:40]:
        if len(s.strip()) > 3:
            print(s[:120])

    print("\n=== CONNECTION HITS ===")
    hits = [s for s in strings if any(term in s.lower() for term in CONNECTION_TERMS)]
    if hits:
        for hit in hits[:20]:
            print(hit[:120])
    else:
        print("No wizard vision / wizard_vision strings found.")

    # Header hexdump
    print("\n=== HEADER HEXDUMP (first 256 bytes) ===")
    print(hexdump(header[:256]))

    print("\n✅ MODEL POINTED. Paste everything above back to Grok.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python point_model_at_binary.py <binary_file>")
        sys.exit(1)
    point_model_at_binary(sys.argv[1])
