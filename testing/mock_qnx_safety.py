"""
mock_qnx_safety.py — mock QNX that specifically exercises the Item 6
RESET_EMERGENCY / EMERGENCY_CLEARED handshake, and confirms SEQ_START
sent while locked is never even queued as a real motion command
(this script just observes what the bridge forwards to it).
"""
import socket
import sys
import time
import threading

HOST = sys.argv[1] if len(sys.argv) > 1 else "localhost"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 12345

received_lines = []


def recv_loop(sock):
    buf = ""
    sock.settimeout(0.2)
    while True:
        try:
            data = sock.recv(4096).decode("utf-8", errors="ignore")
        except socket.timeout:
            continue
        except OSError:
            return
        if not data:
            return
        buf += data
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            print(f"[mock-qnx-safety] <- from bridge: {line}")
            received_lines.append(line)
            # Real QNX behavior: respond to RESET_EMERGENCY with EMERGENCY_CLEARED
            if line == "RESET_EMERGENCY":
                time.sleep(0.2)  # simulate QNX processing time
                sock.sendall(b"EMERGENCY_CLEARED\n")
                print("[mock-qnx-safety] -> EMERGENCY_CLEARED (simulating QNX recovery)")


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    print(f"[mock-qnx-safety] connected to bridge at {HOST}:{PORT}")

    t = threading.Thread(target=recv_loop, args=(s,), daemon=True)
    t.start()

    time.sleep(6)
    print("[mock-qnx-safety] lines received from bridge:", received_lines)
    s.close()


if __name__ == "__main__":
    main()
