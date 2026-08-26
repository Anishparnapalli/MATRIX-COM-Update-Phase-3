"""
mock_qnx_client.py — stands in for the QNX Robotic_ARM binary during
testing when no QNX VM is available. Speaks the EXACT same TCP wire
protocol documented in RTOS_QNX_Momentics_Code.c and bridge.py:

  QNX -> Bridge:
      j0,j1,j2,j3,j4,j5\n
      CYCLE_DONE:<n>\n
      SEQ_COMPLETE\n
      POSE_DONE:<idx>\n
      EMERGENCY_STOP\n
      WATCHDOG_HIT:<ms_over>\n

  Bridge -> QNX:
      SEQ_START:<n>:<dur_ms>\n
      POSE:<idx>:j0,j1,j2,j3,j4,j5\n
      SEQ_END\n
      SEQ_STOP\n
      EMERGENCY_STOP\n

Usage:
    python mock_qnx_client.py [host] [port]
"""
import socket
import sys
import time
import threading

HOST = sys.argv[1] if len(sys.argv) > 1 else "localhost"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 12345


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
            print(f"[mock-qnx] <- from bridge: {line}")


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    print(f"[mock-qnx] connected to bridge at {HOST}:{PORT}")

    t = threading.Thread(target=recv_loop, args=(s,), daemon=True)
    t.start()

    # idle telemetry
    for i in range(3):
        pkt = f"{90+i},{90},{90},{90},{90},{i*10}\n"
        s.sendall(pkt.encode())
        print(f"[mock-qnx] -> angles {pkt.strip()}")
        time.sleep(0.15)

    # simulate a completed pose + cycle
    s.sendall(b"POSE_DONE:0\n")
    time.sleep(0.1)
    s.sendall(b"CYCLE_DONE:1\n")
    time.sleep(0.1)

    # simulate a watchdog hit
    s.sendall(b"WATCHDOG_HIT:12.5\n")
    time.sleep(0.1)

    # simulate cycling stop
    s.sendall(b"SEQ_COMPLETE\n")
    time.sleep(0.1)

    # simulate emergency
    s.sendall(b"EMERGENCY_STOP\n")
    time.sleep(0.3)

    print("[mock-qnx] test sequence sent — closing")
    s.close()


if __name__ == "__main__":
    main()
