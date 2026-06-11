"""
data_generator.py
-----------------
Streams synthetic ride-sharing JSON records over a TCP socket on localhost:9999.
Each record has the schema:
    trip_id, driver_id, distance_km, fare_amount, timestamp

Run this script first and keep it running while executing any task script.
"""

import socket
import json
import time
import random
from datetime import datetime
from faker import Faker

fake = Faker()

HOST = "localhost"
PORT = 9999
EMIT_INTERVAL = 0.8          # seconds between records
NUM_DRIVERS = 20             # pool of driver IDs to cycle through

driver_pool = [f"D{str(i).zfill(3)}" for i in range(1, NUM_DRIVERS + 1)]


def generate_record() -> str:
    """Return a single JSON-encoded ride record followed by a newline."""
    record = {
        "trip_id":     fake.uuid4(),
        "driver_id":   random.choice(driver_pool),
        "distance_km": round(random.uniform(1.0, 50.0), 2),
        "fare_amount": round(random.uniform(5.0, 120.0), 2),
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return json.dumps(record)


def main():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen(1)

    print(f"[data_generator] Listening on {HOST}:{PORT} — waiting for a Spark connection...")

    conn, addr = server_sock.accept()
    print(f"[data_generator] Spark connected from {addr}. Streaming records every {EMIT_INTERVAL}s ...")

    try:
        while True:
            record = generate_record()
            payload = record + "\n"
            try:
                conn.sendall(payload.encode("utf-8"))
                print(f"[data_generator] Sent: {record}")
            except BrokenPipeError:
                print("[data_generator] Spark disconnected. Exiting.")
                break
            time.sleep(EMIT_INTERVAL)
    except KeyboardInterrupt:
        print("\n[data_generator] Interrupted. Shutting down.")
    finally:
        conn.close()
        server_sock.close()


if __name__ == "__main__":
    main()
