import socket
import json
import time
import random
from faker import Faker

fake = Faker()

# Generate a random ride event
def generate_ride_event():
    return {
        "trip_id": fake.uuid4(),
        "driver_id": str(random.randint(1, 100)), # FIXED: Cast integer to a string to match Spark
        "distance_km": round(random.uniform(1, 50), 2),
        "fare_amount": round(random.uniform(5, 150), 2),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

# Start streaming using socket
def start_streaming(host="127.0.0.1", port=9999): # FIXED: Using 127.0.0.1 is bulletproof on Windows
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Allows quick script restarts
    server_socket.bind((host, port))
    server_socket.listen(5)  
    print(f"Streaming data to {host}:{port}...")

    while True:
        try:
            conn, addr = server_socket.accept()
            print(f"New client connected: {addr}")

            while True:
                try:
                    ride_event = generate_ride_event()
                    conn.send((json.dumps(ride_event) + "\n").encode("utf-8"))
                    print("Sent:", ride_event)
                    time.sleep(1)
                except (BrokenPipeError, ConnectionResetError):
                    print(f"Client {addr} disconnected. Waiting for a new client.")
                    break  

        except Exception as e:
            print(f"Error accepting connection: {e}")

if __name__ == "__main__":
    start_streaming()