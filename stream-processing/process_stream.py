import time
from socket_client import SocketClient
import json

# Start client and keep listening for messages
client = SocketClient()
client.connect()

# Keep the script running indefinitely
past_length = 0
while True:
    stream_length = len(client.whole_stream)
    if past_length != stream_length:
        for i in range(past_length, stream_length):
            data = client.whole_stream[i]  
            # Process only new data
            # do the operations what you want .
            # ================================================
            # ================ operation START ================
            # ================================================
            jsn_data = json.dumps(data, indent=4)
            print(jsn_data)
            # ================================================
            # ================ operation END ================
            # ================================================
        past_length = stream_length  # Update counter
    time.sleep(1)  # Prevents high CPU usage
