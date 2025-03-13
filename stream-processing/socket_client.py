import socket
import threading
import json
from logger_config import logger

class SocketClient:
    def __init__(self, host='localhost', port=3456):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.whole_stream = []
        self.buffer = ""  # Buffer to handle incomplete JSON messages

    def connect(self):
        """Connects to the server and starts listening for messages."""
        try:
            self.client_socket.connect((self.host, self.port))
            logger.info("Connected to the server.")
            
            # Start a separate thread to always listen for messages
            listener_thread = threading.Thread(target=self.receive_messages, daemon=True)
            listener_thread.start()

        except Exception as e:
            logger.error(f"Connection failed: {e}")

    def send_message(self, message):
        """Sends a message to the server."""
        try:
            self.client_socket.send((message + "\n").encode())
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def receive_messages(self):
        """Continuously listens for messages from the server."""
        try:
            while True:
                response = self.client_socket.recv(1024).decode()
                if not response:
                    logger.warning("Server disconnected.")
                    break

                self.buffer += response  # Append new data to buffer
                
                while "\n" in self.buffer:  # Process only complete JSON messages
                    message, self.buffer = self.buffer.split("\n", 1)
                    try:
                        parsed_message = json.loads(message)  # Parse JSON
                        self.whole_stream.append(parsed_message)
                        # logger.info(f"Received JSON: {parsed_message}")
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON Decode Error: {e}, data: {message}")

        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
        finally:
            self.client_socket.close()
