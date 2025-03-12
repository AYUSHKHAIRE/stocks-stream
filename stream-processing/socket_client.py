import socket
import threading
from logger_config import logger

class SocketClient:
    def __init__(self, host='localhost', port=3456):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def connect(self):
        """Connects to the server."""
        try:
            self.client_socket.connect((self.host, self.port))
            logger.info("Connected to the server.")
            threading.Thread(target=self.receive_messages, daemon=True).start()
        except Exception as e:
            logger.error(f"Connection failed: {e}")

    def send_message(self, message):
        """Sends a message to the server."""
        try:
            self.client_socket.send(message.encode())
            logger.info(f"Sent: {message}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def receive_messages(self):
        """Receives messages from the server in a loop."""
        try:
            while True:
                response = self.client_socket.recv(1024).decode()
                if not response:
                    logger.info("Server disconnected.")
                    break
                logger.info(f"Received from server: {response}")
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
        finally:
            self.client_socket.close()

# Start the client
# if __name__ == "__main__":
#     client = SocketClient()
#     client.connect()

#     while True:
#         msg = input("Enter message: ")
#         if msg.lower() == "exit":
#             break
#         client.send_message(msg)
