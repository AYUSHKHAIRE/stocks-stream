import socket
import threading
from logger_config import logger

class SocketServer:
    def __init__(self, host='localhost', port=3456, max_clients=5):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def start(self):
        """Starts the server and listens for connections."""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        logger.info(f"Server started on {self.host}:{self.port}, waiting for connections...")

        while True:
            client_socket, addr = self.server_socket.accept()
            logger.info(f"Connected to {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket, addr)).start()
    
    def handle_client(self, client_socket, addr):
        """Handles communication with a client."""
        try:
            while True:
                message = client_socket.recv(1024).decode()
                if not message:
                    logger.info(f"Client {addr} disconnected.")
                    break
                
                logger.info(f"Received from {addr}: {message}")
                client_socket.send(f"Echo: {message}".encode())  # Send back response
        except Exception as e:
            logger.error(f"Error with client {addr}: {e}")
        finally:
            client_socket.close()

# Start the server
# if __name__ == "__main__":
#     server = SocketServer()
#     server.start()
