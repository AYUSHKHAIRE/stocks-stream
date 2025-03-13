import socket
import threading
from logger_config import logger

class SocketServer:
    def __init__(self, host='localhost', port=3456, max_clients=5):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clients = []  # Store active client sockets

    def start(self):
        """Starts the server and listens for connections."""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        logger.info(f"Server started on {self.host}:{self.port}, waiting for connections...")

        while True:
            client_socket, addr = self.server_socket.accept()
            logger.info(f"Connected to {addr}")
            self.clients.append(client_socket)  # Add client to the list
            threading.Thread(target=self.handle_client, args=(client_socket, addr), daemon=True).start()
    
    def handle_client(self, client_socket, addr):
        """Handles communication with a client."""
        try:
            while True:
                message = client_socket.recv(1024).decode()
                if not message:
                    logger.info(f"Client {addr} disconnected.")
                    break

                # logger.info(f"Broadcasting message from {addr}: {message}")
                self.broadcast(message, client_socket)  # Send message to all clients

        except Exception as e:
            logger.error(f"Error with client {addr}: {e}")
        finally:
            self.clients.remove(client_socket)  # Remove client on disconnect
            client_socket.close()

    def broadcast(self, message, sender_socket):
        """Sends a message to all connected clients except the sender."""
        for client in self.clients:
            if client != sender_socket:  # Avoid sending the message back to the sender
                try:
                    client.send(message.encode())
                except Exception as e:
                    logger.error(f"Failed to send message: {e}")
                    self.clients.remove(client)  # Remove broken clients

# Start the server
if __name__ == "__main__":
    server = SocketServer()
    server.start()
