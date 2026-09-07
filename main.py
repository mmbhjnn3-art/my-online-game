```python
import socket
import threading

from kivy.app import App
from kivy.clock import Clock
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.textinput import TextInput
from kivy.uix.button import Button
from kivy.uix.popup import Popup
from kivy.core.window import Window


# =========================
# Server Settings
# =========================

HOST = "0.0.0.0"
PORT = 5000

clients = []
clients_lock = threading.Lock()


# =========================
# Internet Check
# =========================

def check_internet():
    try:
        test_socket = socket.create_connection(
            ("8.8.8.8", 53),
            timeout=3
        )
        test_socket.close()
        return True
    except:
        return False


# =========================
# Server
# =========================

def send_to_all(message):
    dead_clients = []

    with clients_lock:
        for client in clients:
            try:
                client.send(
                    message.encode("utf-8")
                )
            except:
                dead_clients.append(client)

        for client in dead_clients:
            if client in clients:
                clients.remove(client)


def client_handler(conn, address):

    print("Player connected:", address)

    with clients_lock:
        clients.append(conn)

    try:

        conn.send(
            "Welcome to the game server!".encode("utf-8")
        )

        while True:

            data = conn.recv(4096)

            if not data:
                break

            message = data.decode(
                "utf-8",
                errors="ignore"
            )

            print(
                "Message:",
                message
            )

            send_to_all(message)

    except Exception as e:

        print(
            "Client error:",
            e
        )

    finally:

        with clients_lock:
            if conn in clients:
                clients.remove(conn)

        try:
            conn.close()
        except:
            pass

        print(
            "Player disconnected:",
            address
        )


def start_server():

    try:

        server = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        server.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        server.bind(
            (HOST, PORT)
        )

        server.listen(10)

        print(
            "Game server started"
        )

        while True:

            conn, address = server.accept()

            thread = threading.Thread(
                target=client_handler,
                args=(conn, address),
                daemon=True
            )

            thread.start()

    except Exception as e:

        print(
            "Server error:",
            e
        )


# =========================
# Kivy Game
# =========================

class GameLayout(BoxLayout):

    def __init__(self, **kwargs):

        super().__init__(
            orientation="vertical",
            spacing=10,
            padding=15,
            **kwargs
        )

        Window.clearcolor = (
            0.53,
            0.81,
            0.92,
            1
        )

        self.socket = None
        self.connected = False

        # =====================
        # Title
        # =====================

        title = Label(
            text="ONLINE GAME",
            font_size=28,
            size_hint_y=None,
            height=60
        )

        self.add_widget(title)

        # =====================
        # Status
        # =====================

        self.status = Label(
            text="Ready to connect",
            font_size=18,
            size_hint_y=None,
            height=45
        )

        self.add_widget(
            self.status
        )

        # =====================
        # Player Name
        # =====================

        self.name = TextInput(
            text="Player",
            hint_text="Player Name",
            multiline=False,
            font_size=18,
            size_hint_y=None,
            height=50
        )

        self.add_widget(
            self.name
        )

        # =====================
        # Server IP
        # =====================

        self.server_ip = TextInput(
            text="127.0.0.1",
            hint_text="Server IP",
            multiline=False,
            font_size=18,
            size_hint_y=None,
            height=50
        )

        self.add_widget(
            self.server_ip
        )

        # =====================
        # Connect Button
        # =====================

        self.connect_button = Button(
            text="CONNECT TO SERVER",
            font_size=18,
            size_hint_y=None,
            height=55
        )

        self.connect_button.bind(
            on_press=self.connect
        )

        self.add_widget(
            self.connect_button
        )

        # =====================
        # Messages
        # =====================

        self.messages = TextInput(
            text="Game is ready...\n",
            readonly=True,
            multiline=True,
            font_size=16
        )

        self.add_widget(
            self.messages
        )

        # =====================
        # Message Input
        # =====================

        self.message = TextInput(
            text="Hello!",
            hint_text="Write a message...",
            multiline=False,
            font_size=18,
            size_hint_y=None,
            height=50
        )

        self.add_widget(
            self.message
        )

        # =====================
        # Send Button
        # =====================

        self.send_button = Button(
            text="SEND MESSAGE",
            font_size=18,
            size_hint_y=None,
            height=55
        )

        self.send_button.bind(
            on_press=self.send_message
        )

        self.add_widget(
            self.send_button
        )

    # =====================
    # Popup
    # =====================

    def show_popup(
        self,
        title,
        text
    ):

        content = BoxLayout(
            orientation="vertical",
            padding=10,
            spacing=10
        )

        label = Label(
            text=text,
            font_size=17
        )

        close_button = Button(
            text="OK",
            size_hint_y=None,
            height=50
        )

        content.add_widget(label)
        content.add_widget(close_button)

        popup = Popup(
            title=title,
            content=content,
            size_hint=(0.85, 0.4)
        )

        close_button.bind(
            on_press=popup.dismiss
        )

        popup.open()

    # =====================
    # Connect
    # =====================

    def connect(self, instance):

        if not check_internet():

            self.show_popup(
                "No Internet",
                "Please turn on the internet!"
            )

            return

        if self.connected:
            return

        ip = self.server_ip.text.strip()

        if not ip:

            self.show_popup(
                "Error",
                "Enter the server IP."
            )

            return

        try:

            self.socket = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            self.socket.settimeout(5)

            self.socket.connect(
                (
                    ip,
                    PORT
                )
            )

            self.socket.settimeout(None)

            self.connected = True

            self.status.text = (
                "Connected to server"
            )

            self.connect_button.text = (
                "CONNECTED"
            )

            self.connect_button.disabled = True

            self.add_message(
                "Connection established!"
            )

            thread = threading.Thread(
                target=self.receive_messages,
                daemon=True
            )

            thread.start()

            player_name = (
                self.name.text.strip()
                or "Player"
            )

            self.socket.send(
                (
                    player_name +
                    " joined the game!"
                ).encode("utf-8")
            )

        except Exception as e:

            self.socket = None
            self.connected = False

            self.show_popup(
                "Connection Error",
                "Could not connect to server:\n"
                + str(e)
            )

    # =====================
    # Receive Messages
    # =====================

    def receive_messages(self):

        while self.connected:

            try:

                data = self.socket.recv(
                    4096
                )

                if not data:
                    break

                message = data.decode(
                    "utf-8",
                    errors="ignore"
                )

                Clock.schedule_once(
                    lambda dt, msg=message:
                    self.add_message(msg)
                )

            except:

                break

        self.connected = False

        Clock.schedule_once(
            lambda dt:
            self.connection_lost()
        )

    # =====================
    # Connection Lost
    # =====================

    def connection_lost(self):

        self.status.text = (
            "Disconnected"
        )

        self.connect_button.text = (
            "CONNECT TO SERVER"
        )

        self.connect_button.disabled = False

    # =====================
    # Add Message
    # =====================

    def add_message(self, message):

        self.messages.text += (
            message + "\n"
        )

        self.messages.cursor = (
            0,
            len(self.messages.text)
        )

    # =====================
    # Send Message
    # =====================

    def send_message(self, instance):

        if not self.connected:

            self.show_popup(
                "Warning",
                "Connect to the server first!"
            )

            return

        if not check_internet():

            self.show_popup(
                "No Internet",
                "Please turn on the internet!"
            )

            return

        text = self.message.text.strip()

        if not text:
            return

        try:

            name = (
                self.name.text.strip()
                or "Player"
            )

            final_message = (
                name +
                ": " +
                text
            )

            self.socket.send(
                final_message.encode("utf-8")
            )

            self.message.text = ""

        except Exception as e:

            self.show_popup(
                "Error",
                str(e)
            )


# =========================
# App
# =========================

class OnlineGameApp(App):

    def build(self):

        self.title = "My Online Game"

        return GameLayout()


# =========================
# Start
# =========================

if __name__ == "__main__":

    # Start local server
    server_thread = threading.Thread(
        target=start_server,
        daemon=True
    )

    server_thread.start()

    # Start application
    OnlineGameApp().run()
              
