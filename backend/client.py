
import asyncio
import json
import os
import mimetypes
from pathlib import Path

import websockets
from websockets.exceptions import ConnectionClosed


# Change this to your Fedora laptop's LAN IP
SERVER_IP = "192.168.0.102"
PORT = 8765

URI = f"ws://{SERVER_IP}:{PORT}"

CHUNK_SIZE = 64 * 1024

DOWNLOAD_DIR = Path("received_files")
DOWNLOAD_DIR.mkdir(exist_ok=True)


# ---------------- SEND HELPERS ----------------

async def send_json(websocket, data):

    await websocket.send(json.dumps(data))


async def send_file(websocket, receiver, filepath):

    filepath = Path(filepath)

    if not filepath.is_file():
        print("File does not exist")
        return

    filename = filepath.name
    size = filepath.stat().st_size

    metadata = {
        "type": "file_start",
        "receiver": receiver,
        "filename": filename,
        "size": size,
        "content_type": mimetypes.guess_type(filename)[0]
            or "application/octet-stream"
    }

    # Start upload
    await send_json(websocket, metadata)

    # Send file in chunks
    with open(filepath, "rb") as f:

        while True:

            chunk = f.read(CHUNK_SIZE)

            if not chunk:
                break

            await websocket.send(chunk)

    # Finish upload
    await send_json(websocket, {
        "type": "file_end"
    })

    print(f"Uploading {filename}...")


# ---------------- RECEIVE HELPERS ----------------

async def receive_messages(websocket):

    current_file = None
    current_file_handle = None
    current_file_size = 0
    received_size = 0

    try:

        async for message in websocket:

            # Binary frame: file chunk
            if isinstance(message, bytes):

                if current_file_handle is None:
                    print("Unexpected binary data received")
                    continue

                current_file_handle.write(message)
                received_size += len(message)

                continue

            # Text frame: JSON
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                print("Invalid server message")
                continue

            msg_type = data.get("type")

            if msg_type == "chat_message":

                print(
                    f"\n[{data.get('sender')}]: "
                    f"{data.get('text')}"
                )

            elif msg_type == "delivery_success":

                print("\nMessage delivered!")

            elif msg_type == "delivery_failure":

                print(f"\nDelivery failed: {data.get('message')}")

            elif msg_type == "file_available":

                print("\n" + "=" * 45)
                print("NEW FILE RECEIVED")
                print("=" * 45)

                print(f"From       : {data.get('sender')}")
                print(f"Filename   : {data.get('filename')}")
                print(f"Size       : {data.get('size')} bytes")
                print(f"Transfer ID: {data.get('transfer_id')}")

                print(
                    f"\nDownload using:\n"
                    f"/download {data.get('transfer_id')}"
                )

            elif msg_type == "download_start":

                current_file = data["filename"]
                current_file_size = data["size"]
                received_size = 0

                # Avoid overwriting an existing file
                safe_name = os.path.basename(current_file)
                destination = DOWNLOAD_DIR / safe_name

                if destination.exists():
                    destination = DOWNLOAD_DIR / (
                        f"{destination.stem}_"
                        f"{data['transfer_id'][:8]}"
                        f"{destination.suffix}"
                    )

                current_file = destination

                current_file_handle = open(current_file, "wb")

                print(f"\nDownloading {destination.name}...")

            elif msg_type == "download_end":

                if current_file_handle is not None:

                    current_file_handle.close()
                    current_file_handle = None

                    if received_size == current_file_size:
                        print(f"\nFile downloaded: {current_file}")
                    else:
                        print(
                            f"\nIncomplete download: "
                            f"{received_size}/{current_file_size} bytes"
                        )
                        current_file.unlink(missing_ok=True)

                    current_file = None

            elif msg_type == "file_upload_success":

                print(
                    f"\nFile uploaded successfully!"
                    f"\nTransfer ID: {data.get('transfer_id')}"
                )

            elif msg_type in ("file_failure", "download_failure"):

                print(f"\nFile operation failed: {data.get('message')}")

            elif msg_type == "file_upload_started":

                print(
                    f"\nUpload started: {data.get('transfer_id')}"
                )

            elif msg_type == "error":

                print(f"\nServer error: {data.get('message')}")

            else:

                print(f"\nServer: {data}")

    except ConnectionClosed:

        print("\nDisconnected from server")

    finally:

        if current_file_handle is not None:
            current_file_handle.close()

            if current_file is not None:
                current_file.unlink(missing_ok=True)


# ---------------- AUTHENTICATION ----------------

async def authenticate(websocket):

    print("""
============================================
                 WHATSAPP CLONE
============================================

1. Signup
2. Login
3. Exit
""")

    choice = await asyncio.to_thread(input, "$ ")

    if choice == "3":
        return False

    if choice not in ("1", "2"):
        print("Invalid choice")
        return False

    username = await asyncio.to_thread(
        input, "Username/email: "
    )

    password = await asyncio.to_thread(
        input, "Password: "
    )

    if choice == "1":

        name = await asyncio.to_thread(
            input, "Name: "
        )

        age = int(await asyncio.to_thread(
            input, "Age: "
        ))

        payload = {
            "type": "signup",
            "username": username,
            "password": password,
            "name": name,
            "age": age
        }

    else:

        payload = {
            "type": "login",
            "username": username,
            "password": password
        }

    await send_json(websocket, payload)

    response = json.loads(await websocket.recv())

    print(response.get("message"))

    if response.get("type") in ("login_success", "signup_success"):
        return username

    return False


# ---------------- CHAT WINDOW ----------------

async def chat_window(websocket, username):

    print("""
============================================
                  CHAT WINDOW
============================================

Commands:

/send <filepath>       Send a file
/download <transfer_id> Download a file
/exit                  Exit

Anything else is sent as a text message.
""")

    friend = await asyncio.to_thread(
        input, "Friend username: "
    )

    # Exactly one coroutine reads from the socket
    receiver_task = asyncio.create_task(
        receive_messages(websocket)
    )

    try:

        while True:

            message = await asyncio.to_thread(
                input, "$ "
            )

            message = message.strip()

            if not message:
                continue

            if message == "/exit":
                break

            elif message.startswith("/send "):

                filepath = Path(
                    message[len("/send "):].strip().strip('"')
                ).expanduser().resolve()

                if os.path.isfile(filepath):
                    await send_file(
                        websocket,
                        friend,
                        filepath
                    )
                else:
                    print("Invalid file path")

            elif message.startswith("/download "):

                transfer_id = message[len("/download "):].strip()

                await send_json(websocket, {
                    "type": "download_file",
                    "transfer_id": transfer_id
                })

            else:

                await send_json(websocket, {
                    "type": "chat_message",
                    "receiver": friend,
                    "text": message
                })

    except ConnectionClosed:

        print("Connection closed")

    finally:

        receiver_task.cancel()

        await asyncio.gather(
            receiver_task,
            return_exceptions=True
        )


# ---------------- MAIN ----------------

async def client():

    try:

        async with websockets.connect(
            URI,
            max_size=CHUNK_SIZE
        ) as websocket:

            print("Connected to server!")

            username = await authenticate(websocket)

            if not username:
                return

            await chat_window(websocket, username)

    except ConnectionClosed:

        print("Server disconnected")

    except OSError as e:

        print(f"Connection error: {e}")


if __name__ == "__main__":
    asyncio.run(client())
