
import asyncio
import json
import os
import uuid
import mimetypes
from pathlib import Path

import websockets
from websockets.exceptions import ConnectionClosed

from database.database import database
from model.model import ErrorCode


HOST = "0.0.0.0"
PORT = 8765

UPLOAD_DIR = Path("uploads")
INDEX_FILE = UPLOAD_DIR / "index.json"

CHUNK_SIZE = 64 * 1024
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

UPLOAD_DIR.mkdir(exist_ok=True)

db = database()

# Active connections
connected_clients = {}       # websocket -> username
online_clients = {}          # username -> websocket

# One lock per connection for outgoing messages
send_locks = {}

# Active uploads
active_uploads = {}

# Protect persistent file metadata
index_lock = asyncio.Lock()


def load_index():
    if not INDEX_FILE.exists():
        return {}

    try:
        with open(INDEX_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


file_index = load_index()


async def send_json(websocket, data):
    lock = send_locks.get(websocket)

    if lock is None:
        return

    async with lock:
        await websocket.send(json.dumps(data))


async def notify_user(username, data):
    websocket = online_clients.get(username)

    if websocket is not None:
        try:
            await send_json(websocket, data)
        except ConnectionClosed:
            pass


async def save_index():
    async with index_lock:
        temp_file = INDEX_FILE.with_suffix(".tmp")

        with open(temp_file, "w") as f:
            json.dump(file_index, f, indent=2)

        os.replace(temp_file, INDEX_FILE)


# ---------------- AUTHENTICATION ----------------

async def handle_login(websocket, data):

    username = data.get("username")
    password = data.get("password")

    user = db.get_user_info(
        email=username,
        password=password
    )

    if user == ErrorCode.USER_DOESNT_EXIST:
        response = {
            "type": "login_failure",
            "message": "User doesn't exist"
        }

    elif user == ErrorCode.WRONG_PASSWORD:
        response = {
            "type": "login_failure",
            "message": "Incorrect password"
        }

    else:
        # Remove an older connection for the same account
        old_ws = online_clients.get(username)

        if old_ws is not None and old_ws is not websocket:
            connected_clients.pop(old_ws, None)
            await old_ws.close()

        connected_clients[websocket] = username
        online_clients[username] = websocket

        response = {
            "type": "login_success",
            "message": {
                "email": user.email,
                "name": user.name,
                "age": user.age
            }
        }

        print(f"{username} logged in")

    await send_json(websocket, response)


async def handle_signup(websocket, data):

    username = data.get("username")
    password = data.get("password")
    name = data.get("name")
    age = data.get("age")

    if not all([username, password, name]) or not isinstance(age, int):
        await send_json(websocket, {
            "type": "signup_failure",
            "message": "Invalid signup information"
        })
        return

    error = db.add_user(
        email=username,
        password=password,
        name=name,
        age=age
    )

    if error == ErrorCode.USER_ALREADY_EXISTS:
        response = {
            "type": "signup_failure",
            "message": "User already exists"
        }

    else:
        connected_clients[websocket] = username
        online_clients[username] = websocket

        response = {
            "type": "signup_success",
            "message": "Account created successfully"
        }

        print(f"{username} signed up")

    await send_json(websocket, response)


# ---------------- TEXT MESSAGES ----------------

async def handle_chat_message(websocket, data):

    sender = connected_clients.get(websocket)

    if sender is None:
        return

    receiver = data.get("receiver")
    message = data.get("text", "")

    if not isinstance(receiver, str) or not isinstance(message, str):
        return

    recipient_ws = online_clients.get(receiver)

    if recipient_ws is None:
        await send_json(websocket, {
            "type": "delivery_failure",
            "message": f"{receiver} is offline"
        })
        return

    await send_json(recipient_ws, {
        "type": "chat_message",
        "sender": sender,
        "text": message
    })

    await send_json(websocket, {
        "type": "delivery_success",
        "message": "Message delivered"
    })


# ---------------- FILE UPLOAD ----------------

async def start_upload(websocket, data):

    sender = connected_clients.get(websocket)

    if sender is None:
        return

    receiver = data.get("receiver")
    filename = os.path.basename(data.get("filename", ""))
    size = data.get("size")

    if not receiver or not filename:
        return

    if not isinstance(size, int) or not 0 <= size <= MAX_FILE_SIZE:
        await send_json(websocket, {
            "type": "file_failure",
            "message": "Invalid file size"
        })
        return

    transfer_id = str(uuid.uuid4())

    temp_path = UPLOAD_DIR / f"{transfer_id}.part"

    try:
        file_handle = open(temp_path, "wb")
    except OSError:
        await send_json(websocket, {
            "type": "file_failure",
            "message": "Could not create file"
        })
        return

    active_uploads[websocket] = {
        "id": transfer_id,
        "sender": sender,
        "receiver": receiver,
        "filename": filename,
        "content_type": mimetypes.guess_type(filename)[0]
            or "application/octet-stream",
        "size": size,
        "received": 0,
        "path": temp_path,
        "file": file_handle
    }

    await send_json(websocket, {
        "type": "file_upload_started",
        "transfer_id": transfer_id
    })


async def handle_upload_chunk(websocket, chunk):

    upload = active_uploads.get(websocket)

    if upload is None:
        return

    new_size = upload["received"] + len(chunk)

    if new_size > upload["size"]:
        await abort_upload(websocket, "File exceeded declared size")
        return

    upload["file"].write(chunk)
    upload["received"] = new_size


async def abort_upload(websocket, reason):

    upload = active_uploads.pop(websocket, None)

    if upload:
        upload["file"].close()
        upload["path"].unlink(missing_ok=True)

    await send_json(websocket, {
        "type": "file_failure",
        "message": reason
    })


async def finish_upload(websocket):

    upload = active_uploads.get(websocket)

    if upload is None:
        return

    if upload["received"] != upload["size"]:
        await abort_upload(websocket, "Incomplete file transfer")
        return

    upload["file"].close()

    transfer_id = upload["id"]

    final_path = UPLOAD_DIR / transfer_id

    try:
        os.replace(upload["path"], final_path)

        metadata = {
            "transfer_id": transfer_id,
            "sender": upload["sender"],
            "receiver": upload["receiver"],
            "filename": upload["filename"],
            "content_type": upload["content_type"],
            "size": upload["size"],
            "path": str(final_path)
        }

        file_index[transfer_id] = metadata
        await save_index()

    except OSError:
        active_uploads.pop(websocket, None)
        await send_json(websocket, {
            "type": "file_failure",
            "message": "Could not save file"
        })
        return

    active_uploads.pop(websocket, None)

    await send_json(websocket, {
        "type": "file_upload_success",
        "transfer_id": transfer_id
    })

    # Notify recipient, even if they are offline.
    await notify_user(upload["receiver"], {
        "type": "file_available",
        "sender": upload["sender"],
        "filename": upload["filename"],
        "size": upload["size"],
        "transfer_id": transfer_id
    })

    print(
        f"File stored: {upload['filename']} "
        f"from {upload['sender']} to {upload['receiver']}"
    )


# ---------------- FILE DOWNLOAD ----------------

async def download_file(websocket, data):

    username = connected_clients.get(websocket)

    if username is None:
        return

    transfer_id = data.get("transfer_id")
    metadata = file_index.get(transfer_id)

    if metadata is None or metadata["receiver"] != username:
        await send_json(websocket, {
            "type": "download_failure",
            "message": "File not found or access denied"
        })
        return

    path = Path(metadata["path"])

    if not path.is_file():
        await send_json(websocket, {
            "type": "download_failure",
            "message": "File is no longer available"
        })
        return

    lock = send_locks[websocket]

    async with lock:

        try:
            await websocket.send(json.dumps({
                "type": "download_start",
                "transfer_id": transfer_id,
                "filename": metadata["filename"],
                "size": metadata["size"],
                "content_type": metadata["content_type"]
            }))

            with open(path, "rb") as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)

                    if not chunk:
                        break

                    await websocket.send(chunk)

            await websocket.send(json.dumps({
                "type": "download_end",
                "transfer_id": transfer_id
            }))

        except ConnectionClosed:
            print("Download interrupted")

    print(f"{username} downloaded {metadata['filename']}")


# ---------------- MESSAGE ROUTER ----------------

async def router(websocket):

    print("Client connected")

    send_locks[websocket] = asyncio.Lock()

    try:

        async for message in websocket:

            # Binary frames belong to an active upload
            if isinstance(message, bytes):
                await handle_upload_chunk(websocket, message)
                continue

            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await send_json(websocket, {
                    "type": "error",
                    "message": "Invalid JSON"
                })
                continue

            msg_type = data.get("type")

            if msg_type == "login":
                await handle_login(websocket, data)

            elif msg_type == "signup":
                await handle_signup(websocket, data)

            elif msg_type == "chat_message":
                await handle_chat_message(websocket, data)

            elif msg_type == "file_start":
                if websocket in active_uploads:
                    await send_json(websocket, {
                        "type": "file_failure",
                        "message": "Finish the current upload first"
                    })
                else:
                    await start_upload(websocket, data)

            elif msg_type == "file_end":
                await finish_upload(websocket)

            elif msg_type == "download_file":
                await download_file(websocket, data)

            else:
                await send_json(websocket, {
                    "type": "error",
                    "message": f"Unknown message type: {msg_type}"
                })

    except ConnectionClosed:
        pass

    finally:

        upload = active_uploads.pop(websocket, None)

        if upload:
            upload["file"].close()
            upload["path"].unlink(missing_ok=True)

        username = connected_clients.pop(websocket, None)

        if username and online_clients.get(username) is websocket:
            online_clients.pop(username, None)
            print(f"{username} disconnected")

        send_locks.pop(websocket, None)


async def main():

    async with websockets.serve(
        router,
        HOST,
        PORT,
        max_size=CHUNK_SIZE,
        max_queue=32
    ):
        print(f"WebSocket server running on port {PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
