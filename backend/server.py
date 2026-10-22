import asyncio
import json
import websockets
from database.database import database, User
from model.model import ErrorCode
from dataclasses import asdict

db = database()

connected_clients = {}

online_clients = {}

async def handle_login(websocket, data):

    username = data.get("username")
    password = data.get("password")

    user = db.get_user_info(email=username, password=password)

    if user == ErrorCode.USER_DOESNT_EXIST:
        response = {
            "type" : "login_failure",
            "message" : "User doesn't exist actually"
        }
    elif user == ErrorCode.WRONG_PASSWORD:
        response = {
            "type" : "login_failure",
            "message" : f"You ain't {username}, are you?"
        }
    else:
        connected_clients[websocket] = username
        online_clients[username] = websocket
        response = {
            "type" : "login_success",
            "message" : asdict(user)
        }
        print(f"email : {user.email}, name : {user.name},age : {user.age}")

    await websocket.send(json.dumps(response))

async def handle_signup(websocket, data):

    username = data.get("username")
    password = data.get("password")
    name = data.get("name")
    age = data.get("age")

    error = db.add_user(email=username, password=password, name=name, age=age)
    
    if error == ErrorCode.USER_ALREADY_EXISTS:
        response = {
            "type" : "signup_failure",
            "message" : "Sorry, someone already has your name"
        }
    else:
        connected_clients[websocket] = username
        response = {
            "type" : "signup_success",
            "message" : "Lets go 🤑️🤑️, we added you in our list"
        }
        print("Money money more money")

    await websocket.send(json.dumps(response))

async def is_online(websocket, username):

    if username in online_clients:
        response = {
            "type" : "online_status",
            "message" : f"{username} is online"
        }
    else:
        response = {
            "type" : "online_status",
            "message" : f"{username} is offline"
        }

    await websocket.send(json.dumps(response))

async def handle_chat_message(websocket, data):

    # Check if the user is logged in first
    if websocket not in connected_clients:
        error_response = {"type": "error", "message": "Unauthorized. Please log in first."}
        await websocket.send(json.dumps(error_response))
        return

    user = connected_clients[websocket]
    text = data.get("text", "")
    username = data.get("receiver", "")
    print(f"Chat from {user}: {text}")
    
    broadcast_data = {
        "type": "chat_message",
        "sender": user,
        "text": text
    }
    
    recipient_ws = online_clients.get(username)

    if recipient_ws is None:
        await websocket.send(json.dumps({
            "type": "delivery_failure",
            "message": f"{username} is offline or doesn't exist"
        }))
        return

    await recipient_ws.send(json.dumps(broadcast_data))


async def router(websocket):
    print("client connected")

    try:
        async for raw_message in websocket:
            try:
                # 1. Parse JSON payload
                data = json.loads(raw_message)
                msg_type = data.get("type")

                # 2. Route based on message type
                if msg_type == "login":
                    await handle_login(websocket, data)
                elif msg_type == "signup":
                    await handle_signup(websocket, data)
                elif msg_type == "chat_message":
                    await handle_chat_message(websocket, data)
                else:
                    await websocket.send(json.dumps({"type": "error", "message": f"Unknown message type: {msg_type}"}))

            except json.JSONDecodeError:
                await websocket.send(json.dumps({"type": "error", "message": "Invalid JSON format"}))
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        user = connected_clients.pop(websocket, None)
        if user:
            print(f"User '{user}' disconnected")

async def main():
    # Start the server on localhost, port 8765
    async with websockets.serve(router, "localhost", 8765):
        print("WebSocket server started on ws://localhost:8765")
        await asyncio.Future()  # Keeps the server running indefinitely

if __name__ == "__main__":
    asyncio.run(main())