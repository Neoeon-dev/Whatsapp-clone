import asyncio
import websockets
from database.database import database, User
from model.model import ErrorCode
import json

async def client():
    uri = "ws://10.109.86.206:8765"
    async with websockets.connect(uri) as websocket:

        print("""
____________________________________________________________

                        Welcome
____________________________________________________________

click the number in input,

1. signup
2. login
3. exit

""")

        cool = int(input("$ "))

        if cool == 1:
            print("username please, ")
            username = input("$ ")
            print("password please, ")
            password = input("$ ")
            print("name please, ")
            name = input("$ ")
            print("age please, ")
            age = int(input("$ "))
            # 1. signup Message
            signup_payload = {
                "type": "signup",
                "username" : username,
                "password" : password,
                "name" : name,
                "age" : age
            }
            await websocket.send(json.dumps(signup_payload))

            # Read server response for signup
            signup_response = await websocket.recv()

            resp = json.loads(signup_response)

            if resp.get("type") == "signup_success":
                await window(websocket)
            else:
                print(resp.get("message"))

        elif cool == 2:
            print("username please, ")
            username = input("$ ")
            print("password please, ")
            password = input("$ ")
            # 2. Send Login Request
            login_payload = {
                "type": "login",
                "username": username,
                "password": password
            }
            await websocket.send(json.dumps(login_payload))

            # Read server response for login
            login_response = await websocket.recv()

            resp = json.loads(login_response)

            if resp.get("type") == "login_success":
                await window(websocket=websocket)
            else:
                print(resp.get("message"))

async def recieve_messages(websocket):
    try:
        async for message in websocket:
            data = json.loads(message)

            if data.get("type") == "chat_message":
                print(
                    f"\nNew message from {data.get('sender')}: "
                    f"{data.get('text')}"
                )
                print("$ ", end="", flush=True)
            else:
                print(f"\nServer: {data}")
                print("$ ", end="", flush=True)
    except websockets.ConnectionClosed:
        print("\nDisconnected from server.")

async def window(websocket):
    print("""
____________________________________________________________
        
                    Chat with someone
____________________________________________________________

Enter the username with whom you want to chat with

""")

    friend = input("Friend username: ").strip()

    receiver_task = asyncio.create_task(
        recieve_messages(websocket)
    )

    try:
        while True:
            message = await asyncio.to_thread(input, "$ ")

            if message == "$123$":
                break

            payload = {
                "type": "chat_message",
                "receiver": friend,
                "text": message
            }

            await websocket.send(json.dumps(payload))
    except websockets.ConnectionClosed:
        print("Connection closed.")

    finally:
        receiver_task.cancel()
        await asyncio.gather(
            receiver_task,
            return_exceptions=True
        )


if __name__ == "__main__":
    asyncio.run(client())