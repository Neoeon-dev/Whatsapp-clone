import asyncio
import websockets
from database.database import database, User
from model.model import ErrorCode
import json

async def client():
    uri = "ws://localhost:8765"
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
            print("Server response:", signup_response)

            await window(websocket=websocket)
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
            print("Server response:", login_response)

            await window(websocket=websocket)

async def window(websocket):
    print("""
____________________________________________________________
        
                    Chat with someone
____________________________________________________________

enter the number,

1. Enter the username with whom you want to chat with
2. recieve messages from someone

""")

    pulse = int(input("$ "))

    friend = input("$ ")

    second = False

    while(1):
        if pulse == 2 or second:
            message_payload = await websocket.recv()
            data = json.loads(message_payload)
            print(f"New message from {data.get('sender')}: {data.get('text')}")
        
        print("""
        input $123$ to exit,
        """)

        message = input("$ ")

        if message == "$123$":
            break

        message_payload = {
            "type" : "chat_message",
            "receiver" : friend,
            "text" : message
        }

        await websocket.send(json.dumps(message_payload))

        second = True


if __name__ == "__main__":
    asyncio.run(client())