import sqlite3
import logging
from model.model import ErrorCode
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class User:
    email: str
    name: str
    age: int

    def __init__(self, email, name, age):
        self.email = email
        self.name = name
        self.age = age

class database:

    def __init__(self):

        #creates my_database.db
        self.conn = sqlite3.connect("my_database.db")

        #this executes sql commands
        self.cursor = self.conn.cursor()

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                password TEXT,
                name TEXT NOT NULL,
                age INTEGER
            )
            """
        )

        # Commit the changes
        self.conn.commit()

    def add_user(self, email, password, name, age):

        #Before adding check whether the user exists or not

        self.cursor.execute(
            """
            SELECT *
            FROM users
            WHERE email = ?
            """, (email, )
        )

        user = self.cursor.fetchall()

        if len(user) > 0:
            logging.debug("User already exists")
            return ErrorCode.USER_ALREADY_EXISTS

        self.cursor.execute(
            "INSERT INTO users (name, age, email, password) VALUES (?, ?, ?, ?)",
            (name, age, email, password)
        )

        logging.debug(f"Added USER, {email} => {name}")

        self.conn.commit()

        return ErrorCode.SAFE

    def del_user(self, email, password):

        #Before adding check whether the user exists or not
        
        self.cursor.execute(
            """
            SELECT *
            FROM users
            WHERE email = ?
            """, (email, )
        )

        user = self.cursor.fetchall()

        if len(user) == 0:
            logging.debug("User doesn't exist")
            return ErrorCode.USER_DOESNT_EXIST

        validuser = (user[0][2] == password)

        if not validuser:
            logging.debug("Wrong Password")
            return ErrorCode.WRONG_PASSWORD

        self.cursor.execute(
            """
            DELETE
            FROM users
            WHERE email = ?
            """, (email, )
        )

        logging.debug("User successfully deleted")

        self.conn.commit()

        return ErrorCode.SAFE

    

    def get_user_info(self, email, password):

        self.cursor.execute(
            """
            SELECT *
            FROM users
            WHERE email = ?
            """, (email, )
        )

        user = self.cursor.fetchall()

        if len(user) == 0:
            logging.debug("User doesn't exist")
            return ErrorCode.USER_DOESNT_EXIST

        validuser = (user[0][2] == password)

        if not validuser:
            logging.debug("Wrong password")
            return ErrorCode.WRONG_PASSWORD\

        return User(
            user[0][1],
            user[0][3],
            user[0][4]
        )