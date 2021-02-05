import json
import yaml
import time
import sqlite3
from typing import Dict
from dataclasses import dataclass
from requests_oauthlib import OAuth1Session

with open("config.yaml") as f:
    config = yaml.safe_load(f)

consumer_key: str = config["consumer_key"]
consumer_secret: str = config["comsumer_secret"]
access_token: str = config["access_token"]
access_token_secret: str = config["access_token_secret"]
twitter_api = OAuth1Session(
    consumer_key, consumer_secret, access_token, access_token_secret)

user_id: str = config["user_id"]
followers_url = "https://api.twitter.com/2/users/{}/followers".format(user_id)
follows_url = "https://api.twitter.com/2/users/{}/following".format(user_id)
dm_url = 'https://api.twitter.com/1.1/direct_messages/events/new.json'
api_params = {"max_results": 1000}

db_file = config["db_file"]

conn = sqlite3.connect(db_file)
cur = conn.cursor()
cur.execute(
    "CREATE TABLE IF NOT EXISTS followers(user_id INTEGER PRIMARY KEY, screen_name TEXT, user_name TEXT)")
cur.execute(
    "CREATE TABLE IF NOT EXISTS follows(user_id INTEGER PRIMARY KEY, screen_name TEXT, user_name TEXT)")
cur.execute(
    "CREATE TABLE IF NOT EXISTS follower_events(user_id INTEGER, is_follower INTEGER, time REAL)")
cur.execute(
    "CREATE INDEX IF NOT EXISTS follower_user_id_index on follower_events(user_id)")
cur.execute(
    "CREATE TABLE IF NOT EXISTS follow_events(user_id INTEGER, is_followed INTEGER, time REAL)")
cur.execute(
    "CREATE INDEX IF NOT EXISTS follow_user_id_index on follow_events(user_id)")
cur.execute(
    "CREATE TABLE IF NOT EXISTS screen_names(user_id INTEGER, screen_name TEXT, time REAL)")
cur.execute(
    "CREATE INDEX IF NOT EXISTS screen_user_id_index on screen_names(user_id)")
cur.execute(
    "CREATE TABLE IF NOT EXISTS user_names(user_id INTEGER, user_name TEXT, time REAL)")
cur.execute(
    "CREATE INDEX IF NOT EXISTS user_user_id_index on user_names(user_id)")
conn.commit()
conn.close()


@dataclass
class User:
    user_id: int
    screen_name: str
    user_name: str

    def __init__(self, user_id: str, screen_name: str, user_name: str):
        self.user_id = int(user_id)
        self.screen_name = screen_name
        self.user_name = user_name


def followers_analyzer(followers: Dict[int, "User"]):
    current_time = time.time()
    message = ""
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    cur.execute("SELECT * FROM followers")
    db_data = {}
    for user in cur.fetchall():
        db_data[int(user[0])] = User(user[0], user[1], user[2])

    for user in followers.values():
        screen_name_update = False
        user_name_update = False
        if user.user_id in db_data:
            if user.screen_name != db_data[user.user_id].screen_name:
                cur.execute("UPDATE followers SET screen_name = ? WHERE user_id = ?", [
                            user.screen_name, user.user_id])
                screen_name_update = True
            if user.user_name != db_data[user.user_id].user_name:
                cur.execute("UPDATE followers SET user_name = ? WHERE user_id = ?", [
                            user.user_name, user.user_id])
                user_name_update = True
        else:
            cur.execute("INSERT INTO followers VALUES (?, ?, ?)", [
                        user.user_id, user.screen_name, user.user_name])
            cur.execute("INSERT INTO follower_events VALUES (?, ?, ?)", [
                        user.user_id, 1, current_time])
            message += "{}(_at_{}) followed you\n".format(user.screen_name,
                                                          user.user_name)
            screen_name_update = True
            user_name_update = True

        if screen_name_update:
            cur.execute("SELECT screen_name FROM screen_names WHERE time = (SELECT MAX(time) FROM screen_names WHERE user_id = ?) AND user_id = ?", [
                        user.user_id, user.user_id])
            screen_name_data = cur.fetchall()
            if len(screen_name_data) == 0 or screen_name_data[0][0] != user.screen_name:
                cur.execute("INSERT INTO screen_names VALUES (?, ?, ?)", [
                            user.user_id, user.screen_name, current_time])

        if user_name_update:
            cur.execute("SELECT user_name FROM user_names WHERE time = (SELECT MAX(time) FROM user_names WHERE user_id = ?) AND user_id = ?", [
                        user.user_id, user.user_id])
            user_name_data = cur.fetchall()
            if len(user_name_data) == 0 or user_name_data[0][0] != user.user_name:
                cur.execute("INSERT INTO user_names VALUES (?, ?, ?)", [
                            user.user_id, user.user_name, current_time])

    for user in db_data.values():
        if not user.user_id in followers:
            cur.execute("DELETE FROM followers WHERE user_id = ?",
                        [user.user_id])
            cur.execute("INSERT INTO follower_events VALUES (?, ?, ?)", [
                        user.user_id, 0, current_time])
            message += "{}(_at_{}) unfollowed you\n".format(user.screen_name,
                                                            user.user_name)

    conn.commit()
    conn.close()
    if len(message) != 0:
        print(message)
        send_dm(message)


def follows_analyzer(follows: Dict[int, "User"]):
    current_time = time.time()
    message = ""
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    cur.execute("SELECT * FROM follows")
    db_data = {}
    for user in cur.fetchall():
        db_data[user[0]] = User(user[0], user[1], user[2])

    for user in follows.values():
        screen_name_update = False
        user_name_update = False
        if user.user_id in db_data:
            if user.screen_name != db_data[user.user_id].screen_name:
                cur.execute("UPDATE follows SET screen_name = ? WHERE user_id = ?", [
                            user.screen_name, user.user_id])
                screen_name_update = True
            if user.user_name != db_data[user.user_id].user_name:
                cur.execute("UPDATE follows SET user_name = ? WHERE user_id = ?", [
                            user.user_name, user.user_id])
                user_name_update = True
        else:
            cur.execute("INSERT INTO follows VALUES (?, ?, ?)", [
                        user.user_id, user.screen_name, user.user_name])
            cur.execute("INSERT INTO follow_events VALUES (?, ?, ?)", [
                        user.user_id, 1, current_time])
            message += "you followed {}(_at_{})\n".format(
                user.screen_name, user.user_name)
            screen_name_update = True
            user_name_update = True

        if screen_name_update:
            cur.execute("SELECT screen_name FROM screen_names WHERE time = (SELECT MAX(time) FROM screen_names WHERE user_id = ?) AND user_id = ?", [
                        user.user_id, user.user_id])
            screen_name_data = cur.fetchall()
            if len(screen_name_data) == 0 or screen_name_data[0][0] != user.screen_name:
                cur.execute("INSERT INTO screen_names VALUES (?, ?, ?)", [
                            user.user_id, user.screen_name, current_time])

        if user_name_update:
            cur.execute("SELECT user_name FROM user_names WHERE time = (SELECT MAX(time) FROM user_names WHERE user_id = ?) AND user_id = ?", [
                        user.user_id, user.user_id])
            user_name_data = cur.fetchall()
            if len(user_name_data) == 0 or user_name_data[0][0] != user.user_name:
                cur.execute("INSERT INTO user_names VALUES (?, ?, ?)", [
                            user.user_id, user.user_name, current_time])

    for user in db_data.values():
        if not user.user_id in follows:
            cur.execute("DELETE FROM follows WHERE user_id = ?",
                        [user.user_id])
            cur.execute("INSERT INTO follow_events VALUES (?, ?, ?)", [
                        user.user_id, 0, current_time])
            message += "you unfollowed {}(_at_{})\n".format(
                user.screen_name, user.user_name)

    conn.commit()
    conn.close()
    if len(message) != 0:
        print(message)
        send_dm(message)


def send_dm(content: str):
    headers = {"content-type": "application/json"}
    payload = {
        "event": {
            "type": "message_create",
            "message_create": {
                "target": {"recipient_id": user_id},
                "message_data": {"text": content}
            }
        }
    }
    payload_json = json.dumps(payload)
    res = twitter_api.post(dm_url, headers=headers, data=payload_json)
    if res.status_code != 200:
        print("sending dm error: [status_code: {}, text: {}]".format(
            res.status_code, res.text))


while True:
    followers = twitter_api.get(followers_url, params=api_params)
    if followers.status_code == 200:
        data = json.loads(followers.text)
        follower_users = {}
        for user in data["data"]:
            follower_users[int(user["id"])] = User(
                user["id"], user["name"], user["username"])
        followers_analyzer(follower_users)
    else:
        print("getting follower error: [status_code: {}, text: {}]".format(
            followers.status_code, followers.text))

    follows = twitter_api.get(follows_url, params=api_params)
    if follows.status_code == 200:
        data = json.loads(follows.text)
        follows_users = {}
        for user in data["data"]:
            follows_users[int(user["id"])] = User(
                user["id"], user["name"], user["username"])
        follows_analyzer(follows_users)
    else:
        print("getting following error: [status_code: {}, text: {}]".format(
            follows.status_code, follows.text))

    time.sleep(3600)
