from typing import Optional, Tuple
from feedgen import FeedGenerator
import sqlite3
import time
import os

def sqlite_array(inp):
    return ","+",".join(inp)+","

class FollowingFeed(FeedGenerator):
	def __init__(self) -> None:
		self.con = sqlite3.connect("following_feed_posts.db")
		self.cur = self.con.cursor()

		# we don't bother indexing the content of posts, only when
		self.cur.execute("""CREATE TABLE IF NOT EXISTS posts (
			post_aturi TEXT PRIMARY KEY NOT NULL,
			post_timestamp_ns INTEGER NOT NULL,
			intrested_dids TEXT NOT NULL
		)""")

		# we'll be using timestamps as a cursor
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_time ON posts (post_timestamp_ns)")

		self.con.commit()
	
	def get_feed(self, requester_did: str, limit: int, cursor: Optional[str]=None) -> dict:
		if cursor is None:
			cursor = 999999999999999999
		posts = list(self.cur.execute("""
			SELECT post_aturi, post_timestamp_ns
			FROM posts
			WHERE post_timestamp_ns < ?
			AND intrested_dids LIKE ?
			ORDER BY post_timestamp_ns DESC
			LIMIT ?""", (int(cursor), "%," + requester_did + ",%", limit)).fetchall())
		res = {
			"feed": [
				{"post": aturi}
				for aturi, _ in posts
			]
		}
		if posts:
			res["cursor"] = str(posts[-1][1])
		return res

	def process_event(self, event: Tuple[str, str, Optional[dict]]) -> None:
		while os.path.isfile("addlock"):
			pass
		with open("servelock","w"):
			pass
		users = sqlite3.connect("following_feed_users.db")
		cur = users.cursor()
		cur.execute("""CREATE TABLE IF NOT EXISTS intrests (
			user_did TEXT PRIMARY KEY NOT NULL,
			following TEXT NOT NULL,
			watching TEXT NOT NULL,
			tags TEXT NOT NULL
		)""")
#		cur.execute('INSERT INTO intrests VALUES ("a",",a,b,",",art,bees,")')
		event_type, event_aturi, event_record = event
		event_did, event_collection, _ = event_aturi.removeprefix("at://").split("/")
		dids = {}
	
		if event_collection != "app.bsky.feed.post": # we only care about posts
			return
		
		if event_type != "create":
			return
		
		if event_record is None: # this shouldn't happen?
			return
		print(event_did)
		if "text" in event_record:
			tags = event_record["text"].split("#")
			if len(tags) > 2:
				tags = tags[1:]
				for tag in tags:
					cur.execute("SELECT * FROM intrests WHERE tags LIKE ?",("%,"+tag+",%",))
					x = cur.fetchall()
					for did in x:
						dids.add(did)
		cur.execute("SELECT * FROM intrests WHERE following LIKE ?",("%,"+event_did+",%",))
		x = cur.fetchall()
		for did in x:
			dids.add(did)
		cur.execute("SELECT * FROM intrests WHERE watching LIKE ?",("%,"+event_did+",%",))
		x = cur.fetchall()
		for did in x:
			dids.add(did)
		if dids == []:
			return
		print(dids)
		self.cur.execute("""INSERT OR IGNORE INTO posts (
			post_aturi,
			post_timestamp_ns,
			intrested_dids
		) VALUES (?, ?, ?)""", (event_aturi, int(time.time()*1000000),sqlite_array(dids)))

		# housekeeping: delete old posts
		self.cur.execute("DELETE FROM posts WHERE post_timestamp_ns<?", ((time.time()-24*60*60)*10*1_000_000,))
		self.con.commit()
		os.remove("servelock")
