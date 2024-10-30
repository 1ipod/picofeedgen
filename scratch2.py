import sqlite3

users = sqlite3.connect("following_feed_users.db")
cur = users.cursor()
cur.execute("DROP TABLE IF EXISTS intrests")
cur.execute("""CREATE TABLE IF NOT EXISTS intrests (
			user_did TEXT PRIMARY KEY NOT NULL,
			following TEXT NOT NULL,
            watching TEXT NOT NULL,
			tags TEXT NOT NULL
		)""")
#cur.execute('INSERT INTO intrests VALUES ("did:plc:kvwvcn5iqfooopmyzvb4qzba","",",a,b,",",art,bees,")')
for tag in ["art","bees","cats"]:
    cur.execute("SELECT * FROM intrests WHERE tags LIKE ?",("%,"+tag+",%",))
    print(cur.fetchall())
users.commit()
users.close()