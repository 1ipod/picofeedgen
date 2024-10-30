from atproto import Client, IdResolver, models
import time, os, sqlite3

USERNAME = ''
PASSWORD = ''  # never hardcode your password in a real application

def sqlite_array(inp):
    return ","+",".join(inp)+","

def main() -> None:
    # create client instance and login
    while os.path.isfile("servelock"):
        pass
    with open("addlock","w") as f:
        f.write("test")
    users = sqlite3.connect("following_feed_users.db")
    cur = users.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS intrests (
			user_did TEXT PRIMARY KEY NOT NULL,
			following TEXT NOT NULL,
            watching TEXT NOT NULL,
			tags TEXT NOT NULL
		)""")
    client = Client()
    x = client.login(USERNAME, PASSWORD)  # use App Password with access to Direct Messages!
    own_did = x.did
    # create client proxied to Bluesky Chat service
    dm_client = client.with_bsky_chat_proxy()
    # create shortcut to convo methods
    dm = dm_client.chat.bsky.convo

    convo_list = dm.list_convos()  # use limit and cursor to paginate
    print(f'Your conversations ({len(convo_list.convos)}):')
    for convo in convo_list.convos:
        members = [member.did for member in convo.members]
        members.remove(own_did)
        print(members)
        if convo.last_message.text in ["finish","Finish"]:
            print("found")
            tags = []
            accounts = []
            for msg in dm.get_messages(models.chat.bsky.convo.get_messages.Params(convo_id=convo.id)).messages[1:]:
                if msg.text in ["start","Start"]:
                    dm.send_message(models.ChatBskyConvoSendMessage.Data(convo_id=convo.id,message=models.ChatBskyConvoDefs.MessageInput(text='ACK new prefrences',),))  
                    print(tags)
                    print(accounts)
                    cur.execute('INSERT INTO intrests VALUES (?,?,?,?)',(members[0],"",sqlite_array(accounts),sqlite_array(tags)))
                    break
                t = msg.text[1:]
                match msg.text[0]:
                    case "#":
                        tags += [t]
                    case "@":
                        accounts += [t]     
    cur.execute("SELECT user_did FROM intrests")
    for acc in cur.fetchall():
        client.get_follows(acc)
    os.remove("addlock")  

while True:
    main()
    time.sleep(10*60)
