import telegram

TOKEN = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'

bot = telegram.Bot(token=TOKEN)
updates = bot.getUpdates()

chats = {}

for update in updates:
    try:
        id = update['message']['chat']['id']
        name = update['message']['chat']['title']
        chats[name] = id
    except:
        pass

print(chats)