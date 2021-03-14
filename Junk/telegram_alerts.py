import logging

import telegram


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

bot = telegram.Bot(token='1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I')
alert_chat_id = -511211282
error_chat_id = -533383147
whale_alert_id = -504882847
# bot.sendMessage(chat_id=error_chat_id, text="hello")
updates = bot.getUpdates()