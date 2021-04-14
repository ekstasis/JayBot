import telegram

# ALERT_CHAT_ID = -511211282
# ERROR_CHAT_ID = -533383147
# WHALE_CHAT_ID = -504882847

CHAT_IDS = {'1_min_vol': -564132609,
            '1_min_diff': -500072176,
            '1_hr_diff': -599505475,
            'JayBotErrors': -533383147,
            'Test': -511211282,
            'FOUR_HOUR': -537581487
            }

TOKEN = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'

class TelegramChat:
    def __init__(self, chat_id: int, name: str, token: str = None):
        self.chat_id = chat_id
        if token is None:
            self.token = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'
        else:
            self.token = token
        self.bot = telegram.Bot(token=self.token)

    def send_message(self, msg: str):
        self.bot.sendMessage(chat_id=self.chat_id, text=msg)

    @staticmethod
    def create_bot(name):
        return TelegramChat(chat_id=CHAT_IDS[name], name=name)