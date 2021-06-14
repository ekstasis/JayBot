import telegram


XLM_CHAT_IDS = {'1_min_vol': -564132609,
                '1_min_diff': -500072176,
                '1_hr_diff': -599505475,
                'FOUR_HOUR': -537581487}

OTHER_CHAT_IDS = {'JayBotErrors': -533383147, 'Test': -511211282}

ETH_CHAT_IDS = {'ETH 1-Minute': -526020838, 'ETH 1-Hour': -554306398, 'ETH 4-Hour': -493878944}

TOKEN = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'

class TelegramChat:
    def __init__(self, chat_id: int, token: str = None):
        self.chat_id = chat_id
        if token is None:
            self.token = '1694974417:AAE8NAZRqD-AQBaXkw2tJjgnC7NCIa6Ss0I'
        else:
            self.token = token
        self.bot = telegram.Bot(token=self.token)

    def send_message(self, msg: str):
        self.bot.sendMessage(chat_id=self.chat_id, text=msg)