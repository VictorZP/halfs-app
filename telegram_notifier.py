import requests
import logging

class TelegramNotifier:
    def __init__(self, bot_token, chat_id):
        self.bot_token = "8256682834:AAEbrCD6AR2Zytmv_dhGD8mcP4oJ45UnzjQ"
        self.chat_id = "197670357"
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def send_notification(self, tournament, teams, notification_type):
        """Отправка уведомления в Telegram"""
        message = f"🏀 {tournament}\n{teams}\n📢 {notification_type}"
        
        try:
            response = requests.post(
                self.api_url,
                json={
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "HTML"
                }
            )
            if not response.ok:
                logging.error(f"Ошибка отправки в Telegram: {response.text}")
                
        except Exception as e:
            logging.error(f"Ошибка Telegram: {str(e)}")