"""
Модуль для управления уведомлениями о ставках на четверти и половины
"""
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import threading
import time

# Попытаемся импортировать win10toast для Windows уведомлений
try: 
    from win10toast import ToastNotifier
    HAS_TOAST = True
except ImportError: 
    HAS_TOAST = False
    logging.warning("win10toast не установлен.  Уведомления Windows недоступны.")

try:
    import requests
except ImportError:
    requests = None


class BetsNotifier:
    """Управляет уведомлениями о ставках"""
    
    def __init__(self, telegram_token: Optional[str] = None, telegram_chat_id: Optional[str] = None):
        """
        Инициализация уведомителя
        
        Args: 
            telegram_token: Токен Telegram бота
            telegram_chat_id: ID чата для Telegram
        """
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.notified_bets = set()  # Набор ID уже отправленных уведомлений
        self. notification_thread = None
        self.is_running = False
        
        # Путь для сохранения состояния уведомлений
        self. cache_dir = os.path.join(
            os.path.expanduser("~"),
            "AppData",
            "Local",
            "ExcelAnalyzer"
        )
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_file = os.path.join(self.cache_dir, "notified_bets.json")
        
        # Загружаем ранее отправленные уведомления
        self.load_notified_bets()
        
        if HAS_TOAST:
            self.toaster = ToastNotifier()
        else:
            self.toaster = None
    
    def load_notified_bets(self):
        """Загружает список отправленных уведомлений из файла"""
        try:
            if os.path.exists(self. cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.notified_bets = set(data.get("notified", []))
                    logging.info(f"Загружено {len(self.notified_bets)} ранее отправленных уведомлений")
        except Exception as e:
            logging.error(f"Ошибка загрузки кэша уведомлений: {e}")
            self.notified_bets = set()
    
    def save_notified_bets(self):
        """Сохраняет список отправленных уведомлений в файл"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json. dump({"notified": list(self.notified_bets)}, f, ensure_ascii=False)
        except Exception as e: 
            logging.error(f"Ошибка сохранения кэша уведомлений: {e}")
    
    def get_bet_id(self, tournament:  str, team1: str, team2: str, 
                   bet_type: str, line: float, quarter_or_half: str) -> str:
        """Генерирует уникальный ID ставки"""
        return f"{tournament}_{team1}_{team2}_{bet_type}_{line}_{quarter_or_half}"
    
    def parse_time(self, time_str: str) -> Optional[datetime]:
        """Парсит время матча из строки"""
        try:
            if not time_str or time_str.strip() == "-":
                return None
            
            # Предполагаем формат "HH:MM"
            parts = time_str.strip().split(":")
            if len(parts) == 2:
                hour, minute = int(parts[0]), int(parts[1])
                now = datetime.now()
                match_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                
                # Если время прошло, считаем что это завтра
                if match_time < now:
                    match_time += timedelta(days=1)
                
                return match_time
        except Exception as e:
            logging.warning(f"Ошибка парсинга времени '{time_str}': {e}")
        
        return None
    
    def should_notify(self, match_time: Optional[datetime], 
                     bet_id:  str, minutes_before:  int = 5) -> bool:
        """Проверяет, нужно ли отправлять уведомление"""
        
        # Если уведомление уже было отправлено - не отправляем снова
        if bet_id in self.notified_bets:
            return False
        
        # Если время матча неизвестно - не отправляем
        if not match_time:
            return False
        
        now = datetime.now()
        time_diff = (match_time - now).total_seconds() / 60  # в минутах
        
        # Если матч уже стартовал или прошел - не отправляем
        if time_diff <= 0:
            return False
        
        # Отправляем, если до матча осталось в пределах minutes_before минут
        return 0 < time_diff <= minutes_before
    
    def send_desktop_notification(self, title: str, message: str, 
                                  duration: int = 10) -> bool:
        """Отправляет уведомление на рабочий стол Windows"""
        try:
            if not self.toaster:
                logging.warning("ToastNotifier не инициализирован")
                return False
            
            self.toaster.show_toast(
                title=title,
                msg=message,
                duration=duration,
                threaded=True
            )
            return True
            
        except Exception as e: 
            logging.error(f"Ошибка отправки уведомления на рабочий стол: {e}")
            return False
    
    def send_telegram_notification(self, message: str) -> bool:
        """Отправляет уведомление в Telegram"""
        try: 
            if not self.telegram_token or not self.telegram_chat_id or not requests:
                logging.warning("Telegram не настроен")
                return False
            
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            payload = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=payload, timeout=5)
            
            if response. status_code == 200:
                logging.info("Уведомление отправлено в Telegram")
                return True
            else:
                logging.error(f"Ошибка отправки в Telegram: {response.status_code}")
                return False
                
        except Exception as e: 
            logging.error(f"Ошибка отправки Telegram уведомления: {e}")
            return False
    
    def notify_bet(self, tournament: str, team1: str, team2: str, 
                   match_time: str, bet_type: str, line: float, 
                   quarter_or_half: str = "половина") -> bool:
        """
        Отправляет уведомление о ставке
        
        Args:
            tournament:  Название турнира
            team1: Первая команда
            team2: Вторая команда
            match_time: Время матча (HH:MM)
            bet_type:  OVER или UNDER
            line: Значение линии
            quarter_or_half: "половина" или "четверть"
        """
        
        # Генерируем ID ставки
        bet_id = self.get_bet_id(tournament, team1, team2, bet_type, line, quarter_or_half)
        
        # Парсим время матча
        match_datetime = self.parse_time(match_time)
        
        # Проверяем, нужно ли отправлять
        if not self.should_notify(match_datetime, bet_id):
            return False
        
        # Формируем сообщение
        time_str = match_time if match_time else "время неизвестно"
        title = f"Ставка на {quarter_or_half}!"
        message = (
            f"{time_str} | {team1} vs {team2}\n"
            f"{bet_type} {line} ({tournament})"
        )
        
        telegram_message = (
            f"📊 <b>Ставка на {quarter_or_half}! </b>\n\n"
            f"⏰ {time_str}\n"
            f"🏀 {team1} vs {team2}\n"
            f"📈 <b>{bet_type} {line}</b>\n"
            f"🏆 {tournament}"
        )
        
        # Отправляем уведомления
        desktop_sent = self.send_desktop_notification(title, message)
        telegram_sent = self.send_telegram_notification(telegram_message)
        
        # Отмечаем как отправленное
        if desktop_sent or telegram_sent:
            self. notified_bets.add(bet_id)
            self.save_notified_bets()
            logging.info(f"Уведомление отправлено: {bet_id}")
            return True
        
        return False
    
    def start_monitoring(self, bets_data: Dict, check_interval: int = 30):
        """
        Запускает фоновый мониторинг ставок
        
        Args:
            bets_data: Словарь с данными ставок
            check_interval:  Интервал проверки в секундах
        """
        
        def monitor_loop():
            self.is_running = True
            while self.is_running:
                try:
                    # Проходим по всем ставкам
                    for quarter_or_half, bets_list in bets_data.items():
                        if not isinstance(bets_list, list):
                            continue
                        
                        for bet in bets_list:
                            try:
                                # Распаковываем данные ставки
                                tournament, team1, team2, bet_type_line, line, diff = bet[: 6]
                                match_time = bet[6] if len(bet) > 6 else None
                                
                                # Определяем тип ставки из строки ("OVER 41. 2" или "UNDER 39.8")
                                bet_type = "OVER" if "OVER" in str(bet_type_line) else "UNDER"
                                
                                # Отправляем уведомление если нужно
                                self.notify_bet(
                                    tournament=tournament,
                                    team1=team1,
                                    team2=team2,
                                    match_time=match_time,
                                    bet_type=bet_type,
                                    line=line,
                                    quarter_or_half=quarter_or_half
                                )
                            except Exception as e:
                                logging.warning(f"Ошибка обработки ставки:  {e}")
                                continue
                    
                    # Ждем перед следующей проверкой
                    time.sleep(check_interval)
                    
                except Exception as e:
                    logging.error(f"Ошибка в цикле мониторинга:  {e}")
                    time. sleep(check_interval)
        
        # Запускаем мониторинг в отдельном потоке
        if self.notification_thread and self.notification_thread.is_alive():
            self.stop_monitoring()
        
        self.notification_thread = threading.Thread(target=monitor_loop, daemon=True)
        self.notification_thread.start()
        logging.info("Мониторинг уведомлений запущен")
    
    def stop_monitoring(self):
        """Останавливает мониторинг"""
        self.is_running = False
        if self.notification_thread:
            self.notification_thread.join(timeout=5)
        logging.info("Мониторинг уведомлений остановлен")
    
    def clear_history(self):
        """Очищает историю отправленных уведомлений"""
        self.notified_bets.clear()
        self.save_notified_bets()
        logging.info("История уведомлений очищена")