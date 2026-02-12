import traceback
from datetime import datetime
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                           QLabel, QListWidget, QListWidgetItem, QMessageBox)
from PyQt5.QtCore import QTimer, pyqtSignal, Qt
from PyQt5.QtGui import QColor
import logging
import re
from betsapi_handler import BetsAPIHandler


class BetsAPIMonitor(QWidget):
    # Определяем сигнал для уведомлений
    match_notification = pyqtSignal(str, str, str)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Инициализация основных атрибутов
        self.matches = {}  # Словарь для хранения матчей
        self.favorites = set()  # Множество для хранения избранных матчей
        self.viewed_quarters = {}  # Словарь для хранения просмотренных четвертей
        
        # Создание основного layout
        self.layout = QVBoxLayout(self)
        
        # Создание и настройка списка матчей
        self.matches_list = QListWidget()
        self.layout.addWidget(self.matches_list)
        
        # Создание кнопок
        buttons_layout = QHBoxLayout()
        
        self.update_button = QPushButton("Обновить")
        self.update_button.clicked.connect(self.update_matches)
        buttons_layout.addWidget(self.update_button)
        
        self.layout.addLayout(buttons_layout)
        
        # Настройка таймеров
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.update_matches)
        self.update_timer.start(600000)  # 10 минут
        
        self.favorites_timer = QTimer()
        self.favorites_timer.timeout.connect(self.check_favorites)
        self.favorites_timer.start(60000)  # 1 минута
        
        # Инициализация API handler
        self.api_handler = BetsAPIHandler()
        
        # Первоначальное обновление
        self.update_matches()

    def add_log(self, message):
        """Добавляет сообщение в лог"""
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_message = f"[{timestamp}] {message}"
            
            # Проверяем, создан ли уже список
            if hasattr(self, 'matches_list'):
                # Добавляем сообщение в список матчей как временное уведомление
                item = QListWidgetItem(log_message)
                # Устанавливаем цвет текста для лога
                item.setForeground(QColor("#999999"))
                self.matches_list.addItem(item)
                # Прокручиваем к последнему сообщению
                self.matches_list.scrollToBottom()
            
            # Также записываем в файл лога
            logging.info(message)
            
        except Exception as e:
            logging.error(f"Ошибка при добавлении в лог: {str(e)}")

    def update_matches(self):
        """Обновление всех матчей"""
        try:
            self.add_log("Начало обновления списка матчей...")
            current_date = datetime.now().strftime("%Y%m%d")
            
            # Используем существующий BetsAPIHandler для получения матчей
            matches_data = self.api_handler.get_basketball_matches(current_date)
            
            if not matches_data:
                self.add_log("Нет доступных матчей")
                return
                
            # Очищаем старый список
            self.matches.clear()
            
            # Обрабатываем полученные матчи
            for match in matches_data:
                match_id = match.get("id")
                if not match_id:
                    continue
                    
                # Получаем имена команд
                home_team = match.get("home", {}).get("name", "")
                away_team = match.get("away", {}).get("name", "")
                league = match.get("league", {}).get("name", "")
                
                # Получаем время матча
                match_time = match.get("time")
                if match_time:
                    time_str = datetime.fromtimestamp(int(match_time)).strftime("%H:%M")
                else:
                    time_str = "N/A"
                
                # Сохраняем матч
                self.matches[match_id] = {
                    "tournament": league,
                    "team1": home_team,
                    "team2": away_team,
                    "time": time_str,
                    "status": match.get("match_type", "unknown")
                }
            
            # Обновляем отображение
            self.update_matches_list()
            self.add_log(f"Обновлено матчей: {len(self.matches)}")
            
        except Exception as e:
            error_msg = f"Ошибка обновления матчей: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_msg)
            self.add_log(error_msg)

    def check_favorites(self):
        """Проверка избранных матчей"""
        try:
            if not self.favorites:
                return
            self.add_log("Проверка избранных матчей...")
            
            for match_id in self.favorites:
                match = self.matches.get(match_id)
                if not match:
                    continue
                    
                # Если матч в live
                if match.get("status") == "inplay":
                    # Получаем детали матча через API
                    total_data = self.api_handler.get_match_total(match_id, "inplay")
                    
                    if total_data:
                        time_str = match.get("time", "")
                        if self.is_quarter_end(time_str):
                            quarter = self.get_quarter_number(time_str)
                            if quarter in [1, 3] and not self.is_quarter_viewed(match_id, quarter):
                                # Отправляем уведомление
                                notification = f"Конец {quarter}-й четверти"
                                self.match_notification.emit(
                                    match["tournament"],
                                    f"{match['team1']} vs {match['team2']}",
                                    notification
                                )
        except Exception as e:
            error_msg = f"Ошибка проверки избранных: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_msg)
            self.add_log(error_msg)

    def update_matches_list(self):
        """Обновляет отображение списка матчей в интерфейсе"""
        try:
            self.matches_list.clear()
            for match_id, match in self.matches.items():
                item_text = (f"{match['tournament']} - {match['time']}\n"
                            f"{match['team1']} vs {match['team2']}")
                
                item = QListWidgetItem(item_text)
                
                # Устанавливаем цвет в зависимости от статуса
                if match_id in self.favorites:
                    item.setBackground(QColor("#e6ffe6"))  # Светло-зеленый для избранных
                if match.get("status") == "inplay":
                    item.setForeground(QColor("#ff0000"))  # Красный для live матчей
                
                # Сохраняем ID матча в данных элемента
                item.setData(Qt.UserRole, match_id)
                self.matches_list.addItem(item)
                
        except Exception as e:
            error_msg = f"Ошибка обновления списка: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_msg)
            self.add_log(error_msg)

    def mousePressEvent(self, event):
        """Обработка клика мыши по списку матчей"""
        try:
            if event.button() == Qt.RightButton:
                item = self.matches_list.itemAt(event.pos())
                if item:
                    match_id = item.data(Qt.UserRole)
                    if match_id in self.favorites:
                        self.remove_from_favorites(match_id)
                    else:
                        self.add_to_favorites(match_id)
        except Exception as e:
            logging.error(f"Ошибка обработки клика: {str(e)}")

    def add_to_favorites(self, match_id):
        """Добавление матча в избранное"""
        try:
            if match_id not in self.favorites:
                self.favorites.add(match_id)
                self.add_log(f"Матч {match_id} добавлен в избранное")
                self.update_matches_list()
        except Exception as e:
            logging.error(f"Ошибка добавления в избранное: {str(e)}")

    def remove_from_favorites(self, match_id):
        """Удаление матча из избранного"""
        try:
            if match_id in self.favorites:
                self.favorites.remove(match_id)
                self.add_log(f"Матч {match_id} удален из избранного")
                self.update_matches_list()
        except Exception as e:
            logging.error(f"Ошибка удаления из избранного: {str(e)}")

    def is_quarter_end(self, time_str):
        """Проверяет, является ли текущее время концом четверти"""
        try:
            # Примеры форматов времени: "Q1 10:00", "Q2 END", "Q3 02:30"
            return "END" in time_str.upper()
        except Exception as e:
            logging.error(f"Ошибка проверки конца четверти: {str(e)}")
            return False

    def get_quarter_number(self, time_str):
        """Получает номер текущей четверти"""
        try:
            # Ищем число после Q: Q1, Q2, Q3, Q4
            match = re.search(r'Q(\d)', time_str)
            if match:
                return int(match.group(1))
            return 0
        except Exception as e:
            logging.error(f"Ошибка получения номера четверти: {str(e)}")
            return 0

    def is_quarter_viewed(self, match_id, quarter):
        """Проверяет, было ли уже отправлено уведомление для данной четверти"""
        try:
            key = f"{match_id}_{quarter}"
            if key not in self.viewed_quarters:
                self.viewed_quarters[key] = True
                return False
            return True
        except Exception as e:
            logging.error(f"Ошибка проверки просмотра четверти: {str(e)}")
            return True

    def closeEvent(self, event):
        """Обработчик закрытия окна"""
        try:
            # Останавливаем таймеры
            self.update_timer.stop()
            self.favorites_timer.stop()
            event.accept()
        except Exception as e:
            logging.error(f"Ошибка при закрытии: {str(e)}")
            event.accept()