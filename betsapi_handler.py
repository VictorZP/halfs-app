import requests
import logging
import re
import traceback
import json
from datetime import datetime

class BetsAPIHandler:
    """Обработчик для работы с BetsAPI"""
    
    def __init__(self, api_token="37675-eUkGxZrBgrcQwU"):
        self.api_token = api_token
        self.base_url = "https://api.b365api.com/v3"
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        # Обновляем текущее время UTC и логин пользователя
        self.current_utc = "2025-08-27 02:53:15"
        self.user_login = "sgsdgsdgds"

    def get_basketball_matches(self, date_str):
        """Получает все баскетбольные матчи на указанную дату
        date_str: формат YYYYMMDD"""
        try:
            all_matches = []
            
            # Получаем upcoming матчи
            url = f"{self.base_url}/events/upcoming"
            params = {
                "token": self.api_token,
                "sport_id": 18,  # Basketball
                "day": date_str
            }
            
            logging.info(f"[{self.user_login}] Запрос upcoming матчей: {url}")
            logging.info(f"Параметры: {params}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") == 1:
                    events = data.get("results", [])
                    for event in events:
                        # Проверяем, не содержит ли название турнира "Ebasketball"
                        league_name = event.get("league", {}).get("name", "").lower()
                        if "ebasketball" not in league_name:
                            event["match_type"] = "upcoming"
                            all_matches.append(event)
                    logging.info(f"Найдено upcoming матчей (без Ebasketball): {len(all_matches)}")
            
            # Получаем inplay матчи
            url = f"{self.base_url}/events/inplay"
            params = {
                "token": self.api_token,
                "sport_id": 18,
                "day": date_str
            }
            
            logging.info(f"[{self.user_login}] Запрос inplay матчей: {url}")
            logging.info(f"Параметры: {params}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") == 1:
                    events = data.get("results", [])
                    # Конвертируем время матча в UTC
                    current_time = int(datetime.strptime(self.current_utc, "%Y-%m-%d %H:%M:%S").timestamp())
                    
                    for event in events:
                        # Проверяем, не содержит ли название турнира "Ebasketball"
                        league_name = event.get("league", {}).get("name", "").lower()
                        if "Ebasketball" not in league_name:
                            event_time = int(event.get("time", 0))
                            event_date = datetime.fromtimestamp(event_time).strftime("%Y%m%d")
                            
                            # Проверяем время матча
                            if event_date == date_str or abs(current_time - event_time) < 7200:  # 2 часа до/после
                                event["match_type"] = "inplay"
                                all_matches.append(event)
                    
                    logging.info(f"Найдено inplay матчей (без Ebasketball): {len([e for e in all_matches if e['match_type'] == 'inplay'])}")
            
            return all_matches
            
        except Exception as e:
            logging.error(f"[{self.user_login}] Ошибка при получении матчей: {str(e)}")
            return []

    def test_api_access(self):
        """Тестовый метод для проверки доступа к API"""
        try:
            url = f"{self.base_url}/bet365/event"
            params = {
                "token": self.api_token,
                "event_id": "9961953",
                "source": "bet365"
            }

            logging.info(f"[{self.user_login}] Тестовый запрос к API:")
            logging.info(f"URL: {url}")
            logging.info(f"Параметры: {params}")

            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            logging.info(f"Статус ответа: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                logging.info("Структура ответа:")
                logging.info(json.dumps(data, indent=2))

                if data.get("success") == 1:
                    logging.info(f"[{self.user_login}] API доступен")
                    return True, "API доступен"
                else:
                    error = data.get("error", "Unknown error")
                    logging.error(f"[{self.user_login}] Ошибка API: {error}")
                    return False, f"Ошибка API: {error}"
            else:
                logging.error(f"[{self.user_login}] Ошибка HTTP {response.status_code}: {response.text}")
                return False, f"Ошибка HTTP {response.status_code}"

        except Exception as e:
            logging.error(f"[{self.user_login}] Ошибка при тестировании API: {str(e)}")
            return False, f"Ошибка при тестировании API: {str(e)}"

    def get_match_total(self, event_id, match_type):
        """Получает тотал матча из API"""
        try:
            logging.info(f"\n{'='*50}")
            logging.info(f"[{self.user_login}] ПОЛУЧЕНИЕ ТОТАЛА ДЛЯ МАТЧА ID: {event_id} (тип: {match_type})")
            
            url = f"{self.base_url}/event/odds/basketball"
            params = {
                "token": self.api_token,
                "event_id": event_id
            }

            logging.info(f"Запрос к Basketball API: {url}")
            logging.info(f"Параметры: {params}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                if data.get("success") == 1:
                    results = data.get("results", {})
                    odds = results.get("odds", {})
                    
                    for bookmaker_id, bookmaker_odds in odds.items():
                        if not isinstance(bookmaker_odds, dict):
                            continue
                            
                        for market_id, market_data in bookmaker_odds.items():
                            if not isinstance(market_data, dict):
                                continue
                            
                            for key in ["total", "o/u", "ou", "value"]:
                                if key in market_data:
                                    try:
                                        total = float(market_data[key])
                                        if total > 0:
                                            logging.info(f"Найден тотал: {total}")
                                            return {
                                                "value": total,
                                                "bookmaker": f"id_{bookmaker_id}",
                                                "type": "basketball"
                                            }
                                    except:
                                        continue

                    logging.info("Тотал не найден в данных Basketball API")
                    return None
                else:
                    error = data.get("error", "Unknown error")
                    logging.error(f"[{self.user_login}] Ошибка Basketball API: {error}")
                    return None
            else:
                logging.error(f"[{self.user_login}] Ошибка HTTP {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logging.error(f"[{self.user_login}] Исключение при получении тотала: {str(e)}")
            logging.error(traceback.format_exc())
            return None

    def get_match_details(self, event_id):
        """Получает подробную информацию о матче"""
        try:
            url = f"{self.base_url}/event"
            params = {
                "token": self.api_token,
                "event_id": event_id
            }
            
            logging.info(f"[{self.user_login}] Запрос деталей матча {event_id}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") == 1:
                    return data.get("results", {})
            return None
            
        except Exception as e:
            logging.error(f"[{self.user_login}] Ошибка получения деталей матча: {str(e)}")
            return None

    def get_live_scores(self, event_id):
        """Получает текущий счет матча"""
        try:
            url = f"{self.base_url}/event/live"
            params = {
                "token": self.api_token,
                "event_id": event_id
            }
            
            logging.info(f"[{self.user_login}] Запрос live счета матча {event_id}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") == 1:
                    scores = data.get("results", {}).get("scores", {})
                    return {
                        "home": scores.get("home", "0"),
                        "away": scores.get("away", "0"),
                        "quarter": data.get("results", {}).get("quarter")
                    }
            return None
            
        except Exception as e:
            logging.error(f"[{self.user_login}] Ошибка получения live счета: {str(e)}")
            return None

    def format_match_time(self, match_data):
        """Форматирует время матча в читаемый вид"""
        try:
            match_time = int(match_data.get("time", 0))
            if match_time:
                return datetime.fromtimestamp(match_time).strftime("%H:%M")
            return "N/A"
        except Exception as e:
            logging.error(f"[{self.user_login}] Ошибка форматирования времени: {str(e)}")
            return "N/A"

    def get_league_matches(self, league_id, date_str):
        """Получает все матчи определенной лиги на указанную дату"""
        try:
            url = f"{self.base_url}/events/league"
            params = {
                "token": self.api_token,
                "league_id": league_id,
                "day": date_str
            }
            
            logging.info(f"[{self.user_login}] Запрос матчей лиги {league_id}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") == 1:
                    return data.get("results", [])
            return []
            
        except Exception as e:
            logging.error(f"[{self.user_login}] Ошибка получения матчей лиги: {str(e)}")
            return []

    def get_team_matches(self, team_id, date_str):
        """Получает все матчи определенной команды на указанную дату"""
        try:
            url = f"{self.base_url}/events/team"
            params = {
                "token": self.api_token,
                "team_id": team_id,
                "day": date_str
            }
            
            logging.info(f"[{self.user_login}] Запрос матчей команды {team_id}")
            
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") == 1:
                    return data.get("results", [])
            return []
            
        except Exception as e:
            logging.error(f"[{self.user_login}] Ошибка получения матчей команды: {str(e)}")
            return []