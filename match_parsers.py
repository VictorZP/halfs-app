import logging
import re
import os
from abc import ABC, abstractmethod
from datetime import datetime, date
import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from typing import List, Optional, Dict
import traceback

class BaseMatchParser(ABC):
    """Базовый абстрактный класс для парсеров сайтов с матчами"""
    
    def __init__(self, handler):
        self.handler = handler
        self.driver = handler.driver
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def validate_url(self, url: str) -> bool:
        """Проверяет, подходит ли URL для данного парсера"""
        pass
        
    @abstractmethod
    async def parse_matches(self, url: str, target_date: datetime) -> list:
        """Парсит матчи с сайта для указанной даты"""
        pass

    def format_date(self, date: datetime) -> str:
        """Форматирует дату для запроса к сайту"""
        return date.strftime("%Y-%m-%d")

    def normalize_match_url(self, url: str) -> str:
        """Нормализует URL матча"""
        return url.rstrip('/')

    def add_log(self, message: str, level: str = "INFO"):
        """Добавляет сообщение в лог"""
        if hasattr(self.handler, 'add_log'):
            self.handler.add_log(message, level)
        else:
            if level.upper() == "ERROR":
                self.logger.error(message)
            elif level.upper() == "WARNING":
                self.logger.warning(message)
            else:
                self.logger.info(message)

class MultiSourceMatchFinder:
    """Менеджер для работы с разными источниками данных о матчах"""
    
    def __init__(self):
        self.parsers = []
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def register_parser(self, parser: BaseMatchParser):
        """Регистрирует новый парсер"""
        self.parsers.append(parser)
        self.logger.info(f"Зарегистрирован парсер: {parser.__class__.__name__}")
        
    async def find_matches(self, date: datetime, progress_callback=None) -> list:
        """Ищет матчи во всех источниках параллельно"""
        tasks = []
        
        for parser in self.parsers:
            if hasattr(parser, 'base_url'):
                self.logger.info(f"Запуск парсера {parser.__class__.__name__} для {parser.base_url}")
                task = asyncio.create_task(parser.parse_matches(parser.base_url, date))
                tasks.append(task)
                
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_matches = []
        for i, parser_results in enumerate(results):
            parser_name = self.parsers[i].__class__.__name__
            if isinstance(parser_results, list):
                self.logger.info(f"Парсер {parser_name} нашел {len(parser_results)} матчей")
                all_matches.extend(parser_results)
            elif isinstance(parser_results, Exception):
                self.logger.error(f"Ошибка в парсере {parser_name}: {str(parser_results)}")
                
        return all_matches

class LNBPParser(BaseMatchParser):
    """Парсер для сайта Мексиканской Национальной Профессиональной Баскетбольной Лиги"""
    
    def __init__(self, handler):
        super().__init__(handler)
        self.base_url = "https://www.lnbp.mx/stats.html"

    def validate_url(self, url: str) -> bool:
        return "lnbp.mx" in url.lower()

    def _format_date_spanish(self, date: datetime) -> str:
        """Форматирует дату в испанском формате"""
        months_es = {
            1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 
            5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
            9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
        }
        return f"{date.day} de {months_es[date.month]} de {date.year}"

    async def parse_matches(self, url: str, target_date: datetime) -> list:
        """Парсит матчи с сайта LNBP"""
        matches = []
        try:
            self.add_log("Начало парсинга LNBP...")
            self.driver.get(url)
            
            # Ждем загрузку страницы
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Форматируем дату для поиска
            date_str = self._format_date_spanish(target_date)
            self.add_log(f"Ищем матчи на дату: {date_str}")

            # Ищем матчи на странице
            match_elements = self.driver.find_elements(
                By.CSS_SELECTOR, 
                ".match-row, .game-row, .event-row"
            )

            for match in match_elements:
                try:
                    match_text = match.text.strip()
                    
                    # Проверяем наличие даты
                    if date_str.lower() not in match_text.lower():
                        continue
                        
                    # Ищем команды
                    teams = match.find_elements(By.CSS_SELECTOR, ".team-name, .team")
                    if len(teams) < 2:
                        continue
                        
                    home_team = teams[0].text.strip()
                    away_team = teams[1].text.strip()
                    
                    # Определяем статус матча
                    match_type = "SCHEDULED"
                    if "vivo" in match_text.lower():
                        match_type = "LIVE"
                    elif "final" in match_text.lower():
                        match_type = "FINAL"
                    
                    # Получаем URL матча
                    match_url = ""
                    try:
                        link = match.find_element(By.TAG_NAME, "a")
                        match_url = link.get_attribute("href")
                    except:
                        self.add_log("URL матча не найден", "WARNING")
                        continue

                    if match_url:
                        match_data = {
                            'url': match_url,
                            'type': match_type,
                            'teams': {
                                'home': home_team,
                                'away': away_team
                            },
                            'text': f"{home_team} vs {away_team}",
                            'date': target_date.strftime('%d/%m/%Y'),
                            'source': 'LNBP'
                        }
                        matches.append(match_data)
                        self.add_log(f"Добавлен матч: {home_team} vs {away_team} ({match_type})")
                        
                except Exception as e:
                    self.add_log(f"Ошибка при обработке матча: {str(e)}", "ERROR")
                    continue

            self.add_log(f"Найдено матчей LNBP: {len(matches)}")
            return matches

        except Exception as e:
            self.add_log(f"Ошибка при парсинге LNBP: {str(e)}\n{traceback.format_exc()}", "ERROR")
            return []
        
class FibaLiveStatsParser(BaseMatchParser):
    """Парсер для сайта FibaLiveStats"""
    
    def __init__(self, handler):
        super().__init__(handler)
        self.base_url = "https://fibalivestats.dcd.shared.geniussports.com"

    def validate_url(self, url: str) -> bool:
        return "fibalivestats" in url.lower()

    async def parse_matches(self, url: str, target_date: datetime) -> list:
        """Парсит матчи с сайта FibaLiveStats"""
        matches = []
        try:
            # Используем существующую функциональность FibaLiveStatsHandler
            if hasattr(self.handler, 'check_date_in_page'):
                matches = self.handler.check_date_in_page(url, target_date)
                
            return matches if matches else []

        except Exception as e:
            self.add_log(f"Ошибка при парсинге FibaLiveStats: {str(e)}\n{traceback.format_exc()}", "ERROR")
            return []

    def normalize_match_url(self, url: str) -> str:
        """Нормализует URL матча FibaLiveStats"""
        try:
            # Убираем trailing slash
            url = url.rstrip('/')
            
            # Проверяем что это URL матча
            if '/u/' not in url:
                return url
                
            # Извлекаем ID матча
            match_id = url.split('/u/')[-1].split('/')[0]
            
            # Формируем канонический URL
            return f"https://fibalivestats.dcd.shared.geniussports.com/u/{match_id}"
            
        except Exception:
            return url