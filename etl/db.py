import sqlite3
from typing import List

from etl.config import get_config


class DBManager(object):
    """Singleton Database Manager"""

    __db = None

    def __init__(self):
        self.config = get_config()
        self._init_db()

    def _init_db(self):
        if self.__db is None:
            self.__db = sqlite3.connect(self.config.database_uri)

    def get_cursor(self):
        return self.__db.cursor()

    def fetch(self, query: str) -> List[sqlite3.Row]:
        cur = self.get_cursor()
        return cur.execute(query).fetchall()

    def execute_script(self, script_path: str):
        with open(script_path, "rb") as f:
            self.__db.executescript(f.read().decode("utf-8"))


def init_analytics_schema():
    """Create tables for `analytics` database"""
    db_manager = DBManager()
    db_manager.execute_script(
        script_path=db_manager.config.analytics_schema_script_path
    )
