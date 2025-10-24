import sqlite3
import threading
from typing import Optional, List, Tuple, Dict, Any
import time
import os

DB_PATH = os.getenv("DB_PATH", "./data/aggregator.db")

class DB:
    def __init__(self, path: Optional[str] = None):
        self.path = path or DB_PATH
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self):
        with self.lock:
            cur = self._conn.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dedup (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                processed_at REAL NOT NULL,
                PRIMARY KEY (topic, event_id)
            )""")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                payload TEXT,
                processed_at REAL NOT NULL,
                PRIMARY KEY (topic, event_id)
            )""")
            self._conn.commit()

    def mark_processed(self, topic: str, event_id: str, timestamp: str, source: str, payload_json: str) -> bool:
        """
        Insert into dedup and events. Return True if inserted (i.e., was not duplicate).
        If already exists, returns False.
        """
        now = time.time()
        with self.lock:
            cur = self._conn.cursor()
            try:
                cur.execute("INSERT INTO dedup (topic, event_id, processed_at) VALUES (?, ?, ?)",
                            (topic, event_id, now))
                cur.execute("""INSERT INTO events (topic, event_id, timestamp, source, payload, processed_at)
                                VALUES (?, ?, ?, ?, ?, ?)""",
                            (topic, event_id, timestamp, source, payload_json, now))
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                # already processed
                return False

    def is_processed(self, topic: str, event_id: str) -> bool:
        with self.lock:
            cur = self._conn.cursor()
            cur.execute("SELECT 1 FROM dedup WHERE topic=? AND event_id=? LIMIT 1", (topic, event_id))
            return cur.fetchone() is not None

    def list_events(self, topic: str):
        with self.lock:
            cur = self._conn.cursor()
            cur.execute("SELECT topic, event_id, timestamp, source, payload, processed_at FROM events WHERE topic=? ORDER BY processed_at ASC", (topic,))
            rows = cur.fetchall()
            return rows

    def get_topics_count(self) -> Tuple[int, List[str]]:
        with self.lock:
            cur = self._conn.cursor()
            cur.execute("SELECT DISTINCT topic FROM events")
            rows = [r[0] for r in cur.fetchall()]
            return len(rows), rows

    def close(self):
        try:
            self._conn.close()
        except:
            pass
