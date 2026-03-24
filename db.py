import sqlite3
from datetime import datetime, timedelta
import os

DB_PATH = "bot_data.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            language TEXT NOT NULL,
            server_type TEXT NOT NULL,
            area_size TEXT NOT NULL,
            vr_device TEXT,
            duration INTEGER NOT NULL,
            city TEXT NOT NULL,
            topic_id INTEGER NOT NULL,
            message_link TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def save_request(data, message_link: str, expires_at: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO requests (
            user_id, language, server_type, area_size, vr_device,
            duration, city, topic_id, message_link, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data["user_id"], data["language"], data["server_type"], data["area_size"],
        data.get("vr_device"), data["duration"], data["city"],
        data["topic_id"], message_link, expires_at
    ))
    conn.commit()
    conn.close()
    print(f"✅ Запрос сохранён: {data['city']}")

def get_user_requests(user_id, days=30):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    date_limit = datetime.now() - timedelta(days=days)
    cursor.execute('''
        SELECT id, city, duration, created_at FROM requests
        WHERE user_id = ? AND created_at >= ?
        ORDER BY created_at DESC
    ''', (user_id, date_limit))
    rows = cursor.fetchall()
    conn.close()
    return rows

def get_request_by_id(req_id: int):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM requests WHERE id = ?", (req_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        # Индексы: 0=id, 1=user_id, 2=language, 3=server_type,
        # 4=area_size, 5=vr_device, 6=duration, 7=city,
        # 8=topic_id, 9=message_link, 10=created_at, 11=expires_at
        return {
            "id": row[0],
            "user_id": row[1],
            "language": row[2],
            "server_type": row[3],
            "area_size": row[4],
            "vr_device": row[5],
            "duration": row[6],
            "city": row[7],
            "topic_id": row[8],
            "message_link": row[9],
            "created_at": row[10],
            "expires_at": row[11]
        }
    return None
