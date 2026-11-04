import os
import json
import sqlite3
import secrets
import hashlib
import hmac
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

APP_DIR = Path(os.getenv("DATA_DIR", "./data"))
APP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = APP_DIR / "zendegi.sqlite3"

CAPACITY = 20
ACTIVE_SECONDS = 45
SERVER_NAMES = {str(i): f"سرور {i}" for i in range(1, 6)}

app = FastAPI(title="Zendegi Life Game Server", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)


def connect():
    db = sqlite3.connect(DB_PATH, timeout=20)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def init_db():
    with connect() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            server_id TEXT NOT NULL,
            name TEXT NOT NULL,
            name_key TEXT NOT NULL,
            age INTEGER NOT NULL,
            password_salt TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            state_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(server_id, name_key)
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            player_id INTEGER NOT NULL REFERENCES players(id),
            server_id TEXT NOT NULL,
            last_seen INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_server_seen
        ON sessions(server_id, last_seen);
        """)


init_db()


def norm_name(name):
    return " ".join(name.strip().split()).casefold()


def password_hash(password, salt):
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, 240000
    ).hex()


def new_state(name, age):
    now = int(time.time() * 1000)
    return {
        "name": name,
        "age": age,
        "balance": 1000000,
        "startedAt": now,
        "gameSeconds": 0,
        "lastActiveAt": now,
        "createdAt": now,
        "updatedAt": now,
        "xp": 0,
        "inventory": {},
        "lastRewardDay": -1,
        "journal": [],
        "playerX": 0,
        "playerY": 0,
        "cameraX": 0,
        "cameraY": 0,
        "wellStock": 100,
        "wellLastRegenAt": now
    }


def active_count(db, server_id):
    cutoff = int(time.time()) - ACTIVE_SECONDS
    return db.execute(
        """SELECT COUNT(DISTINCT player_id) AS n
        FROM sessions WHERE server_id=? AND last_seen>=?""",
        (server_id, cutoff)
    ).fetchone()["n"]


def make_session(db, player_id, server_id):
    token = secrets.token_urlsafe(40)
    now = int(time.time())

    db.execute("DELETE FROM sessions WHERE player_id=?", (player_id,))
    db.execute(
        """INSERT INTO sessions(token,player_id,server_id,last_seen)
        VALUES(?,?,?,?)""",
        (token, player_id, server_id, now)
    )
    return token


def current_player(db, authorization):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "ابتدا وارد حساب شو.")

    token = authorization[7:].strip()

    row = db.execute(
        """SELECT p.*, s.token, s.server_id AS session_server
        FROM sessions s JOIN players p ON p.id=s.player_id
        WHERE s.token=?""",
        (token,)
    ).fetchone()

    if not row:
        raise HTTPException(401, "دوباره وارد حساب شو.")

    return row


class RegisterIn(BaseModel):
    server_id: int = Field(ge=1, le=5)
    name: str = Field(min_length=2, max_length=24)
    age: int = Field(ge=1, le=120)
    password: str = Field(min_length=4, max_length=64)


class LoginIn(BaseModel):
    server_id: int = Field(ge=1, le=5)
    name: str = Field(min_length=1, max_length=24)
    password: str = Field(min_length=1, max_length=64)


class SaveIn(BaseModel):
    state: dict


@app.get("/")
def root():
    # فایل index.html باید کنار server.py در مخزن GitHub باشد.
    html_file = Path(__file__).resolve().parent / "index.html"

    if html_file.is_file():
        return FileResponse(
            html_file,
            media_type="text/html; charset=utf-8"
        )

    return {
        "app": "بازی زندگی",
        "status": "online",
        "message": "index.html در کنار server.py پیدا نشد.",
        "worlds": 5,
        "capacity_per_world": CAPACITY
    }


@app.get("/api/servers")
def servers():
    result = []

    with connect() as db:
        for sid, name in SERVER_NAMES.items():
            online = active_count(db, sid)

            result.append({
                "id": sid,
                "name": name,
                "online": online,
                "capacity": CAPACITY,
                "available": max(0, CAPACITY - online),
                "full": online >= CAPACITY
            })

    return {"servers": result}


@app.post("/api/register")
def register(data: RegisterIn):
    sid = str(data.server_id)
    name = " ".join(data.name.strip().split())
    key = norm_name(name)

    salt = secrets.token_bytes(16)
    ph = password_hash(data.password, salt)
    now = int(time.time())
    state = new_state(name, data.age)

    with connect() as db:
        exists = db.execute(
            "SELECT 1 FROM players WHERE server_id=? AND name_key=?",
            (sid, key)
        ).fetchone()

        if exists:
            raise HTTPException(409, "این نام قبلاً ثبت شده است.")

        if active_count(db, sid) >= CAPACITY:
            raise HTTPException(409, "این سرور پر است.")

        cur = db.execute(
            """INSERT INTO players
            (server_id,name,name_key,age,password_salt,password_hash,
            state_json,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                sid, name, key, data.age, salt.hex(), ph,
                json.dumps(state, ensure_ascii=False), now, now
            )
        )

        token = make_session(db, cur.lastrowid, sid)

    return {
        "token": token,
        "server_id": sid,
        "server_name": SERVER_NAMES[sid],
        "state": state
    }


@app.post("/api/login")
def login(data: LoginIn):
    sid = str(data.server_id)
    key = norm_name(data.name)

    with connect() as db:
        row = db.execute(
            "SELECT * FROM players WHERE server_id=? AND name_key=?",
            (sid, key)
        ).fetchone()

        if not row:
            raise HTTPException(401, "حساب پیدا نشد.")

        expected = password_hash(
            data.password,
            bytes.fromhex(row["password_salt"])
        )

        if not hmac.compare_digest(expected, row["password_hash"]):
            raise HTTPException(401, "رمز اشتباه است.")

        if active_count(db, sid) >= CAPACITY:
            existing = db.execute(
                """SELECT 1 FROM sessions
                WHERE player_id=? AND last_seen>=?""",
                (row["id"], int(time.time()) - ACTIVE_SECONDS)
            ).fetchone()

            if not existing:
                raise HTTPException(409, "این سرور پر است.")

        token = make_session(db, row["id"], sid)
        state = json.loads(row["state_json"])

    return {
        "token": token,
        "server_id": sid,
        "server_name": SERVER_NAMES[sid],
        "state": state
    }


@app.post("/api/save")
def save(data: SaveIn, authorization: str | None = Header(default=None)):
    with connect() as db:
        player = current_player(db, authorization)

        state = data.state
        state["name"] = player["name"]
        state["age"] = player["age"]
        state.pop("passwordHash", None)

        now = int(time.time())

        db.execute(
            "UPDATE players SET state_json=?, updated_at=? WHERE id=?",
            (
                json.dumps(state, ensure_ascii=False),
                now,
                player["id"]
            )
        )

        db.execute(
            "UPDATE sessions SET last_seen=? WHERE token=?",
            (now, player["token"])
        )

    return {"ok": True, "saved_at": now}


@app.get("/api/players")
def players(authorization: str | None = Header(default=None)):
    now = int(time.time())
    cutoff = now - ACTIVE_SECONDS

    with connect() as db:
        player = current_player(db, authorization)

        db.execute(
            "UPDATE sessions SET last_seen=? WHERE token=?",
            (now, player["token"])
        )

        rows = db.execute(
            """SELECT p.name, p.state_json
            FROM sessions s JOIN players p ON p.id=s.player_id
            WHERE s.server_id=? AND s.last_seen>=?
            ORDER BY p.name""",
            (player["session_server"], cutoff)
        ).fetchall()

    visible = []

    for row in rows:
        state = json.loads(row["state_json"])

        visible.append({
            "name": row["name"],
            "playerX": state.get("playerX", 0),
            "playerY": state.get("playerY", 0)
        })

    return {"players": visible}


@app.post("/api/heartbeat")
def heartbeat(authorization: str | None = Header(default=None)):
    with connect() as db:
        player = current_player(db, authorization)

        db.execute(
            "UPDATE sessions SET last_seen=? WHERE token=?",
            (int(time.time()), player["token"])
        )

    return {"ok": True}


@app.post("/api/logout")
def logout(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        return {"ok": True}

    token = authorization[7:].strip()

    with connect() as db:
        db.execute("DELETE FROM sessions WHERE token=?", (token,))

    return {"ok": True}


@app.get("/api/me")
def me(authorization: str | None = Header(default=None)):
    with connect() as db:
        player = current_player(db, authorization)

        return {
            "server_id": player["server_id"],
            "server_name": SERVER_NAMES[player["server_id"]],
            "name": player["name"],
            "state": json.loads(player["state_json"])
}
