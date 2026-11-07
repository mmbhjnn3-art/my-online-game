
import os
import json
import secrets
import hashlib
import hmac
import time
import asyncio
import math
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]

CAPACITY = 20
ACTIVE_SECONDS = 45
SERVER_NAMES = {str(i): f"سرور {i}" for i in range(1, 6)}

app = FastAPI(title="Zendegi Life Game Server", version="1.3.0")
CITY_LIMIT = 4800.0
CREATOR_NAME = "سازنده بازی"
BUILDER_PASSWORD = os.getenv("BUILDER_PASSWORD", "")
BUILDER_SESSION_SECONDS = 12 * 60 * 60

# اتصالات زنده، جداشده بر اساس شماره سرور
ws_clients = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type", "X-Builder-Token"],
)


class Database:
    """Small compatibility wrapper for the existing ?-placeholder SQL."""

    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError(
                "متغیر DATABASE_URL تنظیم نشده است. آدرس Neon را در Environment رندر وارد کنید."
            )
        self.conn = psycopg.connect(
            DATABASE_URL,
            row_factory=dict_row,
            connect_timeout=15,
            sslmode="require",
        )

    def execute(self, query, params=()):
        return self.conn.execute(query.replace("?", "%s"), params)

    def __enter__(self):
        self.conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self.conn.__exit__(exc_type, exc, tb)


def connect():
    return Database()


def init_db():
    with connect() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id BIGSERIAL PRIMARY KEY,
                server_id TEXT NOT NULL,
                name TEXT NOT NULL,
                name_key TEXT NOT NULL,
                age INTEGER NOT NULL,
                password_salt TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                state_json TEXT NOT NULL,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                UNIQUE(server_id, name_key)
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                server_id TEXT NOT NULL,
                last_seen BIGINT NOT NULL
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sessions_server_seen
            ON sessions(server_id, last_seen)
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS server_wells (
                server_id TEXT PRIMARY KEY,
                stock INTEGER NOT NULL DEFAULT 100,
                last_regen_at BIGINT NOT NULL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS builder_sessions (
                token TEXT PRIMARY KEY,
                player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                server_id TEXT NOT NULL,
                expires_at BIGINT NOT NULL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS road_tiles (
                server_id TEXT NOT NULL,
                tile_x INTEGER NOT NULL,
                tile_y INTEGER NOT NULL,
                tile_type TEXT NOT NULL DEFAULT 'asphalt',
                orientation SMALLINT NOT NULL DEFAULT 0,
                updated_at BIGINT NOT NULL,
                updated_by BIGINT REFERENCES players(id) ON DELETE SET NULL,
                PRIMARY KEY(server_id, tile_x, tile_y)
            )
        """)
        db.execute("ALTER TABLE road_tiles ADD COLUMN IF NOT EXISTS orientation SMALLINT NOT NULL DEFAULT 0")
        db.execute("""
            CREATE TABLE IF NOT EXISTS builder_credentials (
                creator_name TEXT PRIMARY KEY,
                password_salt TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                created_at BIGINT NOT NULL
            )
        """)

        # حساب بازیکن در کل بازی یکتا است؛ server_id در sessions مشخص می‌کند
        # بازیکن اکنون در کدام دنیا حضور دارد.
        db.execute("""
            ALTER TABLE players
            DROP CONSTRAINT IF EXISTS players_server_id_name_key_key
        """)
        # نسخه‌های قدیمی ممکن است برای یک نام در چند سرور حساب ساخته باشند.
        # آخرین پیشرفت نگه داشته می‌شود تا نام در همه سرورها یک حساب باشد.
        db.execute("""
            DELETE FROM players p
            USING players newer
            WHERE p.name_key = newer.name_key
              AND (p.updated_at < newer.updated_at
                   OR (p.updated_at = newer.updated_at AND p.id < newer.id))
        """)
        db.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_players_global_name
            ON players(name_key)
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
        "cameraY": 0
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


def get_or_create_well(db, server_id, lock=True):
    """Return the shared well for the selected game server only."""
    now = int(time.time())
    db.execute(
        """INSERT INTO server_wells(server_id, stock, last_regen_at)
        VALUES(?, 100, ?) ON CONFLICT(server_id) DO NOTHING""",
        (server_id, now)
    )
    suffix = " FOR UPDATE" if lock else ""
    row = db.execute(
        "SELECT server_id, stock, last_regen_at FROM server_wells WHERE server_id=?" + suffix,
        (server_id,)
    ).fetchone()
    stock = max(0, min(100, int(row["stock"])))
    last_regen_at = int(row["last_regen_at"])
    if stock >= 100:
        if last_regen_at != now:
            last_regen_at = now
            db.execute(
                "UPDATE server_wells SET stock=100, last_regen_at=? WHERE server_id=?",
                (last_regen_at, server_id)
            )
    else:
        minutes = max(0, (now - last_regen_at) // 60)
        if minutes:
            stock = min(100, stock + minutes)
            last_regen_at += minutes * 60
            if stock >= 100:
                last_regen_at = now
            db.execute(
                "UPDATE server_wells SET stock=?, last_regen_at=? WHERE server_id=?",
                (stock, last_regen_at, server_id)
            )
    return {"server_id": server_id, "stock": stock, "last_regen_at": last_regen_at}


async def broadcast_well(server_id, well):
    await ws_send_group(server_id, {
        "type": "well",
        "stock": well["stock"],
        "last_regen_at": well["last_regen_at"]
    })


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


class BuilderUnlockIn(BaseModel):
    password: str = Field(min_length=1, max_length=128)


class RoadTileIn(BaseModel):
    x: int = Field(ge=-100, le=99)
    y: int = Field(ge=-100, le=99)
    tile_type: str = Field(default="asphalt", pattern="^asphalt$")
    orientation: int = Field(default=0, ge=0, le=1)


class RoadTilePositionIn(BaseModel):
    x: int = Field(ge=-100, le=99)
    y: int = Field(ge=-100, le=99)


def require_builder(db, authorization, builder_token):
    player = current_player(db, authorization)
    if norm_name(player["name"]) != norm_name(CREATOR_NAME):
        raise HTTPException(403, "فقط حساب سازنده بازی اجازه ساخت دارد.")
    if not builder_token:
        raise HTTPException(401, "ابتدا رمز ساخت را وارد کن.")
    now = int(time.time())
    row = db.execute(
        """SELECT token FROM builder_sessions
        WHERE token=? AND player_id=? AND server_id=? AND expires_at>=?""",
        (builder_token, player["id"], player["session_server"], now)
    ).fetchone()
    if not row:
        raise HTTPException(403, "دسترسی ساخت معتبر نیست؛ دوباره رمز را وارد کن.")
    return player


@app.get("/api/version")
def api_version():
    return {"app": "بازی زندگی", "version": "1.3.0", "city_size": "200x200", "global_accounts": True}


@app.get("/health")
def health():
    # Readiness check: verifies that the configured PostgreSQL database is reachable.
    with connect() as db:
        db.execute("SELECT 1").fetchone()
    return {"ok": True, "database": "connected"}


@app.get("/")
def root():
    page = Path(__file__).with_name("index.html")
    if page.exists():
        return FileResponse(page)
    return {"app": "بازی زندگی", "status": "online",
            "message": "فایل index.html کنار server.py قرار نگرفته است."}


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
            "SELECT 1 FROM players WHERE name_key=?",
            (key,)
        ).fetchone()

        if exists:
            raise HTTPException(409, "این نام قبلاً ثبت شده است.")

        if active_count(db, sid) >= CAPACITY:
            raise HTTPException(409, "این سرور پر است.")

        cur = db.execute(
            """INSERT INTO players
            (server_id,name,name_key,age,password_salt,password_hash,
            state_json,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?) RETURNING id""",
            (
                sid, name, key, data.age, salt.hex(), ph,
                json.dumps(state, ensure_ascii=False), now, now
            )
        )
        player_id = cur.fetchone()["id"]
        token = make_session(db, player_id, sid)

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
        # ابتدا جست‌وجوی سریع با کلید استاندارد؛ سپس سازگاری با حساب‌های
        # قدیمی که name_key آن‌ها با روش قبلی ذخیره شده است.
        rows = db.execute(
            "SELECT * FROM players WHERE name_key=? ORDER BY updated_at DESC, id DESC",
            (key,)
        ).fetchall()
        if not rows:
            legacy_rows = db.execute(
                "SELECT * FROM players ORDER BY updated_at DESC, id DESC"
            ).fetchall()
            rows = [r for r in legacy_rows if norm_name(r["name"]) == key]

        if not rows:
            raise HTTPException(401, "حساب پیدا نشد. اگر این حساب قبلاً در سرور دیگری ساخته شده، مطمئن شو Render نسخه جدید server.py را اجرا می‌کند.")

        # اگر نسخه قدیمی برای یک نام چند ردیف ساخته باشد، همان ردیفی را
        # انتخاب کن که رمز واردشده با آن تطبیق دارد.
        row = None
        for candidate in rows:
            expected = password_hash(
                data.password,
                bytes.fromhex(candidate["password_salt"])
            )
            if hmac.compare_digest(expected, candidate["password_hash"]):
                row = candidate
                break

        if row is None:
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

        state = dict(data.state)
        state["name"] = player["name"]
        state["age"] = player["age"]
        try:
            state["playerX"] = max(-CITY_LIMIT, min(CITY_LIMIT, float(state.get("playerX", 0))))
            state["playerY"] = max(-CITY_LIMIT, min(CITY_LIMIT, float(state.get("playerY", 0))))
        except (TypeError, ValueError):
            state["playerX"] = 0
            state["playerY"] = 0
        state.pop("passwordHash", None)
        # موجودی چاه مشترک است و نباید از اطلاعات محلی هر بازیکن بازنویسی شود.
        state.pop("wellStock", None)
        state.pop("wellLastRegenAt", None)

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


@app.get("/api/well")
def well_status(authorization: str | None = Header(default=None)):
    with connect() as db:
        player = current_player(db, authorization)
        well = get_or_create_well(db, player["session_server"])
    return well


@app.post("/api/well/buy")
async def buy_well_water(authorization: str | None = Header(default=None)):
    """Atomically buy one bottle from this server's shared well."""
    with connect() as db:
        player = current_player(db, authorization)
        server_id = player["session_server"]
        well = get_or_create_well(db, server_id)
        if well["stock"] < 1:
            raise HTTPException(409, "آب چاه تمام شده؛ برای اضافه شدن آب یک دقیقه صبر کن.")

        # Re-read and lock the buyer row after locking the well, so two quick
        # purchases by the same account cannot overwrite one another.
        locked_player = db.execute(
            "SELECT state_json FROM players WHERE id=? FOR UPDATE",
            (player["id"],)
        ).fetchone()
        state = json.loads(locked_player["state_json"])
        balance = int(state.get("balance", 0) or 0)
        if balance < 2000:
            raise HTTPException(400, "موجودی پول کافی نیست.")

        inventory = state.get("inventory")
        if not isinstance(inventory, dict):
            inventory = {}
        inventory["آب"] = int(inventory.get("آب", 0) or 0) + 1
        state["inventory"] = inventory
        state["balance"] = balance - 2000
        state["xp"] = int(state.get("xp", 0) or 0) + 2
        now = int(time.time())
        state["updatedAt"] = now * 1000

        db.execute(
            "UPDATE players SET state_json=?, updated_at=? WHERE id=?",
            (json.dumps(state, ensure_ascii=False), now, player["id"])
        )
        new_stock = well["stock"] - 1
        db.execute(
            "UPDATE server_wells SET stock=?, last_regen_at=? WHERE server_id=?",
            (new_stock, well["last_regen_at"], server_id)
        )
        well["stock"] = new_stock

    await broadcast_well(server_id, well)
    return {"ok": True, "state": state, "well": well}


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
            "server_id": player["session_server"],
            "server_name": SERVER_NAMES[player["session_server"]],
            "name": player["name"],
            "state": json.loads(player["state_json"])
}
  

@app.post("/api/builder/unlock")
def unlock_builder(data: BuilderUnlockIn, authorization: str | None = Header(default=None)):
    if len(data.password) < 4:
        raise HTTPException(400, "رمز باید حداقل ۴ نویسه داشته باشد.")
    with connect() as db:
        player = current_player(db, authorization)
        if norm_name(player["name"]) != norm_name(CREATOR_NAME):
            raise HTTPException(403, "فقط حساب سازنده بازی اجازه ساخت دارد.")
        saved = db.execute(
            "SELECT password_salt, password_hash FROM builder_credentials WHERE creator_name=?",
            (norm_name(CREATOR_NAME),)
        ).fetchone()
        first_setup = saved is None
        if first_setup:
            salt = secrets.token_bytes(16)
            digest = hashlib.pbkdf2_hmac("sha256", data.password.encode("utf-8"), salt, 310000)
            db.execute(
                "INSERT INTO builder_credentials(creator_name,password_salt,password_hash,created_at) VALUES(?,?,?,?)",
                (norm_name(CREATOR_NAME), salt.hex(), digest.hex(), int(time.time()))
            )
        else:
            salt = bytes.fromhex(saved["password_salt"])
            digest = hashlib.pbkdf2_hmac("sha256", data.password.encode("utf-8"), salt, 310000)
            if not hmac.compare_digest(digest.hex(), saved["password_hash"]):
                raise HTTPException(403, "رمز ساخت اشتباه است.")
        token = secrets.token_urlsafe(48)
        expires = int(time.time()) + BUILDER_SESSION_SECONDS
        db.execute(
            "INSERT INTO builder_sessions(token, player_id, server_id, expires_at) VALUES(?,?,?,?)",
            (token, player["id"], player["session_server"], expires)
        )
    return {"ok": True, "builder_token": token, "expires_at": expires, "first_setup": first_setup}


@app.get("/api/roads")
def get_roads(authorization: str | None = Header(default=None)):
    with connect() as db:
        player = current_player(db, authorization)
        rows = db.execute(
            """SELECT tile_x AS x, tile_y AS y, tile_type, orientation
            FROM road_tiles WHERE server_id=? ORDER BY tile_y, tile_x""",
            (player["session_server"],)
        ).fetchall()
    return {"tiles": rows}


@app.post("/api/roads")
async def place_road_tile(
    data: RoadTileIn,
    authorization: str | None = Header(default=None),
    x_builder_token: str | None = Header(default=None)
):
    with connect() as db:
        player = require_builder(db, authorization, x_builder_token)
        now = int(time.time())
        db.execute(
            """INSERT INTO road_tiles(server_id, tile_x, tile_y, tile_type, orientation, updated_at, updated_by)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(server_id, tile_x, tile_y)
            DO UPDATE SET tile_type=EXCLUDED.tile_type, orientation=EXCLUDED.orientation,
                          updated_at=EXCLUDED.updated_at, updated_by=EXCLUDED.updated_by""",
            (player["session_server"], data.x, data.y, data.tile_type, data.orientation, now, player["id"])
        )
    tile = {"x": data.x, "y": data.y, "tile_type": data.tile_type, "orientation": data.orientation}
    await ws_send_group(player["session_server"], {"type": "road_tile", "tile": tile})
    return {"ok": True, "tile": tile}


@app.post("/api/roads/rotate")
async def rotate_road_tile(
    data: RoadTilePositionIn,
    authorization: str | None = Header(default=None),
    x_builder_token: str | None = Header(default=None)
):
    with connect() as db:
        player = require_builder(db, authorization, x_builder_token)
        row = db.execute(
            "SELECT orientation FROM road_tiles WHERE server_id=? AND tile_x=? AND tile_y=?",
            (player["session_server"], data.x, data.y)
        ).fetchone()
        if row is None:
            raise HTTPException(404, "در این خانه آسفالت وجود ندارد.")
        orientation = 1 - int(row["orientation"] or 0)
        db.execute(
            "UPDATE road_tiles SET orientation=?, updated_at=?, updated_by=? WHERE server_id=? AND tile_x=? AND tile_y=?",
            (orientation, int(time.time()), player["id"], player["session_server"], data.x, data.y)
        )
    tile = {"x": data.x, "y": data.y, "tile_type": "asphalt", "orientation": orientation}
    await ws_send_group(player["session_server"], {"type": "road_tile", "tile": tile})
    return {"ok": True, "tile": tile}


@app.post("/api/roads/delete")
async def delete_road_tile(
    data: RoadTilePositionIn,
    authorization: str | None = Header(default=None),
    x_builder_token: str | None = Header(default=None)
):
    with connect() as db:
        player = require_builder(db, authorization, x_builder_token)
        result = db.execute(
            "DELETE FROM road_tiles WHERE server_id=? AND tile_x=? AND tile_y=?",
            (player["session_server"], data.x, data.y)
        )
        if result.rowcount == 0:
            raise HTTPException(404, "آسفالتی در این خانه پیدا نشد.")
    tile = {"x": data.x, "y": data.y}
    await ws_send_group(player["session_server"], {"type": "road_tile_deleted", "tile": tile})
    return {"ok": True, "tile": tile}


async def ws_send_group(server_id, message, exclude=None):
    """ارسال پیام فقط به بازیکنان همان سرور."""
    dead = []
    for client in list(ws_clients.get(server_id, set())):
        if client is exclude:
            continue
        try:
            await client.send_json(message)
        except Exception:
            dead.append(client)
    for client in dead:
        ws_clients.get(server_id, set()).discard(client)


@app.websocket("/ws")
async def realtime_socket(websocket: WebSocket):
    await websocket.accept()
    player = None
    server_id = None
    try:
        # توکن بعد از برقراری اتصال و از طریق پیام اول ارسال می‌شود.
        auth = await asyncio.wait_for(websocket.receive_json(), timeout=12)
        token = str(auth.get("token", "")) if isinstance(auth, dict) else ""
        if not token:
            await websocket.close(code=4401)
            return

        with connect() as db:
            player = db.execute(
                """SELECT p.id, p.name, p.server_id, p.state_json, s.token
                FROM sessions s JOIN players p ON p.id=s.player_id
                WHERE s.token=?""", (token,)
            ).fetchone()
            if not player:
                await websocket.close(code=4401)
                return
            server_id = player["server_id"]
            db.execute(
                "UPDATE sessions SET last_seen=? WHERE token=?",
                (int(time.time()), token)
            )
            cutoff = int(time.time()) - ACTIVE_SECONDS
            rows = db.execute(
                """SELECT p.name, p.state_json
                FROM sessions s JOIN players p ON p.id=s.player_id
                WHERE s.server_id=? AND s.last_seen>=?
                ORDER BY p.name""", (server_id, cutoff)
            ).fetchall()

        clients = ws_clients.setdefault(server_id, set())
        clients.add(websocket)

        snapshot = []
        for row in rows:
            state = json.loads(row["state_json"])
            snapshot.append({
                "name": row["name"],
                "playerX": state.get("playerX", 0),
                "playerY": state.get("playerY", 0)
            })
        await websocket.send_json({"type": "snapshot", "players": snapshot})
        with connect() as db:
            initial_well = get_or_create_well(db, server_id)
        await websocket.send_json({
            "type": "well",
            "stock": initial_well["stock"],
            "last_regen_at": initial_well["last_regen_at"]
        })

        # اعلام ورود بازیکن تازه به سایر افراد همان سرور
        initial_state = json.loads(player["state_json"])
        await ws_send_group(server_id, {
            "type": "upsert",
            "player": {
                "name": player["name"],
                "playerX": initial_state.get("playerX", 0),
                "playerY": initial_state.get("playerY", 0)
            }
        }, exclude=websocket)

        last_db_touch = time.monotonic()
        while True:
            message = await websocket.receive_json()
            if not isinstance(message, dict):
                continue

            if message.get("type") == "ping":
                if time.monotonic() - last_db_touch >= 15:
                    with connect() as db:
                        db.execute(
                            "UPDATE sessions SET last_seen=? WHERE token=?",
                            (int(time.time()), token)
                        )
                    last_db_touch = time.monotonic()
                await websocket.send_json({"type": "pong"})
                continue

            if message.get("type") != "position":
                continue

            try:
                x = float(message.get("playerX", 0))
                y = float(message.get("playerY", 0))
                if not (math.isfinite(x) and math.isfinite(y)):
                    continue
                # جلوگیری از مقادیر غیرعادی یا خراب
                x = max(-CITY_LIMIT, min(CITY_LIMIT, x))
                y = max(-CITY_LIMIT, min(CITY_LIMIT, y))
            except (TypeError, ValueError):
                continue

            if time.monotonic() - last_db_touch >= 15:
                with connect() as db:
                    db.execute(
                        "UPDATE sessions SET last_seen=? WHERE token=?",
                        (int(time.time()), token)
                    )
                last_db_touch = time.monotonic()

            await ws_send_group(server_id, {
                "type": "position",
                "player": {
                    "name": player["name"],
                    "playerX": x,
                    "playerY": y
                }
            }, exclude=websocket)

    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    except Exception:
        # خطای یک اتصال نباید سرور را متوقف کند.
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
    finally:
        if server_id and websocket in ws_clients.get(server_id, set()):
            ws_clients[server_id].discard(websocket)
            if player:
                await ws_send_group(server_id, {
                    "type": "leave", "name": player["name"]
                }, exclude=websocket)
