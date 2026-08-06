from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
)
from aiogram.enums import ContentType
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    bot_token: str
    database_path: Path
    image_dir: Path
    payment_url: str
    payment_provider_token: str
    course_price_kopecks: int
    yookassa_tax_system_code: int
    yookassa_vat_code: int
    course_url: str
    admin_ids: frozenset[int]
    reminder_delays: tuple[timedelta, ...]
    payment_webhook_secret: str
    payment_webhook_host: str
    payment_webhook_port: int
    telegram_proxy: str

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.getenv("BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("BOT_TOKEN не задан. Скопируйте .env.example в .env.")

        admin_ids = frozenset(
            int(value.strip())
            for value in os.getenv("ADMIN_IDS", "").split(",")
            if value.strip()
        )
        delay_values = tuple(
            float(value.strip())
            for value in os.getenv("REMINDER_DELAYS_HOURS", "3,24,48").split(",")
            if value.strip()
        )
        if len(delay_values) != 3:
            raise RuntimeError("REMINDER_DELAYS_HOURS должен содержать ровно 3 числа.")

        database_path = Path(os.getenv("DATABASE_PATH", "data/bot.sqlite3"))
        image_dir = Path(os.getenv("IMAGE_DIR", "assets"))
        if not database_path.is_absolute():
            database_path = BASE_DIR / database_path
        if not image_dir.is_absolute():
            image_dir = BASE_DIR / image_dir

        return cls(
            bot_token=token,
            database_path=database_path,
            image_dir=image_dir,
            payment_url=os.getenv("PAYMENT_URL", "").strip(),
            payment_provider_token=os.getenv("PAYMENT_PROVIDER_TOKEN", "").strip(),
            course_price_kopecks=int(
                os.getenv("COURSE_PRICE_KOPECKS", "199000")
            ),
            yookassa_tax_system_code=int(
                os.getenv("YOOKASSA_TAX_SYSTEM_CODE", "0") or "0"
            ),
            yookassa_vat_code=int(os.getenv("YOOKASSA_VAT_CODE", "1") or "1"),
            course_url=os.getenv("COURSE_URL", "https://t.me/example_course").strip(),
            admin_ids=admin_ids,
            reminder_delays=tuple(timedelta(hours=value) for value in delay_values),
            payment_webhook_secret=os.getenv("PAYMENT_WEBHOOK_SECRET", "").strip(),
            payment_webhook_host=os.getenv("PAYMENT_WEBHOOK_HOST", "127.0.0.1").strip(),
            payment_webhook_port=int(os.getenv("PAYMENT_WEBHOOK_PORT", "8080")),
            telegram_proxy=normalize_telegram_proxy(
                os.getenv("TELEGRAM_PROXY", "").strip()
            ),
        )


def normalize_telegram_proxy(raw: str) -> str:
    """socks5://user:pass@host:port или host:port:user:pass."""
    value = raw.strip()
    if not value:
        return ""
    if "://" in value:
        return value
    parts = value.split(":")
    if len(parts) == 4:
        host, port, user, password = parts
        return (
            f"socks5://{quote(user, safe='')}:{quote(password, safe='')}"
            f"@{host}:{port}"
        )
    return value


def mask_proxy_url(proxy_url: str) -> str:
    if "@" not in proxy_url:
        return proxy_url
    scheme, rest = proxy_url.split("://", maxsplit=1)
    credentials, host = rest.rsplit("@", maxsplit=1)
    user = credentials.split(":", maxsplit=1)[0]
    return f"{scheme}://{user}:***@{host}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_db_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[aiosqlite.Connection]:
        connection = await aiosqlite.connect(self.path)
        connection.row_factory = aiosqlite.Row
        await connection.execute("PRAGMA journal_mode=WAL")
        await connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
        finally:
            await connection.close()

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with self.connect() as connection:
            await connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    segment TEXT,
                    started_at TEXT NOT NULL,
                    payment_started_at TEXT,
                    reminders_sent INTEGER NOT NULL DEFAULT 0,
                    reminder_claimed INTEGER,
                    purchased INTEGER NOT NULL DEFAULT 0,
                    access_sent INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    event TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS questions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    answered INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
                );

                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    currency TEXT NOT NULL,
                    total_amount INTEGER NOT NULL,
                    invoice_payload TEXT NOT NULL,
                    telegram_payment_charge_id TEXT NOT NULL,
                    provider_payment_charge_id TEXT,
                    paid_at TEXT NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id),
                    UNIQUE(telegram_payment_charge_id)
                );
                """
            )
            cursor = await connection.execute("PRAGMA table_info(users)")
            user_columns = {row["name"] for row in await cursor.fetchall()}
            if "reminder_claimed" not in user_columns:
                await connection.execute(
                    "ALTER TABLE users ADD COLUMN reminder_claimed INTEGER"
                )
            if "access_sent" not in user_columns:
                await connection.execute(
                    """
                    ALTER TABLE users
                    ADD COLUMN access_sent INTEGER NOT NULL DEFAULT 0
                    """
                )
            await connection.execute(
                "UPDATE users SET reminder_claimed = NULL"
            )
            await connection.execute(
                "UPDATE questions SET answered = 0 WHERE answered = 2"
            )
            await connection.commit()

    async def start_user(self, message: Message) -> None:
        user = message.from_user
        if user is None:
            return
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            await connection.execute(
                """
                INSERT INTO users (
                    telegram_id, username, full_name, status, started_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username = excluded.username,
                    full_name = excluded.full_name,
                    status = CASE
                        WHEN users.purchased = 1 THEN users.status
                        ELSE excluded.status
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    user.id,
                    user.username,
                    user.full_name,
                    "Запустил бота",
                    now,
                    now,
                ),
            )
            await connection.execute(
                "INSERT INTO events (telegram_id, event, created_at) VALUES (?, ?, ?)",
                (user.id, "Запустил бота", now),
            )
            await connection.commit()

    async def set_status(self, telegram_id: int, status: str) -> None:
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            await connection.execute(
                "UPDATE users SET status = ?, updated_at = ? WHERE telegram_id = ?",
                (status, now, telegram_id),
            )
            await connection.execute(
                "INSERT INTO events (telegram_id, event, created_at) VALUES (?, ?, ?)",
                (telegram_id, status, now),
            )
            await connection.commit()

    async def set_segment(self, telegram_id: int, segment: str) -> None:
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            await connection.execute(
                """
                UPDATE users
                SET segment = ?, status = ?, updated_at = ?
                WHERE telegram_id = ?
                """,
                (segment, "Узнал себя", now, telegram_id),
            )
            await connection.executemany(
                "INSERT INTO events (telegram_id, event, created_at) VALUES (?, ?, ?)",
                [
                    (telegram_id, "Узнал себя", now),
                    (telegram_id, segment, now),
                ],
            )
            await connection.commit()

    async def start_payment(self, telegram_id: int) -> None:
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                UPDATE users
                SET status = ?,
                    payment_started_at = COALESCE(payment_started_at, ?),
                    reminders_sent = CASE
                        WHEN payment_started_at IS NULL THEN 0
                        ELSE reminders_sent
                    END,
                    updated_at = ?
                WHERE telegram_id = ? AND purchased = 0
                """,
                ("Перешёл к оплате", now, now, telegram_id),
            )
            if cursor.rowcount:
                await connection.execute(
                    "INSERT INTO events (telegram_id, event, created_at) VALUES (?, ?, ?)",
                    (telegram_id, "Перешёл к оплате", now),
                )
            await connection.commit()

    async def mark_paid(self, telegram_id: int) -> str:
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                UPDATE users
                SET status = ?, purchased = 1, payment_started_at = NULL,
                    access_sent = 0, updated_at = ?
                WHERE telegram_id = ? AND purchased = 0
                """,
                ("Получил доступ", now, telegram_id),
            )
            if not cursor.rowcount:
                cursor = await connection.execute(
                    "SELECT 1 FROM users WHERE telegram_id = ?",
                    (telegram_id,),
                )
                return (
                    "already_paid"
                    if await cursor.fetchone()
                    else "not_found"
                )
            await connection.executemany(
                "INSERT INTO events (telegram_id, event, created_at) VALUES (?, ?, ?)",
                [
                    (telegram_id, "Купил курс", now),
                    (telegram_id, "Получил доступ", now),
                ],
            )
            await connection.commit()
            return "paid"

    async def record_payment(
        self,
        telegram_id: int,
        currency: str,
        total_amount: int,
        invoice_payload: str,
        telegram_payment_charge_id: str,
        provider_payment_charge_id: str | None,
    ) -> bool:
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                INSERT OR IGNORE INTO payments(
                    telegram_id, currency, total_amount, invoice_payload,
                    telegram_payment_charge_id, provider_payment_charge_id, paid_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    currency.strip().upper(),
                    int(total_amount),
                    invoice_payload[:512],
                    telegram_payment_charge_id[:256],
                    (provider_payment_charge_id or "")[:256] or None,
                    now,
                ),
            )
            await connection.commit()
        return cursor.rowcount > 0

    async def needs_access_delivery(self, telegram_id: int) -> bool:
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                SELECT 1
                FROM users
                WHERE telegram_id = ? AND purchased = 1 AND access_sent = 0
                """,
                (telegram_id,),
            )
            return await cursor.fetchone() is not None

    async def mark_access_sent(self, telegram_id: int) -> None:
        async with self.connect() as connection:
            await connection.execute(
                """
                UPDATE users
                SET access_sent = 1, updated_at = ?
                WHERE telegram_id = ? AND purchased = 1
                """,
                (to_db_time(utc_now()), telegram_id),
            )
            await connection.commit()

    async def save_question(self, telegram_id: int, text: str) -> None:
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            await connection.execute(
                "INSERT INTO questions (telegram_id, text, created_at) VALUES (?, ?, ?)",
                (telegram_id, text, now),
            )
            await connection.execute(
                "UPDATE users SET status = ?, updated_at = ? WHERE telegram_id = ?",
                ("Вопрос отправлен", now, telegram_id),
            )
            await connection.commit()

    async def unanswered_questions(self, limit: int = 10) -> list[aiosqlite.Row]:
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                SELECT id, telegram_id, text, created_at
                FROM questions
                WHERE answered = 0
                ORDER BY id
                LIMIT ?
                """,
                (limit,),
            )
            return await cursor.fetchall()

    async def claim_question(self, question_id: int) -> int | None:
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                UPDATE questions
                SET answered = 2
                WHERE id = ? AND answered = 0
                """,
                (question_id,),
            )
            if not cursor.rowcount:
                return None
            cursor = await connection.execute(
                """
                SELECT telegram_id
                FROM questions
                WHERE id = ? AND answered = 2
                """,
                (question_id,),
            )
            row = await cursor.fetchone()
            await connection.commit()
            return int(row["telegram_id"]) if row else None

    async def mark_question_answered(self, question_id: int) -> None:
        async with self.connect() as connection:
            await connection.execute(
                "UPDATE questions SET answered = 1 WHERE id = ?",
                (question_id,),
            )
            await connection.commit()

    async def release_question(self, question_id: int) -> None:
        async with self.connect() as connection:
            await connection.execute(
                "UPDATE questions SET answered = 0 WHERE id = ? AND answered = 2",
                (question_id,),
            )
            await connection.commit()

    async def get_status(self, telegram_id: int) -> str | None:
        async with self.connect() as connection:
            cursor = await connection.execute(
                "SELECT status FROM users WHERE telegram_id = ?",
                (telegram_id,),
            )
            row = await cursor.fetchone()
            return str(row["status"]) if row else None

    async def is_purchased(self, telegram_id: int) -> bool:
        async with self.connect() as connection:
            cursor = await connection.execute(
                "SELECT purchased FROM users WHERE telegram_id = ?",
                (telegram_id,),
            )
            row = await cursor.fetchone()
            return bool(row and row["purchased"])

    async def admin_metrics(
        self,
        funnel_events: tuple[str, ...],
        segments: tuple[str, ...],
        excluded_ids: tuple[int, ...] = (),
    ) -> dict[str, object]:
        excluded_sql = (
            f" AND telegram_id NOT IN ({','.join('?' for _ in excluded_ids)})"
            if excluded_ids
            else ""
        )
        async with self.connect() as connection:
            cursor = await connection.execute(
                "SELECT COUNT(*) AS count FROM users WHERE 1 = 1" + excluded_sql,
                excluded_ids,
            )
            total_users = int((await cursor.fetchone())["count"])
            cursor = await connection.execute(
                """
                SELECT event, COUNT(DISTINCT telegram_id) AS count
                FROM events
                WHERE event IN ({})
                {}
                GROUP BY event
                """.format(
                    ",".join("?" for _ in funnel_events),
                    excluded_sql,
                ),
                funnel_events + excluded_ids,
            )
            funnel = {
                str(row["event"]): int(row["count"])
                for row in await cursor.fetchall()
            }
            cursor = await connection.execute(
                """
                SELECT segment, COUNT(*) AS count
                FROM users
                WHERE segment IN ({})
                {}
                GROUP BY segment
                """.format(
                    ",".join("?" for _ in segments),
                    excluded_sql,
                ),
                segments + excluded_ids,
            )
            segment_counts = {
                str(row["segment"]): int(row["count"])
                for row in await cursor.fetchall()
            }
            return {
                "total_users": total_users,
                "funnel": funnel,
                "segments": segment_counts,
            }

    async def admin_users(
        self,
        page: int,
        page_size: int = 6,
        excluded_ids: tuple[int, ...] = (),
    ) -> tuple[list[aiosqlite.Row], int]:
        excluded_sql = (
            f" WHERE telegram_id NOT IN ({','.join('?' for _ in excluded_ids)})"
            if excluded_ids
            else ""
        )
        async with self.connect() as connection:
            cursor = await connection.execute(
                "SELECT COUNT(*) AS count FROM users" + excluded_sql,
                excluded_ids,
            )
            total = int((await cursor.fetchone())["count"])
            cursor = await connection.execute(
                """
                SELECT telegram_id, username, full_name, status, segment,
                       purchased, updated_at
                FROM users
                {}
                ORDER BY updated_at DESC, telegram_id DESC
                LIMIT ? OFFSET ?
                """.format(excluded_sql),
                excluded_ids + (page_size, page * page_size),
            )
            return await cursor.fetchall(), total

    async def admin_user(self, telegram_id: int) -> dict[str, object] | None:
        async with self.connect() as connection:
            cursor = await connection.execute(
                "SELECT * FROM users WHERE telegram_id = ?",
                (telegram_id,),
            )
            user = await cursor.fetchone()
            if user is None:
                return None
            cursor = await connection.execute(
                """
                SELECT event, created_at
                FROM events
                WHERE telegram_id = ?
                ORDER BY id DESC
                LIMIT 10
                """,
                (telegram_id,),
            )
            events = await cursor.fetchall()
            return {
                "user": user,
                "events": events,
            }

    async def payment_candidates(self) -> list[aiosqlite.Row]:
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                SELECT telegram_id, payment_started_at, reminders_sent
                FROM users
                WHERE purchased = 0 AND payment_started_at IS NOT NULL
                  AND reminders_sent < 3
                """
            )
            return await cursor.fetchall()

    async def claim_reminder(self, telegram_id: int, count: int) -> bool:
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                UPDATE users
                SET reminder_claimed = ?
                WHERE telegram_id = ? AND purchased = 0
                  AND reminders_sent = ?
                  AND reminder_claimed IS NULL
                """,
                (count, telegram_id, count - 1),
            )
            await connection.commit()
            return bool(cursor.rowcount)

    async def complete_reminder(self, telegram_id: int, count: int) -> None:
        now = to_db_time(utc_now())
        async with self.connect() as connection:
            cursor = await connection.execute(
                """
                UPDATE users
                SET reminders_sent = ?, reminder_claimed = NULL,
                    status = ?, updated_at = ?
                WHERE telegram_id = ? AND purchased = 0
                  AND reminder_claimed = ?
                """,
                (count, "Оплата не завершена", now, telegram_id, count),
            )
            if cursor.rowcount:
                await connection.executemany(
                    "INSERT INTO events (telegram_id, event, created_at) VALUES (?, ?, ?)",
                    [
                        (telegram_id, "Оплата не завершена", now),
                        (telegram_id, f"Напоминание {count}", now),
                    ],
                )
            await connection.commit()

    async def release_reminder(self, telegram_id: int, count: int) -> None:
        async with self.connect() as connection:
            await connection.execute(
                """
                UPDATE users
                SET reminder_claimed = NULL
                WHERE telegram_id = ? AND reminder_claimed = ?
                """,
                (telegram_id, count),
            )
            await connection.commit()


STEPS: dict[int, tuple[str, str, str, str]] = {
    1: (
        "Здравствуйте, меня зовут Юля.\n\n"
        "Я — коуч. Не психолог, а именно коуч, который помогает женщинам "
        "лучше понять себя, отношения и свои сценарии.\n\n"
        "Здесь я мягко и честно помогу тебе посмотреть на свою жизнь глубже.",
        "Я не буду учить тебя, как стать удобнее для мужчины.\n\n"
        "Наша задача — понять, почему в отношениях повторяются одни и те же "
        "ситуации и что можно изменить в собственном выборе.",
        "Хочу разобраться",
        "step:2",
    ),
    2: (
        "Почему красивые и стильные женщины часто остаются без семьи?\n\n"
        "Часто причина не во внешности. И не в том, что с тобой что-то не так.\n\n"
        "Просто за красотой нередко стоят сильные сценарии:\nбыть удобной, всё "
        "тянуть на себе, выбирать недоступных и снова разочаровываться.\n\n"
        "Именно это мы и разберём дальше.",
        "Внимание мужчин ещё не гарантирует отношений.\n\n"
        "Можно нравиться, ходить на свидания и получать комплименты, но каждый "
        "раз оказываться в одной и той же неопределённости.",
        "Почему так происходит?",
        "step:3",
    ),
    4: (
        "Красота привлекает. Но семья строится на другом.\n\n"
        "Можно быть яркой, стильной и получать много внимания.\n\n"
        "Но внимание — это ещё не намерение. Сильные эмоции — ещё не любовь. "
        "Красивые слова — ещё не поступки.\n\n"
        "Иногда женщина выбирает не мужчину, а надежду на то, каким он однажды "
        "станет.\nИ снова оказывается в отношениях, у которых изначально не было "
        "будущего.\n\nВажно научиться видеть это раньше — до сильной привязанности.",
        "Один из самых важных навыков — научиться смотреть не на обещания и "
        "потенциал человека, а на его реальные действия.\n\n"
        "Потому что отношения строятся не на том, каким мужчина может стать. "
        "А на том, какой он рядом с тобой уже сейчас.",
        "Хочу научиться видеть раньше",
        "step:5",
    ),
    5: (
        "Тебе не нужно становиться удобнее, чтобы создать семью.\n\n"
        "Важно не стать ещё красивее, мягче или терпеливее.\n\n"
        "Важно научиться:\n"
        "— видеть намерения мужчины; — отличать любовь от иллюзии; "
        "— не соглашаться на неопределённость; "
        "— выбирать того, кто действительно готов к семье.\n\n"
        "Когда меняется твой выбор, начинает меняться и сценарий отношений.\n\n"
        "Именно об этом мы будем говорить дальше.",
        "Изменения начинаются не с попытки переделать мужчину.\n\n"
        "Они начинаются с понимания себя: кого ты выбираешь, почему остаёшься "
        "и на что соглашаешься в отношениях.",
        "Показать решение",
        "step:6",
    ),
    6: (
        "Я создала курс, который поможет тебе увидеть свой сценарий отношений.\n\n"
        "Курс «Почему красивые и стильные женщины часто остаются без семьи?»\n\n"
        "Внутри ты разберёшь:\n— почему внимание мужчин не приводит к семье;\n"
        "— каких мужчин ты выбираешь снова и снова;\n"
        "— как отличать серьёзные намерения от красивых слов;\n"
        "— почему ты остаёшься в неопределённости;\n"
        "— что изменить, чтобы строить отношения по-другому.\n\n"
        "Короткие видеоуроки. Без сложной психологии и лишней воды.\n"
        "Чтобы ты не просто всё поняла, а начала делать другой выбор.",
        "Это не курс о манипуляциях и не инструкция, как понравиться мужчине.\n\n"
        "Это возможность честно увидеть свой сценарий и понять, почему прежний "
        "выбор снова приводит к похожему результату.",
        "Посмотреть, почему мне можно доверять",
        "step:7",
    ),
    7: (
        "Почему я могу об этом говорить.\n\n"
        "Я не психолог. Я коуч и практик.\n\n"
        "Прошла обучение в Erickson International по программе «Искусство и "
        "наука коучинга», модули I–IV.\n\n"
        "15 лет в браке. Двое детей. Общий бизнес с мужем.\n\n"
        "Я знаю отношения не только из книг, но и из собственной жизни.\n\n"
        "Моя задача — не учить тебя жить, а помочь увидеть то, что ты сама пока "
        "не замечаешь.",
        "В основе курса — моя коучинговая подготовка, личный опыт и вопросы, "
        "которые помогают не просто слушать, а действительно смотреть на себя честнее.",
        "Посмотреть стоимость",
        "step:8",
    ),
    8: (
        "Всё, что нужно, чтобы начать менять свой сценарий отношений.\n\n"
        "Внутри курса:\n— короткие видеоуроки;\n"
        "— понятные разборы без лишней воды;\n"
        "— вопросы для самостоятельной работы;\n"
        "— доступ сразу после оплаты;\n— прохождение в своём темпе.\n\n"
        "Стоимость — 1 990 ₽.\n\nТы получаешь не просто информацию.\n"
        "Ты начинаешь лучше понимать, кого выбираешь, почему остаёшься в "
        "неопределённости и что можешь изменить уже сейчас.",
        "Доступ к материалам открывается сразу после оплаты.\n\n"
        "Все уроки можно проходить в своём темпе и возвращаться к ним повторно.",
        "",
        "",
    ),
    9: (
        "Можно продолжать ждать, что в следующий раз всё будет иначе.\n\n"
        "А можно уже сейчас понять:\n— почему ты выбираешь именно таких мужчин;\n"
        "— почему остаёшься там, где нет определённости;\n"
        "— почему снова надеешься вместо того, чтобы видеть поступки;\n"
        "— что нужно изменить, чтобы начать выбирать по-другому.\n\n"
        "Иногда один честный взгляд на себя меняет гораздо больше, чем годы "
        "ожидания.\n\nТы готова перестать повторять прежний сценарий?\n\n"
        "Оплатить и начать.",
        "После оплаты ты сразу получишь доступ ко всем урокам курса.",
        "Оплатить 1 990 ₽",
        "payment:start",
    ),
    10: (
        "Оплата прошла успешно 🤍 Ты внутри.\n\n"
        "Начинай с первого урока и проходи всё по порядку.\n\n"
        "Не спеши. Возвращайся к вопросам. Отвечай на них честно — в первую "
        "очередь перед собой.\n\n"
        "Задача курса — не просто дать тебе новые мысли, а помочь увидеть свой "
        "сценарий и начать делать другой выбор.\n\nПерейти к урокам.",
        "Доступ открыт. Начни с первого урока и проходи программу последовательно.",
        "Перейти к урокам",
        "course:open",
    ),
}

STEP_3_CARD = (
    "Возможно, ты узнаешь себя.\n"
    "Мужчины проявляют интерес, но не делают серьёзных шагов.\n"
    "Отношения начинаются ярко, а потом остаётся неопределённость.\n"
    "Ты долго ждёшь, что мужчина изменится.\n"
    "Снаружи ты сильная, а внутри устала всё тянуть на себе.\n\n"
    "Если откликнулся хотя бы один пункт — дело не в невезении.\n"
    "Чаще всего это повторяющийся сценарий, который можно увидеть и изменить."
)

SEGMENTS = {
    "no_steps": "Нет серьёзных шагов",
    "uncertainty": "Неопределённость",
    "unavailable": "Недоступные мужчины",
    "all_alone": "Всё тянет сама",
}

FUNNEL_EVENTS = (
    "Запустил бота",
    "Прошёл знакомство",
    "Узнал себя",
    "Посмотрел презентацию курса",
    "Увидел цену",
    "Перешёл к оплате",
    "Оплата не завершена",
    "Купил курс",
    "Получил доступ",
    "Начал уроки",
    "Прошёл курс",
)

SEGMENT_EVENTS = tuple(SEGMENTS.values())
ADMIN_PAGE_SIZE = 6

STATUS_BY_STEP = {
    2: "Прошёл знакомство",
    7: "Посмотрел презентацию курса",
    9: "Увидел цену",
}

REMINDERS = (
    (
        "Ты уже увидела, что внимание мужчины и готовность создать семью — не "
        "одно и то же.\n\nНо знать об этом недостаточно.\n\n"
        "Важно понять, почему именно ты снова оказываешься в похожем сценарии "
        "и в какой момент перестаёшь видеть реальность.\n\n"
        "Доступ к курсу остаётся открытым.",
        "Вернуться к курсу",
    ),
    (
        "Один честный вопрос:\nТы действительно выбираешь мужчину — или ждёшь, "
        "когда наконец выберут тебя?\n\nМежду этими позициями огромная разница.\n\n"
        "В курсе мы разберём, как выйти из ожидания и начать принимать решения "
        "из уважения к себе.",
        "Получить доступ за 1 990 ₽",
    ),
    (
        "Можно встретить другого мужчину, но снова построить те же отношения.\n\n"
        "Потому что меняются люди, а внутренний сценарий остаётся прежним.\n"
        "Пока ты его не увидишь.\n\nКурс поможет понять, что именно повторяется "
        "в твоём выборе и что можно изменить.",
        "Оплатить и начать",
    ),
)


INVOICE_PAYLOAD_COURSE = "course_access_v1"
TELEGRAM_INVOICE_TITLE_LIMIT = 32
INVOICE_TITLE = "Доступ к курсу"
INVOICE_DESCRIPTION = (
    "Курс «Почему красивые и стильные женщины часто остаются без семьи?»\n"
    "Доступ ко всем урокам сразу после оплаты."
)
INVOICE_PRICE_LABEL = "Доступ к курсу"
RECEIPT_ITEM_DESCRIPTION = "Доступ к онлайн-курсу"


def format_price_rub(kopecks: int) -> str:
    if kopecks % 100 == 0:
        rubles = kopecks // 100
        if rubles >= 1000:
            return f"{rubles:,}".replace(",", " ") + " ₽"
        return f"{rubles} ₽"
    return f"{kopecks / 100:.2f}".replace(".", ",") + " ₽"


def keyboard(text: str, callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data=callback_data)]
        ]
    )


def build_yookassa_provider_data(
    settings: Settings, amount_kopecks: int, description: str
) -> str | None:
    if not settings.yookassa_tax_system_code:
        return None
    if amount_kopecks % 100 == 0:
        value_rub: int | float = amount_kopecks // 100
    else:
        value_rub = round(amount_kopecks / 100.0, 2)
    return json.dumps(
        {
            "receipt": {
                "tax_system_code": settings.yookassa_tax_system_code,
                "items": [
                    {
                        "description": description[:128],
                        "quantity": 1,
                        "amount": {"value": value_rub, "currency": "RUB"},
                        "vat_code": settings.yookassa_vat_code or 1,
                        "payment_mode": "full_payment",
                        "payment_subject": "service",
                    }
                ],
            }
        },
        ensure_ascii=False,
    )


async def send_course_invoice(
    bot: Bot, chat_id: int, settings: Settings
) -> None:
    if not settings.payment_provider_token:
        raise RuntimeError("PAYMENT_PROVIDER_TOKEN is not configured")
    amount = settings.course_price_kopecks
    await bot.send_invoice(
        chat_id=chat_id,
        title=truncate_text(INVOICE_TITLE, TELEGRAM_INVOICE_TITLE_LIMIT),
        description=truncate_text(INVOICE_DESCRIPTION, 255),
        payload=INVOICE_PAYLOAD_COURSE,
        provider_token=settings.payment_provider_token,
        currency="RUB",
        prices=[LabeledPrice(label=INVOICE_PRICE_LABEL, amount=amount)],
        need_email=True,
        send_email_to_provider=True,
        need_phone_number=True,
        send_phone_number_to_provider=True,
        provider_data=build_yookassa_provider_data(
            settings, amount, RECEIPT_ITEM_DESCRIPTION
        ),
    )


def payment_url_for_user(base_url: str, telegram_id: int) -> str:
    parts = urlsplit(base_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["telegram_id"] = str(telegram_id)
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
    )


IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")
TELEGRAM_CAPTION_LIMIT = 1024

def find_step_image(settings: Settings, step: int) -> Path | None:
    image_dir = settings.image_dir
    if not image_dir.is_dir():
        return None

    stem_candidates = (
        f"step_{step}",
        f"IMG_{step}",
        f"img_{step}",
        str(step),
    )
    for stem in stem_candidates:
        for path in image_dir.iterdir():
            if not path.is_file():
                continue
            if path.suffix.casefold() not in IMAGE_EXTENSIONS:
                continue
            if path.stem.casefold() == stem.casefold():
                return path

    for path in image_dir.iterdir():
        if not path.is_file():
            continue
        if path.suffix.casefold() not in IMAGE_EXTENSIONS:
            continue
        numbers = re.findall(r"\d+", path.stem)
        if numbers and int(numbers[-1]) == step:
            return path

    return None


async def send_step_card(
    bot: Bot,
    chat_id: int,
    settings: Settings,
    step: int,
    card: str,
) -> None:
    image_path = find_step_image(settings, step)
    if image_path is None:
        logging.warning(
            "Фото для шага %s не найдено в %s", step, settings.image_dir
        )
        await bot.send_message(chat_id, card)
        return
    await bot.send_photo(
        chat_id,
        FSInputFile(image_path),
        caption=truncate_text(card, TELEGRAM_CAPTION_LIMIT),
    )
    logging.info("Отправлено фото шага %s: %s", step, image_path.name)


def build_step_markup(
    step: int, settings: Settings, chat_id: int, button_text: str, callback_data: str
) -> InlineKeyboardMarkup:
    if step == 8:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"Получить доступ за {format_price_rub(settings.course_price_kopecks)}",
                        callback_data="step:9",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="У меня остался вопрос", callback_data="question:start"
                    )
                ],
            ]
        )
    if step == 9:
        return keyboard(
            f"Оплатить {format_price_rub(settings.course_price_kopecks)}",
            "payment:start",
        )
    if step == 10:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=button_text,
                        url=settings.course_url,
                    )
                ]
            ]
        )
    return keyboard(button_text, callback_data)


async def send_step(bot: Bot, chat_id: int, settings: Settings, step: int) -> None:
    if step == 3:
        answers = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Мужчины не делают серьёзных шагов",
                        callback_data="segment:no_steps",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="Долго остаюсь в неопределённости",
                        callback_data="segment:uncertainty",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="Выбираю недоступных мужчин",
                        callback_data="segment:unavailable",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="Устала всё тянуть сама",
                        callback_data="segment:all_alone",
                    )
                ],
            ]
        )
        await send_step_card(bot, chat_id, settings, step, STEP_3_CARD)
        await bot.send_message(chat_id, "Что тебе ближе всего?", reply_markup=answers)
        return

    card, after, button_text, callback_data = STEPS[step]
    if step == 10:
        after = f"{after}\n\nСсылка на канал:\n{settings.course_url}"
    markup = build_step_markup(step, settings, chat_id, button_text, callback_data)
    await send_step_card(bot, chat_id, settings, step, card)
    await bot.send_message(chat_id, after, reply_markup=markup)


async def grant_paid_access(
    bot: Bot, settings: Settings, database: Database, telegram_id: int
) -> str:
    result = await database.mark_paid(telegram_id)
    if result != "not_found" and await database.needs_access_delivery(telegram_id):
        await send_step(bot, telegram_id, settings, 10)
        await database.mark_access_sent(telegram_id)
    return result


def start_keyboard(is_admin: bool) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="Начать", callback_data="step:1")]]
    if is_admin:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⚙️ Админ-панель",
                    callback_data="admin:open",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_admin_summary(metrics: dict[str, object]) -> str:
    return (
        "⚙️ Админ-панель\n\n"
        f"Пользователей: {metrics['total_users']}"
    )


def format_funnel_metrics(metrics: dict[str, object]) -> str:
    funnel = metrics["funnel"]
    segments = metrics["segments"]
    funnel_lines = [
        f"{event}: {funnel.get(event, 0)}" for event in FUNNEL_EVENTS
    ]
    segment_lines = [
        f"{segment}: {segments.get(segment, 0)}" for segment in SEGMENT_EVENTS
    ]
    return (
        "📊 Метрики воронки\n\n"
        + "\n".join(funnel_lines)
        + "\n\nСегменты ответов:\n"
        + "\n".join(segment_lines)
    )


def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📊 Метрики воронки",
                    callback_data="admin:metrics",
                )
            ],
            [
                InlineKeyboardButton(
                    text="👥 Пользователи",
                    callback_data="admin:users:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✖️ Закрыть",
                    callback_data="admin:close",
                )
            ],
        ]
    )


def admin_back_keyboard(target: str = "admin:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=target)]
        ]
    )


def compact_datetime(value: object) -> str:
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed.astimezone().strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return str(value)


def compact_name(value: object, limit: int = 35) -> str:
    name = " ".join(str(value).split())
    return name if len(name) <= limit else f"{name[: limit - 1]}…"


def truncate_text(value: object, limit: int) -> str:
    text = str(value)
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def create_router(settings: Settings, database: Database) -> Router:
    router = Router()

    def is_admin(user_id: int) -> bool:
        return user_id in settings.admin_ids

    async def admin_summary_text() -> str:
        metrics = await database.admin_metrics(
            FUNNEL_EVENTS,
            SEGMENT_EVENTS,
            tuple(settings.admin_ids),
        )
        return format_admin_summary(metrics)

    async def ensure_admin(callback: CallbackQuery) -> bool:
        if is_admin(callback.from_user.id):
            return True
        await callback.answer("Нет доступа", show_alert=True)
        return False

    async def edit_admin(
        callback: CallbackQuery,
        text: str,
        reply_markup: InlineKeyboardMarkup,
    ) -> None:
        await callback.answer()
        try:
            await callback.message.edit_text(text, reply_markup=reply_markup)
        except TelegramBadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise

    async def send_admin_panel(message: Message) -> None:
        await message.answer(
            await admin_summary_text(),
            reply_markup=admin_main_keyboard(),
        )

    @router.message(CommandStart())
    async def start(message: Message) -> None:
        await database.start_user(message)
        admin = bool(message.from_user and is_admin(message.from_user.id))
        await message.answer(
            "Здравствуй 🤍 Здесь мы разберёмся, почему красивая, стильная и "
            "самостоятельная женщина может получать внимание мужчин, но при "
            "этом не приходить к отношениям и семье.\n\nНачнём со знакомства.",
            reply_markup=start_keyboard(admin),
        )

    @router.message(Command("admin"))
    async def admin_command(message: Message) -> None:
        if message.from_user is None or not is_admin(message.from_user.id):
            return
        await send_admin_panel(message)

    @router.callback_query(F.data == "admin:open")
    async def admin_open(callback: CallbackQuery) -> None:
        if not await ensure_admin(callback):
            return
        await callback.answer()
        await send_admin_panel(callback.message)

    @router.callback_query(F.data == "admin:main")
    async def admin_main(callback: CallbackQuery) -> None:
        if not await ensure_admin(callback):
            return
        await edit_admin(
            callback,
            await admin_summary_text(),
            admin_main_keyboard(),
        )

    @router.callback_query(F.data == "admin:metrics")
    async def admin_metrics(callback: CallbackQuery) -> None:
        if not await ensure_admin(callback):
            return
        metrics = await database.admin_metrics(
            FUNNEL_EVENTS,
            SEGMENT_EVENTS,
            tuple(settings.admin_ids),
        )
        await edit_admin(callback, format_funnel_metrics(metrics), admin_back_keyboard())

    @router.callback_query(F.data.startswith("admin:users:"))
    async def admin_users(callback: CallbackQuery) -> None:
        if not await ensure_admin(callback) or callback.data is None:
            return
        raw_page = callback.data.rsplit(":", maxsplit=1)[1]
        page = (
            int(raw_page)
            if raw_page.isdigit() and len(raw_page) <= 6
            else 0
        )
        excluded_ids = tuple(settings.admin_ids)
        users, total = await database.admin_users(
            page,
            ADMIN_PAGE_SIZE,
            excluded_ids,
        )
        pages = max((total + ADMIN_PAGE_SIZE - 1) // ADMIN_PAGE_SIZE, 1)
        if page >= pages:
            page = pages - 1
            users, total = await database.admin_users(
                page,
                ADMIN_PAGE_SIZE,
                excluded_ids,
            )

        if users:
            lines = [
                (
                    f"{page * ADMIN_PAGE_SIZE + index}. "
                    f"{compact_name(row['full_name'])}\n"
                    f"   {row['status']} · "
                    f"{'купил' if row['purchased'] else 'не купил'}"
                )
                for index, row in enumerate(users, start=1)
            ]
        else:
            lines = ["Пользователей пока нет."]

        rows = [
            [
                InlineKeyboardButton(
                    text=compact_name(row["full_name"], 28),
                    callback_data=(
                        f"admin:user:{row['telegram_id']}:{page}"
                    ),
                )
            ]
            for row in users
        ]
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="◀️",
                    callback_data=f"admin:users:{page - 1}",
                )
            )
        navigation.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{pages}",
                callback_data=f"admin:users:{page}",
            )
        )
        if page + 1 < pages:
            navigation.append(
                InlineKeyboardButton(
                    text="▶️",
                    callback_data=f"admin:users:{page + 1}",
                )
            )
        rows.append(navigation)
        rows.append(
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:main")]
        )
        await edit_admin(
            callback,
            f"👥 Пользователи: {total}\n\n" + "\n".join(lines),
            InlineKeyboardMarkup(inline_keyboard=rows),
        )

    @router.callback_query(F.data.startswith("admin:user:"))
    async def admin_user(callback: CallbackQuery) -> None:
        if not await ensure_admin(callback) or callback.data is None:
            return
        parts = callback.data.split(":")
        if (
            len(parts) != 4
            or not parts[2].isdigit()
            or len(parts[2]) > 18
            or not parts[3].isdigit()
            or len(parts[3]) > 6
        ):
            await callback.answer("Некорректный пользователь", show_alert=True)
            return
        telegram_id = int(parts[2])
        page = int(parts[3])
        details = await database.admin_user(telegram_id)
        if details is None:
            await edit_admin(
                callback,
                "Пользователь не найден.",
                admin_back_keyboard(f"admin:users:{page}"),
            )
            return
        user = details["user"]
        events = details["events"]
        event_lines = [
            f"• {row['event']} — {compact_datetime(row['created_at'])}"
            for row in events
        ]
        username = f"@{user['username']}" if user["username"] else "нет"
        text = (
            f"👤 {user['full_name']}\n\n"
            f"Telegram ID: {user['telegram_id']}\n"
            f"Username: {username}\n"
            f"Статус: {user['status']}\n"
            f"Сегмент: {user['segment'] or 'не выбран'}\n"
            f"Оплата: {'да' if user['purchased'] else 'нет'}\n"
            f"Напоминаний: {user['reminders_sent']}/3\n"
            f"Обновлён: {compact_datetime(user['updated_at'])}\n\n"
            "Последние события:\n"
            + ("\n".join(event_lines) if event_lines else "Нет событий.")
        )
        await edit_admin(
            callback,
            text,
            admin_back_keyboard(f"admin:users:{page}"),
        )

    @router.callback_query(F.data == "admin:close")
    async def admin_close(callback: CallbackQuery) -> None:
        if not await ensure_admin(callback):
            return
        await callback.answer()
        await callback.message.edit_text("Админ-панель закрыта.")

    @router.callback_query(F.data.startswith("step:"))
    async def open_step(callback: CallbackQuery) -> None:
        if callback.data is None or callback.from_user is None:
            return
        step = int(callback.data.split(":", maxsplit=1)[1])
        if step not in range(1, 10):
            await callback.answer("Неизвестный шаг", show_alert=True)
            return
        await callback.answer()
        status = STATUS_BY_STEP.get(step)
        if status:
            await database.set_status(callback.from_user.id, status)
        if step == 9:
            if await database.is_purchased(callback.from_user.id):
                await send_step(callback.bot, callback.from_user.id, settings, 10)
                return
            await database.start_payment(callback.from_user.id)
        await send_step(callback.bot, callback.from_user.id, settings, step)

    @router.callback_query(F.data.startswith("segment:"))
    async def save_segment(callback: CallbackQuery) -> None:
        if callback.data is None:
            return
        segment_key = callback.data.split(":", maxsplit=1)[1]
        segment = SEGMENTS.get(segment_key)
        if segment is None:
            await callback.answer("Неизвестный вариант", show_alert=True)
            return
        await database.set_segment(callback.from_user.id, segment)
        await callback.answer()
        await callback.message.answer(
            "Спасибо за честный ответ.\n\n"
            "Уже то, что ты смогла это заметить, — первый шаг к изменениям.",
            reply_markup=keyboard("Продолжить", "step:4"),
        )

    @router.callback_query(F.data == "question:start")
    async def request_question(callback: CallbackQuery) -> None:
        await database.set_status(callback.from_user.id, "Ожидает вопрос")
        await callback.answer()
        await callback.message.answer(
            "Напиши свой вопрос сюда, и мы обязательно ответим."
        )

    @router.callback_query(F.data == "payment:start")
    async def start_payment(callback: CallbackQuery) -> None:
        if callback.from_user is None or callback.message is None:
            return
        if await database.is_purchased(callback.from_user.id):
            await callback.answer("У тебя уже есть доступ")
            await send_step(callback.bot, callback.from_user.id, settings, 10)
            return
        await database.start_payment(callback.from_user.id)
        await callback.answer()
        if settings.payment_provider_token:
            try:
                await send_course_invoice(
                    callback.bot, callback.message.chat.id, settings
                )
            except Exception:
                logging.exception("Не удалось отправить счёт на оплату")
                await callback.message.answer(
                    "Не удалось открыть оплату. Попробуй ещё раз через минуту."
                )
            return
        if settings.payment_url:
            markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="Перейти к безопасной оплате",
                            url=payment_url_for_user(
                                settings.payment_url, callback.from_user.id
                            ),
                        )
                    ]
                ]
            )
            await callback.message.answer(
                "Нажми кнопку ниже. После оплаты доступ откроется автоматически.",
                reply_markup=markup,
            )
            return
        await callback.message.answer(
            "Платёжная система пока не подключена.\n\n"
            "Для проверки MVP используй тестовое подтверждение:",
            reply_markup=keyboard(
                "Тест: подтвердить успешную оплату", "payment:demo_success"
            ),
        )

    @router.pre_checkout_query()
    async def pre_checkout_handler(
        pre_checkout_query: PreCheckoutQuery, bot: Bot
    ) -> None:
        query = pre_checkout_query
        if query.currency != "RUB":
            await bot.answer_pre_checkout_query(
                query.id, ok=False, error_message="Поддерживается только RUB."
            )
            return
        if query.invoice_payload != INVOICE_PAYLOAD_COURSE:
            await bot.answer_pre_checkout_query(
                query.id, ok=False, error_message="Неизвестный счёт."
            )
            return
        if query.total_amount != settings.course_price_kopecks:
            await bot.answer_pre_checkout_query(
                query.id, ok=False, error_message="Сумма не совпадает."
            )
            return
        await bot.answer_pre_checkout_query(query.id, ok=True)

    @router.message(F.content_type == ContentType.SUCCESSFUL_PAYMENT)
    async def successful_payment_handler(message: Message) -> None:
        if message.from_user is None or message.successful_payment is None:
            return
        payment = message.successful_payment
        payload = str(payment.invoice_payload or "")
        if payload != INVOICE_PAYLOAD_COURSE:
            return
        provider_charge_id = getattr(payment, "provider_payment_charge_id", None)
        inserted = await database.record_payment(
            telegram_id=message.from_user.id,
            currency=str(payment.currency or "RUB"),
            total_amount=int(payment.total_amount),
            invoice_payload=payload,
            telegram_payment_charge_id=str(payment.telegram_payment_charge_id),
            provider_payment_charge_id=(
                str(provider_charge_id) if provider_charge_id else None
            ),
        )
        if inserted:
            await grant_paid_access(
                message.bot, settings, database, message.from_user.id
            )
        elif await database.is_purchased(message.from_user.id):
            if await database.needs_access_delivery(message.from_user.id):
                await grant_paid_access(
                    message.bot, settings, database, message.from_user.id
                )

    async def complete_payment(bot: Bot, telegram_id: int) -> None:
        await grant_paid_access(bot, settings, database, telegram_id)

    @router.callback_query(F.data == "payment:demo_success")
    async def demo_payment(callback: CallbackQuery) -> None:
        if settings.payment_provider_token or settings.payment_url:
            await callback.answer("Тестовая оплата отключена", show_alert=True)
            return
        await callback.answer()
        await complete_payment(callback.bot, callback.from_user.id)

    @router.message(Command("grant"))
    async def grant_access(message: Message) -> None:
        if message.from_user is None or message.from_user.id not in settings.admin_ids:
            return
        parts = (message.text or "").split()
        if (
            len(parts) != 2
            or not parts[1].isdigit()
            or len(parts[1]) > 18
        ):
            await message.answer("Формат: /grant TELEGRAM_ID")
            return
        telegram_id = int(parts[1])
        result = await grant_paid_access(
            message.bot, settings, database, telegram_id
        )
        if result == "not_found":
            await message.answer(
                "Пользователь не найден. Сначала он должен запустить бота."
            )
            return
        if result == "already_paid":
            await message.answer("У пользователя уже есть доступ.")
            return
        await message.answer(f"Доступ выдан пользователю {telegram_id}.")

    @router.message(Command("questions"))
    async def show_questions(message: Message) -> None:
        if message.from_user is None or message.from_user.id not in settings.admin_ids:
            return
        questions = await database.unanswered_questions()
        if not questions:
            await message.answer("Новых вопросов нет.")
            return
        chunks = [
            f"#{row['id']} · user {row['telegram_id']}\n{row['text']}"
            for row in questions
        ]
        await message.answer(
            "\n\n".join(chunks)
            + "\n\nОтвет: /answer НОМЕР текст ответа"
        )

    @router.message(Command("answer"))
    async def answer_question(message: Message) -> None:
        if message.from_user is None or message.from_user.id not in settings.admin_ids:
            return
        parts = (message.text or "").split(maxsplit=2)
        if (
            len(parts) != 3
            or not parts[1].isdigit()
            or len(parts[1]) > 18
        ):
            await message.answer("Формат: /answer НОМЕР текст ответа")
            return
        question_id = int(parts[1])
        telegram_id = await database.claim_question(question_id)
        if telegram_id is None:
            await message.answer("Вопрос не найден или на него уже ответили.")
            return
        delivered = False
        try:
            await message.bot.send_message(
                telegram_id,
                f"Ответ на твой вопрос:\n\n{parts[2]}",
            )
            delivered = True
        finally:
            if not delivered:
                await database.release_question(question_id)
        await database.mark_question_answered(question_id)
        await message.answer("Ответ отправлен.")

    @router.message(F.text)
    async def receive_text(message: Message) -> None:
        if message.from_user is None:
            return
        status = await database.get_status(message.from_user.id)
        if status != "Ожидает вопрос":
            return
        text = message.text or ""
        await database.save_question(message.from_user.id, text)
        await message.answer(
            "Спасибо 🤍 Вопрос передан. Мы обязательно ответим."
        )
        for admin_id in settings.admin_ids:
            try:
                await message.bot.send_message(
                    admin_id,
                    "Новый вопрос из бота\n\n"
                    f"От: {message.from_user.full_name} "
                    f"(@{message.from_user.username or 'без username'}, "
                    f"ID {message.from_user.id})\n\n{text}",
                )
            except Exception:
                logging.exception("Не удалось отправить вопрос менеджеру %s", admin_id)

    return router


async def start_payment_webhook(
    bot: Bot, settings: Settings, database: Database
) -> web.AppRunner | None:
    if not settings.payment_webhook_secret:
        return None

    async def payment_success(request: web.Request) -> web.Response:
        authorization = request.headers.get("Authorization", "")
        expected = f"Bearer {settings.payment_webhook_secret}"
        if not hmac.compare_digest(authorization, expected):
            raise web.HTTPUnauthorized()
        try:
            payload = await request.json()
            telegram_id = int(payload["telegram_id"])
        except (KeyError, TypeError, ValueError):
            raise web.HTTPBadRequest(
                text='Ожидается JSON: {"telegram_id": 123456789}'
            )

        result = await grant_paid_access(
            bot, settings, database, telegram_id
        )
        if result == "not_found":
            raise web.HTTPNotFound(text="Пользователь не найден")
        return web.json_response({"ok": True, "status": result})

    application = web.Application()
    application.router.add_post("/payment/success", payment_success)
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(
        runner,
        host=settings.payment_webhook_host,
        port=settings.payment_webhook_port,
    )
    await site.start()
    logging.info(
        "Payment webhook запущен на %s:%s",
        settings.payment_webhook_host,
        settings.payment_webhook_port,
    )
    return runner


async def process_payment_reminders(
    bot: Bot,
    settings: Settings,
    database: Database,
    *,
    now: datetime | None = None,
) -> int:
    current_time = now or utc_now()
    sent_count = 0
    for row in await database.payment_candidates():
        telegram_id = int(row["telegram_id"])
        sent = int(row["reminders_sent"])
        try:
            started_at = datetime.fromisoformat(str(row["payment_started_at"]))
            if current_time < started_at + settings.reminder_delays[sent]:
                continue
            if not await database.claim_reminder(telegram_id, sent + 1):
                continue
            if await database.is_purchased(telegram_id):
                await database.release_reminder(telegram_id, sent + 1)
                continue
            text, button_text = REMINDERS[sent]
            target_step = 8 if sent == 0 else 9
            await bot.send_message(
                telegram_id,
                text,
                reply_markup=keyboard(button_text, f"step:{target_step}"),
            )
            await database.complete_reminder(telegram_id, sent + 1)
            sent_count += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            logging.info("Напоминание недоступно пользователю %s", telegram_id)
            await database.complete_reminder(telegram_id, sent + 1)
        except Exception:
            await database.release_reminder(telegram_id, sent + 1)
            logging.exception(
                "Не удалось отправить напоминание пользователю %s",
                telegram_id,
            )
    return sent_count


async def reminder_worker(
    bot: Bot, settings: Settings, database: Database
) -> None:
    while True:
        try:
            await process_payment_reminders(bot, settings, database)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Ошибка фоновой проверки напоминаний")
        await asyncio.sleep(60)


def create_bot(settings: Settings) -> Bot:
    if settings.telegram_proxy:
        logging.info("Telegram proxy: %s", mask_proxy_url(settings.telegram_proxy))
        session = AiohttpSession(proxy=settings.telegram_proxy)
        return Bot(settings.bot_token, session=session)
    return Bot(settings.bot_token)


async def ensure_telegram_connection(bot: Bot, settings: Settings) -> None:
    try:
        me = await bot.get_me()
    except TelegramNetworkError as error:
        hint = (
            "Не удалось подключиться к api.telegram.org.\n"
            "На Windows это часто блокировка или таймаут сети.\n"
            "Включи VPN и/или укажи прокси в .env:\n"
            "TELEGRAM_PROXY=http://127.0.0.1:7890\n"
            "или TELEGRAM_PROXY=socks5://127.0.0.1:10808"
        )
        if settings.telegram_proxy:
            hint += f"\n\nСейчас прокси задан: {mask_proxy_url(settings.telegram_proxy)}"
        raise SystemExit(f"{hint}\n\nОшибка: {error}") from error
    logging.info("Бот подключён: @%s (id=%s)", me.username, me.id)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    settings = Settings.from_env()
    database = Database(settings.database_path)
    await database.initialize()

    if settings.payment_provider_token:
        logging.info("Оплата: ЮKassa provider token задан")
    else:
        logging.warning(
            "PAYMENT_PROVIDER_TOKEN не задан — кнопка оплаты покажет заглушку"
        )

    bot = create_bot(settings)
    dispatcher = Dispatcher()
    dispatcher.include_router(create_router(settings, database))
    await ensure_telegram_connection(bot, settings)
    try:
        await bot.set_my_commands(
            [
                BotCommand(command="start", description="Начать сначала"),
            ]
        )
    except TelegramNetworkError:
        logging.warning("Не удалось обновить меню команд, polling продолжится")

    reminder_task = asyncio.create_task(
        reminder_worker(bot, settings, database),
        name="payment-reminders",
    )
    webhook_runner = await start_payment_webhook(bot, settings, database)
    try:
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
        )
    finally:
        reminder_task.cancel()
        await asyncio.gather(reminder_task, return_exceptions=True)
        if webhook_runner is not None:
            await webhook_runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
