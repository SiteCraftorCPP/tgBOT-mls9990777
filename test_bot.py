from __future__ import annotations

import asyncio
import re
import socket
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

from aiohttp import ClientSession

from bot import (
    Database,
    FUNNEL_EVENTS,
    INVOICE_PAYLOAD_COURSE,
    REMINDERS,
    SEGMENT_EVENTS,
    STEPS,
    Settings,
    build_yookassa_provider_data,
    find_step_image,
    format_funnel_metrics,
    normalize_telegram_proxy,
    payment_url_for_user,
    process_payment_reminders,
    send_step,
    start_keyboard,
    start_payment_webhook,
    to_db_time,
    truncate_text,
    utc_now,
)


def settings(
    image_dir: Path,
    *,
    webhook_secret: str = "",
    webhook_port: int = 8080,
    reminder_delays: tuple[timedelta, ...] | None = None,
) -> Settings:
    return Settings(
        bot_token="test",
        database_path=image_dir / "unused.sqlite3",
        image_dir=image_dir,
        payment_url="",
        payment_provider_token="",
        course_price_kopecks=199000,
        yookassa_tax_system_code=2,
        yookassa_vat_code=1,
        course_url="https://t.me/example_course",
        admin_ids=frozenset(),
        reminder_delays=reminder_delays
        or (timedelta(hours=3), timedelta(hours=24), timedelta(hours=48)),
        payment_webhook_secret=webhook_secret,
        payment_webhook_host="127.0.0.1",
        payment_webhook_port=webhook_port,
        telegram_proxy="",
    )


class FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str, object]] = []
        self.photos: list[tuple[int, object, str | None, object]] = []

    async def send_message(
        self, chat_id: int, text: str, reply_markup: object = None
    ) -> None:
        self.messages.append((chat_id, text, reply_markup))

    async def send_photo(
        self,
        chat_id: int,
        photo: object,
        caption: str | None = None,
        reply_markup: object = None,
    ) -> None:
        self.photos.append((chat_id, photo, caption, reply_markup))


class BotMvpTests(unittest.IsolatedAsyncioTestCase):
    def test_telegram_proxy_normalizes_host_port_user_pass(self) -> None:
        self.assertEqual(
            normalize_telegram_proxy("155.212.96.154:64475:user:pass"),
            "socks5://user:pass@155.212.96.154:64475",
        )

    def test_admin_button_is_visible_only_for_admin_start(self) -> None:
        user_buttons = start_keyboard(False).inline_keyboard
        admin_buttons = start_keyboard(True).inline_keyboard
        self.assertEqual(len(user_buttons), 1)
        self.assertEqual(len(admin_buttons), 2)
        self.assertEqual(
            admin_buttons[1][0].callback_data,
            "admin:open",
        )
        self.assertEqual(len(truncate_text("x" * 5000, 3000)), 3000)

    def test_payment_url_contains_user_id(self) -> None:
        self.assertEqual(
            payment_url_for_user("https://pay.example/order?product=1", 42),
            "https://pay.example/order?product=1&telegram_id=42",
        )

    def test_yookassa_provider_data_contains_receipt(self) -> None:
        current_settings = settings(Path("."))
        payload = build_yookassa_provider_data(
            current_settings, 199000, "Доступ к курсу"
        )
        self.assertIsNotNone(payload)
        self.assertIn("receipt", payload)
        self.assertIn("199", payload)

    def test_yookassa_provider_data_disabled_without_tax_code(self) -> None:
        current_settings = Settings(
            bot_token="test",
            database_path=Path("unused.sqlite3"),
            image_dir=Path("."),
            payment_url="",
            payment_provider_token="",
            course_price_kopecks=199000,
            yookassa_tax_system_code=0,
            yookassa_vat_code=1,
            course_url="https://t.me/example_course",
            admin_ids=frozenset(),
            reminder_delays=(timedelta(hours=3), timedelta(hours=24), timedelta(hours=48)),
            payment_webhook_secret="",
            payment_webhook_host="127.0.0.1",
            payment_webhook_port=8080,
            telegram_proxy="",
        )
        self.assertIsNone(
            build_yookassa_provider_data(current_settings, 199000, "Доступ")
        )

    async def test_record_payment_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Database(Path(temp_dir) / "test.sqlite3")
            await database.initialize()
            user = SimpleNamespace(id=42, username="test", full_name="Test User")
            await database.start_user(SimpleNamespace(from_user=user))
            first = await database.record_payment(
                42,
                "RUB",
                199000,
                INVOICE_PAYLOAD_COURSE,
                "tg_charge_1",
                "yk_charge_1",
            )
            second = await database.record_payment(
                42,
                "RUB",
                199000,
                INVOICE_PAYLOAD_COURSE,
                "tg_charge_1",
                "yk_charge_1",
            )
            self.assertTrue(first)
            self.assertFalse(second)

    async def test_every_step_follows_tz_message_order_without_images(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bot = FakeBot()
            current_settings = settings(Path(temp_dir))
            for step in range(1, 11):
                await send_step(bot, 1, current_settings, step)

            self.assertEqual(len(bot.photos), 0)
            self.assertEqual(len(bot.messages), 20)
            card, after, _, _ = STEPS[1]
            self.assertEqual(bot.messages[0][1], card)
            self.assertEqual(bot.messages[1][1], after)

    async def test_step_with_image_follows_tz_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            image_dir = Path(temp_dir)
            (image_dir / "IMG_1.JPG").write_bytes(b"placeholder")
            bot = FakeBot()
            card, after, _, _ = STEPS[1]

            await send_step(bot, 1, settings(image_dir), 1)

            self.assertEqual(len(bot.photos), 1)
            self.assertEqual(bot.photos[0][2], card)
            self.assertEqual(len(bot.messages), 1)
            self.assertIsNotNone(bot.messages[0][2])

    async def test_project_images_map_to_all_steps(self) -> None:
        image_dir = Path(__file__).resolve().parent / "images"
        if not image_dir.is_dir():
            self.skipTest("images folder missing")
        current_settings = settings(image_dir)
        for step in range(1, 11):
            path = find_step_image(current_settings, step)
            self.assertIsNotNone(path, f"step {step}")
            self.assertEqual(int(re.findall(r"\d+", path.stem)[-1]), step)

    async def test_sparse_images_are_not_assigned_to_wrong_steps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            image_dir = Path(temp_dir)
            (image_dir / "step_2.jpg").write_bytes(b"placeholder")
            bot = FakeBot()

            await send_step(bot, 1, settings(image_dir), 1)

            self.assertEqual(len(bot.photos), 0)
            self.assertEqual(len(bot.messages), 2)
            card, _, _, _ = STEPS[1]
            self.assertEqual(bot.messages[0][1], card)

    async def test_payment_is_idempotent_and_unknown_grant_is_safe(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Database(Path(temp_dir) / "test.sqlite3")
            await database.initialize()
            self.assertEqual(await database.mark_paid(999), "not_found")

            user = SimpleNamespace(id=42, username="test", full_name="Test User")
            message = SimpleNamespace(from_user=user)
            await database.start_user(message)
            await database.start_payment(42)
            first_candidate = (await database.payment_candidates())[0]
            self.assertTrue(await database.claim_reminder(42, 1))
            self.assertFalse(await database.claim_reminder(42, 1))
            await database.complete_reminder(42, 1)
            await database.start_payment(42)
            repeated_candidate = (await database.payment_candidates())[0]
            self.assertEqual(repeated_candidate["reminders_sent"], 1)
            self.assertEqual(
                repeated_candidate["payment_started_at"],
                first_candidate["payment_started_at"],
            )
            payment_results = await asyncio.gather(
                database.mark_paid(42),
                database.mark_paid(42),
            )
            self.assertCountEqual(payment_results, ["paid", "already_paid"])

            await database.start_user(message)
            self.assertEqual(await database.get_status(42), "Получил доступ")

            await database.save_question(42, "Тестовый вопрос")
            question_id = int((await database.unanswered_questions())[0]["id"])
            claims = await asyncio.gather(
                database.claim_question(question_id),
                database.claim_question(question_id),
            )
            self.assertCountEqual(claims, [42, None])

    async def test_payment_webhook_opens_access(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            database = Database(path / "test.sqlite3")
            await database.initialize()
            user = SimpleNamespace(id=42, username="test", full_name="Test User")
            await database.start_user(SimpleNamespace(from_user=user))

            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = int(probe.getsockname()[1])

            bot = FakeBot()
            current_settings = settings(
                path,
                webhook_secret="secret",
                webhook_port=port,
            )
            runner = await start_payment_webhook(
                bot, current_settings, database
            )
            self.assertIsNotNone(runner)
            try:
                async with ClientSession() as session:
                    response = await session.post(
                        f"http://127.0.0.1:{port}/payment/success",
                        headers={"Authorization": "Bearer secret"},
                        json={"telegram_id": 42},
                    )
                    self.assertEqual(response.status, 200)
                    self.assertEqual((await response.json())["status"], "paid")
                    repeated = await session.post(
                        f"http://127.0.0.1:{port}/payment/success",
                        headers={"Authorization": "Bearer secret"},
                        json={"telegram_id": 42},
                    )
                    self.assertEqual(repeated.status, 200)
                    self.assertEqual(
                        (await repeated.json())["status"],
                        "already_paid",
                    )
            finally:
                if runner is not None:
                    await runner.cleanup()

            self.assertTrue(await database.is_purchased(42))
            self.assertEqual(len(bot.messages), 2)

    async def test_admin_metrics_track_funnel_and_segments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Database(Path(temp_dir) / "test.sqlite3")
            await database.initialize()
            user = SimpleNamespace(id=42, username="test", full_name="Test User")
            await database.start_user(SimpleNamespace(from_user=user))
            admin = SimpleNamespace(id=99, username="admin", full_name="Admin")
            await database.start_user(SimpleNamespace(from_user=admin))
            await database.set_status(42, "Прошёл знакомство")
            await database.set_segment(42, "Неопределённость")
            await database.start_payment(42)
            self.assertTrue(await database.claim_reminder(42, 1))
            await database.complete_reminder(42, 1)

            metrics = await database.admin_metrics(
                FUNNEL_EVENTS,
                SEGMENT_EVENTS,
                (99,),
            )
            self.assertEqual(metrics["total_users"], 1)
            self.assertEqual(metrics["funnel"]["Запустил бота"], 1)
            self.assertEqual(metrics["funnel"]["Прошёл знакомство"], 1)
            self.assertEqual(metrics["funnel"]["Узнал себя"], 1)
            self.assertEqual(metrics["funnel"]["Перешёл к оплате"], 1)
            self.assertEqual(metrics["funnel"]["Оплата не завершена"], 1)
            self.assertEqual(metrics["segments"]["Неопределённость"], 1)
            self.assertNotIn("purchased", metrics)
            text = format_funnel_metrics(metrics)
            self.assertNotIn("Конверсия", text)
            self.assertNotIn("вопрос", text.lower())

            users, total = await database.admin_users(0, excluded_ids=(99,))
            self.assertEqual(total, 1)
            self.assertEqual(users[0]["telegram_id"], 42)
            details = await database.admin_user(42)
            self.assertIsNotNone(details)
            self.assertNotIn("pending_questions", details)

    async def test_reminder_waits_until_delay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            database = Database(path / "test.sqlite3")
            await database.initialize()
            user = SimpleNamespace(id=42, username="test", full_name="Test User")
            await database.start_user(SimpleNamespace(from_user=user))
            await database.start_payment(42)
            bot = FakeBot()
            current_settings = settings(
                path,
                reminder_delays=(
                    timedelta(hours=3),
                    timedelta(hours=24),
                    timedelta(hours=48),
                ),
            )

            sent = await process_payment_reminders(
                bot, current_settings, database, now=utc_now()
            )
            self.assertEqual(sent, 0)
            self.assertEqual(len(bot.messages), 0)

    async def test_reminder_sequence_and_buttons(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            database = Database(path / "test.sqlite3")
            await database.initialize()
            user = SimpleNamespace(id=42, username="test", full_name="Test User")
            await database.start_user(SimpleNamespace(from_user=user))
            await database.start_payment(42)
            bot = FakeBot()
            delays = (
                timedelta(hours=3),
                timedelta(hours=24),
                timedelta(hours=48),
            )
            current_settings = settings(path, reminder_delays=delays)

            async with database.connect() as connection:
                past = to_db_time(utc_now() - timedelta(hours=4))
                await connection.execute(
                    "UPDATE users SET payment_started_at = ? WHERE telegram_id = ?",
                    (past, 42),
                )
                await connection.commit()

            sent = await process_payment_reminders(
                bot, current_settings, database, now=utc_now()
            )
            self.assertEqual(sent, 1)
            self.assertEqual(bot.messages[0][1], REMINDERS[0][0])
            self.assertEqual(
                bot.messages[0][2].inline_keyboard[0][0].callback_data,
                "step:8",
            )
            row = (await database.payment_candidates())[0]
            self.assertEqual(int(row["reminders_sent"]), 1)

            async with database.connect() as connection:
                past = to_db_time(utc_now() - timedelta(hours=25))
                await connection.execute(
                    "UPDATE users SET payment_started_at = ? WHERE telegram_id = ?",
                    (past, 42),
                )
                await connection.commit()

            sent = await process_payment_reminders(
                bot, current_settings, database, now=utc_now()
            )
            self.assertEqual(sent, 1)
            self.assertEqual(
                bot.messages[1][2].inline_keyboard[0][0].callback_data,
                "step:9",
            )

            async with database.connect() as connection:
                past = to_db_time(utc_now() - timedelta(hours=49))
                await connection.execute(
                    "UPDATE users SET payment_started_at = ? WHERE telegram_id = ?",
                    (past, 42),
                )
                await connection.commit()

            sent = await process_payment_reminders(
                bot, current_settings, database, now=utc_now()
            )
            self.assertEqual(sent, 1)
            self.assertEqual(len(await database.payment_candidates()), 0)

    async def test_reminder_skips_purchased_users(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            database = Database(path / "test.sqlite3")
            await database.initialize()
            user = SimpleNamespace(id=42, username="test", full_name="Test User")
            await database.start_user(SimpleNamespace(from_user=user))
            await database.start_payment(42)
            await database.mark_paid(42)
            bot = FakeBot()
            current_settings = settings(
                path,
                reminder_delays=(timedelta(0), timedelta(hours=24), timedelta(hours=48)),
            )

            sent = await process_payment_reminders(
                bot, current_settings, database, now=utc_now()
            )
            self.assertEqual(sent, 0)
            self.assertEqual(len(bot.messages), 0)
            self.assertEqual(len(await database.payment_candidates()), 0)


if __name__ == "__main__":
    unittest.main()
