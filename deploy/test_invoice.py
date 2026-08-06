import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot import Settings, create_bot, send_course_invoice


async def main() -> None:
    settings = Settings.from_env()
    admin_id = next(iter(settings.admin_ids), None)
    if admin_id is None:
        admin_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    if not admin_id:
        print("NO_ADMIN")
        return
    bot = create_bot(settings)
    try:
        await send_course_invoice(bot, admin_id, settings)
        print(f"INVOICE_OK admin={admin_id} price={settings.course_price_kopecks}")
    except Exception as error:
        print(f"INVOICE_FAIL {error!r}")
        raise
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
