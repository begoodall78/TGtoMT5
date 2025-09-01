# tg_list_channels.py  (Telethon v2)
import asyncio, os
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient

SCRIPT_DIR = Path(__file__).resolve().parent
loaded = load_dotenv(SCRIPT_DIR / ".env") or load_dotenv(SCRIPT_DIR / "creds.env")
API_ID   = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
PHONE    = os.getenv("TG_PHONE")

client = TelegramClient("tg_session", API_ID, API_HASH)

async def main():
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(PHONE)
        code = input("Login code: ").strip()
        await client.sign_in(PHONE, code)

    print("\nYour dialogs (channels/groups/chats):")
    async for d in client.iter_dialogs():
        ent = d.entity
        title = getattr(ent, "title", getattr(ent, "first_name", "")) or ""
        uname = getattr(ent, "username", None)
        cid   = getattr(ent, "id", None)  # channels are usually -100xxxxxxxxxx
        kind  = ent.__class__.__name__
        print(f"- {kind:12}  title='{title}'  username='{uname}'  id={cid}")

    # Quick filter for “Trade With Kam”
    print("\nMatches for 'Trade With Kam':")
    async for d in client.iter_dialogs():
        ent = d.entity
        title = getattr(ent, "title", "") or ""
        if "trade with kam".lower() in title.lower():
            print(f"  → title='{title}', username='{getattr(ent,'username',None)}', id={getattr(ent,'id',None)}")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
