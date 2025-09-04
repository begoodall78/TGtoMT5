# tg_list_channels.py  (Telethon v2)
import asyncio, os, sys
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User, InputPeerChannel, InputPeerChat, InputPeerUser
from telethon.utils import get_peer_id, resolve_id

# Try multiple locations for the .env file
SCRIPT_DIR = Path(__file__).resolve().parent
POSSIBLE_ENV_PATHS = [
    SCRIPT_DIR / ".env",
    SCRIPT_DIR / "creds.env",
    SCRIPT_DIR.parent / ".env",
    SCRIPT_DIR.parent / "creds.env",
    Path.cwd() / ".env",
    Path.cwd() / "creds.env",
    Path("C:/Trading/TGtoMT5/.env"),  # Your specific path
    Path("C:/Trading/TGtoMT5/creds.env"),
]

# Try to load env file from various locations
loaded = False
for env_path in POSSIBLE_ENV_PATHS:
    if env_path.exists():
        print(f"📁 Found env file at: {env_path}")
        loaded = load_dotenv(env_path)
        if loaded:
            print(f"✅ Loaded environment from: {env_path}")
            break
        
if not loaded:
    print("❌ Could not find .env or creds.env file!")
    print("\nSearched in these locations:")
    for p in POSSIBLE_ENV_PATHS:
        print(f"  - {p}")
    print("\nPlease create a .env or creds.env file with:")
    print("TG_API_ID=your_api_id")
    print("TG_API_HASH=your_api_hash")
    print("TG_PHONE=your_phone_number")
    sys.exit(1)

# Load with error checking
API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
PHONE = os.getenv("TG_PHONE")

# Validate required variables
if not API_ID or not API_HASH:
    print("❌ Missing required environment variables!")
    print(f"   TG_API_ID: {'✓ Set' if API_ID else '✗ Missing'}")
    print(f"   TG_API_HASH: {'✓ Set' if API_HASH else '✗ Missing'}")
    print(f"   TG_PHONE: {'✓ Set' if PHONE else '✗ Missing'}")
    print("\nPlease check your .env file contains:")
    print("TG_API_ID=your_api_id")
    print("TG_API_HASH=your_api_hash")
    print("TG_PHONE=your_phone_number")
    sys.exit(1)

try:
    API_ID = int(API_ID)
except ValueError:
    print(f"❌ TG_API_ID must be a number, got: {API_ID}")
    sys.exit(1)

client = TelegramClient("tg_session", API_ID, API_HASH)

def get_proper_chat_id(entity):
    """Get the proper chat ID that works with send_message"""
    if isinstance(entity, Channel):
        # Channels and megagroups need -100 prefix
        return -1000000000000 - entity.id
    elif isinstance(entity, Chat):
        # Regular chats are negative
        return -entity.id
    elif isinstance(entity, User):
        # Users are positive
        return entity.id
    else:
        # Fallback to whatever ID is present
        return getattr(entity, 'id', None)

def get_entity_type_detail(entity):
    """Get detailed entity type information"""
    if isinstance(entity, Channel):
        if entity.megagroup:
            return "Megagroup"
        elif entity.broadcast:
            return "Channel"
        else:
            return "Channel/Group"
    elif isinstance(entity, Chat):
        return "Chat"
    elif isinstance(entity, User):
        if entity.bot:
            return "Bot"
        else:
            return "User"
    else:
        return entity.__class__.__name__

async def test_send_capability(client, entity):
    """Test if we can send messages to this entity"""
    try:
        # Try to get input entity (this validates if we can interact with it)
        input_entity = await client.get_input_entity(entity)
        return "✓ Can send"
    except Exception as e:
        return f"✗ Cannot send: {str(e)[:30]}"

async def main():
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(PHONE)
        code = input("Login code: ").strip()
        await client.sign_in(PHONE, code)

    print("\n" + "="*80)
    print("YOUR DIALOGS - WITH PROPER IDS FOR send_message()")
    print("="*80)
    
    dialogs = []
    async for d in client.iter_dialogs():
        dialogs.append(d)
    
    # Sort by title for easier reading
    dialogs.sort(key=lambda d: getattr(d.entity, 'title', getattr(d.entity, 'first_name', '')) or '')
    
    for d in dialogs:
        ent = d.entity
        title = getattr(ent, "title", getattr(ent, "first_name", "")) or ""
        uname = getattr(ent, "username", None)
        
        # Get various ID formats
        raw_id = getattr(ent, "id", None)
        proper_id = get_proper_chat_id(ent)
        peer_id = get_peer_id(ent)
        
        # Get entity type
        entity_type = get_entity_type_detail(ent)
        
        # Test send capability
        can_send = await test_send_capability(client, ent)
        
        print(f"\n📌 {entity_type:12} | {title[:40]:<40}")
        print(f"   Username:     @{uname}" if uname else "   Username:     None")
        print(f"   Raw ID:       {raw_id}")
        print(f"   Proper ID:    {proper_id} ← USE THIS FOR send_message()")
        print(f"   Peer ID:      {peer_id}")
        print(f"   Send status:  {can_send}")
        
        # Show example usage
        if proper_id:
            print(f"   Example:      await client.send_message({proper_id}, 'Hello')")

    # Quick filter for specific search term
    search_term = input("\n\nSearch for a specific channel/chat (or press Enter to skip): ").strip()
    
    if search_term:
        print(f"\n" + "="*80)
        print(f"MATCHES FOR '{search_term}'")
        print("="*80)
        
        found = False
        for d in dialogs:
            ent = d.entity
            title = getattr(ent, "title", getattr(ent, "first_name", "")) or ""
            uname = getattr(ent, "username", "") or ""
            
            if search_term.lower() in title.lower() or search_term.lower() in uname.lower():
                found = True
                proper_id = get_proper_chat_id(ent)
                entity_type = get_entity_type_detail(ent)
                
                print(f"\n✅ FOUND: {title}")
                print(f"   Type:         {entity_type}")
                print(f"   Username:     @{uname}" if uname else "   Username:     None")
                print(f"   Proper ID:    {proper_id}")
                print(f"   Use this ID:  {proper_id}")
                print(f"   Example:      await client.send_message({proper_id}, 'Your message')")
        
        if not found:
            print(f"\n❌ No matches found for '{search_term}'")
    
    # Special section for debugging a specific problematic chat
    print(f"\n" + "="*80)
    print("DEBUG: Enter a chat ID to test (or press Enter to skip):")
    print("="*80)
    test_id = input("Chat ID to test: ").strip()
    
    if test_id:
        try:
            # Try different ID formats
            if test_id.startswith('@'):
                test_entity = await client.get_entity(test_id)
            else:
                test_id_int = int(test_id)
                test_entity = await client.get_entity(test_id_int)
            
            print(f"\n✅ Successfully retrieved entity!")
            print(f"   Type:         {get_entity_type_detail(test_entity)}")
            print(f"   Title:        {getattr(test_entity, 'title', getattr(test_entity, 'first_name', 'Unknown'))}")
            print(f"   Proper ID:    {get_proper_chat_id(test_entity)}")
            
            # Try to get input entity (required for sending)
            try:
                input_ent = await client.get_input_entity(test_entity)
                print(f"   ✅ Can send messages to this entity")
                print(f"   Use ID:       {get_proper_chat_id(test_entity)}")
            except Exception as e:
                print(f"   ❌ Cannot send: {e}")
                
        except Exception as e:
            print(f"\n❌ Error retrieving entity: {e}")
            print("\nTrying alternative ID formats...")
            
            # Try with -100 prefix if it's not already there
            if not str(test_id).startswith('-100'):
                try:
                    alt_id = f"-100{test_id}"
                    print(f"   Trying: {alt_id}")
                    test_entity = await client.get_entity(int(alt_id))
                    print(f"   ✅ Success with {alt_id}!")
                    print(f"   Use this ID: {alt_id}")
                except:
                    print(f"   ❌ Failed with {alt_id}")

    await client.disconnect()
    print("\n✅ Done!")

if __name__ == "__main__":
    asyncio.run(main())