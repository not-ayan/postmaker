import os
import re
import sqlite3
import asyncio
import logging
import requests
from datetime import datetime
from urllib.parse import unquote, urlparse
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.tl.custom import Conversation
# Import specific error
from telethon.errors import SessionPasswordNeededError, AlreadyInConversationError, MessageNotModifiedError # Removed ConversationNotStartedError
from telethon.utils import get_display_name

# Import bgen module
try:
    import bgen
    BANNER_SUPPORT = True
except ImportError as e:
    logger.warning(f"Banner generation module not available: {e}")
    BANNER_SUPPORT = False

# Import index module
try:
    import index
    INDEX_SUPPORT = True
except ImportError as e:
    logger.warning(f"Index module not available: {e}")
    INDEX_SUPPORT = False

# --- Configuration ---

# It's highly recommended to use environment variables or a config file for sensitive data
API_ID = os.environ.get("API_ID") # Replace with your API ID from my.telegram.org
API_HASH = os.environ.get("API_HASH") # Replace with your API HASH from my.telegram.org
BOT_TOKEN = os.environ.get("BOT_TOKEN") # Replace with your Bot Token
# Optional: Set owner ID directly if needed, otherwise the first user to start might be set
OWNER_ID = os.environ.get("OWNER_ID", None) # Replace with your Telegram User ID (optional)
if OWNER_ID:
    try:
        OWNER_ID = int(OWNER_ID)
    except ValueError:
        print("Warning: OWNER_ID environment variable is not a valid integer. Owner not set.")
        OWNER_ID = None

PASTEBIN_API_KEY =  os.environ.get("PASTEBIN_API_KEY") # Your Pastebin Dev Key
DB_FILE = 'settings.db'

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Database Setup ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS allowed_chats (
            chat_id INTEGER PRIMARY KEY
        )
    ''')
    # Add user_data table with device_changelog
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_data (
            user_id INTEGER PRIMARY KEY,
            support_group TEXT,
            notes TEXT,
            credits TEXT
            -- device_changelog will be added below if missing
        )
    ''')

    # --- Schema Migration: Add device_changelog if missing ---
    try:
        cursor.execute("PRAGMA table_info(user_data)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'device_changelog' not in columns:
            cursor.execute("ALTER TABLE user_data ADD COLUMN device_changelog TEXT")
            logger.info("Added missing 'device_changelog' column to 'user_data' table.")
    except Exception as e:
        logger.error(f"Error during schema migration for user_data table: {e}")
    # --- End Schema Migration ---

    # Add rom_presets table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rom_presets (
            preset_name TEXT PRIMARY KEY,
            rom_name TEXT NOT NULL,
            source_changelog TEXT
        )
    ''')
    # Set initial owner if provided and not already set
    cursor.execute("SELECT value FROM settings WHERE key = 'owner_id'")
    owner = cursor.fetchone()
    if not owner and OWNER_ID:
        cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('owner_id', str(OWNER_ID)))
        logger.info(f"Initial owner set to {OWNER_ID}")

    # Default PM setting to False (off) if not set
    cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('pm_enabled', 'false'))

    # Add banned_users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS banned_users (
            user_id INTEGER PRIMARY KEY,
            banned_at TEXT NOT NULL,
            banned_by INTEGER NOT NULL,
            reason TEXT
        )
    ''')

    # Add post_limits table to track daily posts
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS post_limits (
            user_id INTEGER,
            post_date TEXT,
            count INTEGER,
            PRIMARY KEY (user_id, post_date)
        )
    ''')

    # Add user_stats table for tracking activity
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            total_posts INTEGER DEFAULT 0,
            last_post_date TEXT,
            first_seen TEXT
        )
    ''')

    conn.commit()
    conn.close()

def db_execute(query, params=()):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    conn.close()

def db_fetchone(query, params=()):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(query, params)
    result = cursor.fetchone()
    conn.close()
    return result

def db_fetchall(query, params=()):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(query, params)
    result = cursor.fetchall()
    conn.close()
    return result

# --- User Data DB Functions ---
def get_user_data(user_id):
    """Fetches saved data for a user."""
    # Select the new device_changelog field - Corrected typo suport_group -> support_group
    return db_fetchone("SELECT support_group, notes, credits, device_changelog FROM user_data WHERE user_id = ?", (user_id,))

def save_user_data(user_id, support_group=None, notes=None, credits=None, device_changelog=None):
    """Saves or updates user data. Only updates fields that are not None."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO user_data (user_id) VALUES (?)", (user_id,))

    if support_group is not None:
        cursor.execute("UPDATE user_data SET support_group = ? WHERE user_id = ?", (support_group, user_id))
    if notes is not None:
        cursor.execute("UPDATE user_data SET notes = ? WHERE user_id = ?", (notes, user_id))
    if credits is not None:
        cursor.execute("UPDATE user_data SET credits = ? WHERE user_id = ?", (credits, user_id))
    # Save device_changelog
    if device_changelog is not None:
        cursor.execute("UPDATE user_data SET device_changelog = ? WHERE user_id = ?", (device_changelog, user_id))

    conn.commit()
    conn.close()
    logger.info(f"Updated user data for {user_id}")

# --- Preset DB Functions ---
def add_preset(preset_name, rom_name, source_changelog):
    """Adds or replaces a ROM preset."""
    # Allow 'none' for source changelog, store as NULL
    cl_to_store = None if source_changelog.lower() == 'none' else source_changelog
    db_execute("INSERT OR REPLACE INTO rom_presets (preset_name, rom_name, source_changelog) VALUES (?, ?, ?)",
               (preset_name.lower(), rom_name, cl_to_store)) # Store preset name in lowercase for consistency
    logger.info(f"Preset '{preset_name}' added/updated.")

def get_preset(preset_name):
    """Fetches a specific preset by name."""
    return db_fetchone("SELECT rom_name, source_changelog FROM rom_presets WHERE preset_name = ?", (preset_name.lower(),))

def list_presets():
    """Lists the names of all saved presets."""
    rows = db_fetchall("SELECT preset_name FROM rom_presets ORDER BY preset_name")
    return [row[0] for row in rows]

def delete_preset(preset_name):
    """Deletes a preset by name."""
    db_execute("DELETE FROM rom_presets WHERE preset_name = ?", (preset_name.lower(),))
    logger.info(f"Preset '{preset_name}' deleted.")

def get_owner_id():
    row = db_fetchone("SELECT value FROM settings WHERE key = 'owner_id'")
    return int(row[0]) if row else None

def set_owner_id(user_id):
    db_execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ('owner_id', str(user_id)))

def is_owner(user_id):
    owner = get_owner_id()
    return owner is not None and user_id == owner

def get_pm_setting():
    row = db_fetchone("SELECT value FROM settings WHERE key = 'pm_enabled'")
    return row[0].lower() == 'true' if row else False

def set_pm_setting(enabled: bool):
    db_execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ('pm_enabled', str(enabled).lower()))

def add_chat(chat_id):
    db_execute("INSERT OR IGNORE INTO allowed_chats (chat_id) VALUES (?)", (chat_id,))

def remove_chat(chat_id):
    db_execute("DELETE FROM allowed_chats WHERE chat_id = ?", (chat_id,))

def get_allowed_chats():
    rows = db_fetchall("SELECT chat_id FROM allowed_chats")
    return {row[0] for row in rows}

def is_allowed_chat(chat_id):
    return chat_id in get_allowed_chats()

# --- Pastebin ---
def create_paste(text, title="Changelog/Notes"):
    logger.info("Creating Pastebin paste...")
    data = {
        'api_dev_key': PASTEBIN_API_KEY,
        'api_option': 'paste',
        'api_paste_code': text,
        'api_paste_private': '1', # 1 = Unlisted
        'api_paste_name': title,
        'api_paste_expire_date': 'N', # N = Never
        'api_paste_format': 'text'
    }
    try:
        response = requests.post('https://pastebin.com/api/api_post.php', data=data, timeout=10)
        response.raise_for_status() # Raise an exception for bad status codes
        if response.text.startswith("https://pastebin.com/"):
            logger.info(f"Pastebin link created: {response.text}")
            return response.text
        else:
            logger.error(f"Pastebin API error: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to connect to Pastebin API: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during Pastebin upload: {e}")
        return None

# --- Filename Fetching (Requests Method) ---
def get_filename_from_url(url):
    """
    Attempts to get the filename from URL headers or final URL path.
    """
    try:
        # Try HEAD request first (less data)
        with requests.head(url, allow_redirects=True, timeout=10) as response:
            response.raise_for_status()
            content_disposition = response.headers.get('content-disposition')
            if content_disposition:
                # Try to parse filename* first (handles encoding)
                filename_match = re.search(r"filename\*=UTF-8''(.+)", content_disposition, re.IGNORECASE)
                if filename_match:
                    filename = unquote(filename_match.group(1))
                    if filename.lower().endswith('.zip'):
                        logger.info(f"Filename from Content-Disposition (UTF-8): {filename}")
                        return filename, None
                # Fallback to filename=
                filename_match = re.search(r'filename="?([^"]+)"?', content_disposition, re.IGNORECASE)
                if filename_match:
                    filename = unquote(filename_match.group(1)) # Basic unquote
                    if filename.lower().endswith('.zip'):
                        logger.info(f"Filename from Content-Disposition: {filename}")
                        return filename, None

            # If no header, use the final URL path
            final_url = response.url
            parsed_url = urlparse(final_url)
            filename = os.path.basename(unquote(parsed_url.path))
            if filename and filename.lower().endswith('.zip'):
                 logger.info(f"Filename from final URL path: {filename}")
                 return filename, None
            else:
                 logger.warning(f"Could not determine valid zip filename from final URL: {final_url}")
                 return None, "Could not determine filename from URL path."

    except requests.exceptions.Timeout:
        logger.error(f"Timeout while fetching headers for URL: {url}")
        return None, "Timeout while fetching URL headers."
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching headers for URL {url}: {e}")
        # Don't try GET if HEAD failed significantly
        return None, f"Error fetching URL headers: {e}"
    except Exception as e:
        logger.error(f"Unexpected error fetching filename for {url}: {e}")
        return None, f"Unexpected error: {e}"

    # Fallback if HEAD didn't work or gave no filename (rarely needed if HEAD works)
    logger.info("HEAD request didn't yield filename, trying GET with stream...")
    try:
        with requests.get(url, stream=True, allow_redirects=True, timeout=10) as response:
            response.raise_for_status()
            content_disposition = response.headers.get('content-disposition')
            if content_disposition:
                 # Try to parse filename* first (handles encoding)
                filename_match = re.search(r"filename\*=UTF-8''(.+)", content_disposition, re.IGNORECASE)
                if filename_match:
                    filename = unquote(filename_match.group(1))
                    if filename.lower().endswith('.zip'):
                        logger.info(f"Filename from Content-Disposition (GET): {filename}")
                        return filename, None
                # Fallback to filename=
                filename_match = re.search(r'filename="?([^"]+)"?', content_disposition, re.IGNORECASE)
                if filename_match:
                    filename = unquote(filename_match.group(1))
                    if filename.lower().endswith('.zip'):
                        logger.info(f"Filename from Content-Disposition (GET): {filename}")
                        return filename, None

            # If no header, use the final URL path from GET
            final_url = response.url
            parsed_url = urlparse(final_url)
            filename = os.path.basename(unquote(parsed_url.path))
            if filename and filename.lower().endswith('.zip'):
                 logger.info(f"Filename from final URL path (GET): {filename}")
                 return filename, None
            else:
                 logger.warning(f"Could not determine valid zip filename from final URL (GET): {final_url}")
                 return None, "Could not determine filename from URL path after GET."

    except requests.exceptions.Timeout:
        logger.error(f"Timeout during GET stream for URL: {url}")
        return None, "Timeout while fetching URL."
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during GET stream for URL {url}: {e}")
        return None, f"Error fetching URL: {e}"
    except Exception as e:
        logger.error(f"Unexpected error fetching filename via GET for {url}: {e}")
        return None, f"Unexpected error during GET: {e}"

    return None, "Could not determine filename from URL headers or path."

# --- Filename Parsing ---
def parse_filename(filename):
    """
    Parse ROM filename to extract version, date, status, and variant.
    
    Guidelines:
    - First convert the filename to lowercase for consistent parsing
    - Timestamps can be 6+ digits but never less
    - Device name can be anywhere in the name
    - Official/Unofficial status can be anywhere too
    - Project name is typically the first part
    - Convert relevant parts to appropriate case afterward
    """
    logger.info(f"Attempting to parse filename: {filename}")
    
    # Clean the filename and convert to lowercase for consistent parsing
    clean_filename = re.sub(r'[*_]', '', filename).lower()
    
    # Split by hyphens to get the components
    parts = clean_filename.split('-')
    if len(parts) < 2:  # Need at least ROM name and one other component
        logger.warning(f"Not enough parts in filename: {filename}")
        return None
    
    # Initialize the result dictionary
    result = {
        "version": None,
        "build_date": None,
        "status": None,
        "variant_type": None,
        "device_name": None,
        "rom_name": None  # Add ROM name field
    }
    
    # FIRST PRIORITY: Extract the ROM name from the first part
    # This is now treated as a simple first-part extraction
    if parts[0].strip():
        rom_name = parts[0].strip()
        # Make each word capitalized for a nice display
        result["rom_name"] = ' '.join(word.capitalize() for word in rom_name.split())
        logger.info(f"Extracted ROM name from first part: {result['rom_name']}")
    
    # Known status keywords ordered by priority (keep lowercase for now)
    status_priority = [
        "official",    # Highest priority
        "unofficial",
        "community",
        "stable",
        "beta",
        "alpha",
        "rc",
        "nightly",     # Lower priority
        "experimental", 
        "test", 
        "enchanted"
    ]
    
    # Known variant keywords (keep lowercase for now)
    variant_keywords = ["gapps", "gms", "vanilla", "core", "lite", "full", "mini"]
    
    # Device codenames typically have 3-10 chars and no numbers at the start
    device_candidates = []
    
    # First look for version numbers containing periods (e.g., 2.5, 1.2.3)
    # Improved regex to better capture version numbers
    version_match = re.search(r'v?(\d+\.\d+(?:\.\d+)*)', clean_filename)
    if version_match:
        result["version"] = version_match.group(1)
        logger.info(f"Found version number with period: {result['version']}")
    
    # Track if we found a status with specific priorities
    found_status_priority = float('inf')  # Initialize with highest possible value
    
    # Look for all patterns in remaining parts
    for part in parts[1:]:  # Skip the first part (ROM name)
        clean_part = part.split('.')[0].strip()
        
        # Skip empty parts
        if not clean_part:
            continue
            
        # Check for date (6+ digits)
        if re.match(r'^\d{6,}$', clean_part):
            if result["build_date"] is None:  # Take the first match
                result["build_date"] = clean_part
                logger.info(f"Found timestamp: {clean_part}")
            continue
            
        # Check for version (if not already found) - has v prefix or is just digits
        if result["version"] is None and ((clean_part.startswith('v') and any(c.isdigit() for c in clean_part)) or 
            re.match(r'^\d+$', clean_part)):
            # Remove v prefix if present
            result["version"] = clean_part.lstrip('v')
            logger.info(f"Found version: {result['version']}")
            continue
            
        # Check for status
        # Scan for any status keywords in the part
        for i, status in enumerate(status_priority):
            if status in clean_part:
                # If this status has higher priority (lower index) than any previously found
                if i < found_status_priority:
                    found_status_priority = i
                    result["status"] = status
                    logger.info(f"Found status: {status} (priority: {i})")
                continue
            
        # Check for variant
        for variant in variant_keywords:
            if variant in clean_part:
                if result["variant_type"] is None:
                    result["variant_type"] = variant
                    logger.info(f"Found variant: {variant}")
                    break
        
        # Potential device name (3-10 chars, not purely numeric)
        # We want to find typical device codenames like "vayu", "cancunf", etc.
        if (3 <= len(clean_part) <= 10 and 
            not clean_part.isdigit() and 
            not any(status in clean_part for status in status_priority) and 
            not any(variant in clean_part for variant in variant_keywords) and
            re.match(r'^[a-z][a-z0-9]{2,9}$', clean_part)):
            device_candidates.append(clean_part)
            logger.info(f"Found device candidate: {clean_part}")
    
    # Process date if found
    if result["build_date"]:
        try:
            # Format depends on length
            if len(result["build_date"]) == 8:  # YYYYMMDD
                date_obj = datetime.strptime(result["build_date"], '%Y%m%d')
            elif len(result["build_date"]) == 6:  # YYMMDD
                date_obj = datetime.strptime(result["build_date"], '%y%m%d')
            else:
                # Best effort, treat as YYYYMMDD and take last 8 chars
                date_str = result["build_date"][-8:] if len(result["build_date"]) > 8 else result["build_date"]
                date_obj = datetime.strptime(date_str, '%Y%m%d')
                
            # Standard format for display
            result["build_date"] = date_obj.strftime('%d/%m/%y')
            logger.info(f"Formatted date: {result['build_date']}")
        except ValueError:
            logger.warning(f"Could not parse date: {result['build_date']}")
    
    # Fill in missing values and properly capitalize
    
    # If no version found, look for digits in a part
    if result["version"] is None:
        for part in parts:
            if any(c.isdigit() for c in part):
                result["version"] = part
                logger.info(f"Using alternative version: {part}")
                break
    
    # If still no version, ask user later
    if result["version"] is None:
        result["version"] = "Unknown"
    
    # If no build date, use today
    if result["build_date"] is None:
        result["build_date"] = datetime.now().strftime('%d/%m/%y')
        logger.info(f"Using current date: {result['build_date']}")
    
    # If no status found, use Unofficial
    if result["status"] is None:
        result["status"] = "unofficial"
        logger.info("No status found, defaulting to unofficial")
    
    # Properly capitalize status
    result["status"] = result["status"].capitalize()
    
    # If no variant found, use Standard
    if result["variant_type"] is None:
        result["variant_type"] = "standard"
        logger.info("No variant found, defaulting to standard")
    
    # Uppercase variant type
    result["variant_type"] = result["variant_type"].upper()
    
    # Process device name candidates more carefully
    if device_candidates:
        # First try to find a device name that matches common patterns
        for candidate in device_candidates:
            # Common device naming patterns like x00td, cancunf, etc.
            if re.match(r'^[a-z][a-z0-9]{2,9}$', candidate):
                result["device_name"] = candidate
                logger.info(f"Identified likely device name: {candidate}")
                break
        
        # If no specific match was found, use the last candidate as fallback
        if not result["device_name"] and device_candidates:
            result["device_name"] = device_candidates[-1]
            logger.info(f"Using last device candidate: {device_candidates[-1]}")
    
    logger.info(f"Parsed filename data: {result}")
    return result

# --- Text Formatting ---
def format_bullets(text):
    if not text or not isinstance(text, str):
        return ""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    # Ensure the first letter is capitalized after the bullet
    bullets = [f"• {line[0].upper() + line[1:] if line else ''}" for line in lines]
    return "\n".join(bullets)

# --- Post Formatting Templates ---
POST_FORMATS = {
    "default": """
**{rom_name} - {status} | v{version} | Android {android_version}**

Updated: {build_date}
Build Variant: {variant_names}

▪️Download: {download_links}{optional_links}{changelogs_section}{bugs_section}{notes_section}{checksums_section}{credits_section}\n
By: {maintainer_mention}
""",
    "minimal": """
**{rom_name} v{version} is now up for {device_name}**!💫
Build Date: {build_date}
Maintainer: {maintainer_name}
{changelogs_minimal}
Download: {download_links_minimal}{support_group_minimal}{bugs_section}{notes_section}{checksums_section}{credits_section}
"""
# Removed some potentially problematic blank lines between placeholders
}

def generate_post_text(format_name, data):
    """Generates the post text based on the chosen format and data."""
    template = POST_FORMATS.get(format_name, POST_FORMATS["default"]) # Default to 'default'

    # --- Prepare data for formatting ---
    android_version = data.get("android_version", "15") # Default or fetched version
    variant_names = ' & '.join([v['name'] for v in data["variants"]])

    # Download links (Default format)
    download_links_default = ' | '.join([f"[{v['name']}]({v['link']})" for v in data["variants"]])

    # Download links (Minimal format)
    download_links_minimal = ' | '.join([f"[{v['name']}]({v['link']})" for v in data["variants"]]) # Same links, different context maybe

    # Optional Links (Default format) - Add newline prefix only if content exists
    optional_links_list = []
    if data.get("support_group"):
        optional_links_list.append(f"▪️[Support]({data['support_group']})")
    if data.get("screenshots"):
        optional_links_list.append(f"▪️[Screenshots]({data['screenshots']})")
    optional_links_str = ""
    if optional_links_list:
        optional_links_str = "\n" + "\n".join(optional_links_list) # Add newline prefix

    # Changelogs (Default format) - Add newline prefix/suffix only if content exists
    changelog_links_default = []
    if data.get("device_changelog"):
        changelog_links_default.append(f"[Device]({data['device_changelog']})")
    if data.get("source_changelog"):
        changelog_links_default.append(f"[Source]({data['source_changelog']})")
    changelogs_section_str = ""
    if changelog_links_default:
        changelogs_section_str = f"\n\nChangelog:\n• {' | '.join(changelog_links_default)}" # Add newline prefix
    elif data.get("device_changelog") or data.get("source_changelog"): # Handle pastebin fail case
        changelogs_section_str = "\n\nChangelog:" # Add newline prefix
        if data.get("device_changelog"): changelogs_section_str += f"\n• Device: {data['device_changelog']}"
        if data.get("source_changelog"): changelogs_section_str += f"\n• Source: {data['source_changelog']}"

    # Changelogs (Minimal format) - Add newline prefix only if content exists
    changelogs_minimal_list = []
    if data.get("source_changelog"):
        changelogs_minimal_list.append(f"[Source]({data['source_changelog']})")
    if data.get("device_changelog"): # Add device if available
         changelogs_minimal_list.append(f"[Device]({data['device_changelog']})")
    changelogs_minimal_str = ""
    if changelogs_minimal_list:
        changelogs_minimal_str = f"\nChangelogs: {' | '.join(changelogs_minimal_list)}" # Add newline prefix


    # Bugs (Default format) - Always included for now, add newline prefix
    # If this section should also be optional, it needs a condition like the others
    bugs_section_str = "\n\nBugs:\n• Report issues with send adb logcat" # Add newline prefix

    # Notes (Default format) - Add newline prefix only if content exists
    notes_section_str = ""
    if data.get("notes"):
        notes_section_str = f"\n\nNotes:\n{data['notes']}" # Add newline prefix

    # Notes (Minimal format) - Add newline prefix only if content exists
    notes_minimal_str = ""
    if data.get("notes"):
        # Remove bullet points for minimal format if they exist
        plain_notes = "\n".join([line.replace("• ","", 1) for line in data['notes'].split('\n')])
        notes_minimal_str = f"\nNotes:\n{plain_notes}" # Add newline prefix


    # Checksums (Default format) - Add newline prefix only if content exists
    checksums_section_str = ""
    checksum_lines = []
    for v in data["variants"]:
        if v.get("sha256"):
            checksum_lines.append(f"   {v['name']}: `{v['sha256']}`")
    if checksum_lines:
        checksums_section_str = "\n\n• MD5/SHA256:\n" + "\n".join(checksum_lines) # Add newline prefix

    # Credits (Default format) - Add newline prefix only if content exists
    credits_section_str = ""
    if data.get("credits"):
        credits_section_str = f"\n\nCredits:\n{data['credits']}" # Add newline prefix

    # Support Group (Minimal format) - Add newline prefix only if content exists
    support_group_minimal_str = ""
    if data.get("support_group"):
        support_group_minimal_str = f"\n[Support Group]({data['support_group']})" # Add newline prefix

    # --- Apply formatting ---
    try:
        # Use a dictionary to hold all format values, including the potentially empty section strings
        format_data = {
            "rom_name": data.get("rom_name", "Unknown ROM"),
            "status": data.get("status", "Unofficial"),
            "version": data.get("version", "N/A"),
            "android_version": android_version,
            "build_date": data.get("build_date", "Unknown Date"),
            "variant_names": variant_names,
            "download_links": download_links_default,
            "download_links_minimal": download_links_minimal,
            "optional_links": optional_links_str, # Will be "" if empty
            "changelogs_section": changelogs_section_str, # Will be "" if empty
            "changelogs_minimal": changelogs_minimal_str, # Will be "" if empty
            "bugs_section": bugs_section_str, # Will be "" if empty (if made conditional)
            "notes_section": notes_section_str, # Will be "" if empty
            "notes_minimal": notes_minimal_str, # Will be "" if empty
            "checksums_section": checksums_section_str, # Will be "" if empty
            "credits_section": credits_section_str, # Will be "" if empty
            "maintainer_mention": data.get("maintainer_mention", "Unknown Maintainer"),
            "maintainer_name": data.get("maintainer_name", "Unknown Maintainer"), # For minimal
            "device_name": data.get("device_name", "Unknown Device"), # For minimal
            "support_group_minimal": support_group_minimal_str # Will be "" if empty
        }
        # Use .format with the dictionary - missing keys won't be an issue if placeholders are removed/handled
        formatted_text = template.format(**format_data)
        # Strip leading/trailing whitespace from the final result
        return formatted_text.strip()
    except KeyError as e:
        # This error is less likely now, but kept for safety
        logger.error(f"Missing key in post_data for format '{format_name}': {e}")
        return f"Error: Could not generate post. Missing data: {e}"
    except Exception as e:
        logger.error(f"Error formatting post with format '{format_name}': {e}")
        return f"Error: Could not generate post due to formatting error: {e}"


# --- Telethon Client ---
init_db() # Initialize DB before client starts
client = TelegramClient(StringSession(), API_ID, API_HASH)
active_conversations = {} # Dictionary to store active convs: {(chat_id, user_id): conv_object}

# --- Event Handlers ---

@client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    sender = await event.get_sender()
    sender_id = sender.id
    current_owner = get_owner_id()

    if current_owner is None:
        set_owner_id(sender_id)
        await event.reply(f"Hello {get_display_name(sender)}! You have been set as the bot owner. Use /help for commands.")
        logger.info(f"Owner automatically set to {sender_id}")
    elif is_owner(sender_id):
        await event.reply(f"Welcome back, owner {get_display_name(sender)}! Use /help for commands.")
    else:
        await event.reply("Hello! I'm a post maker bot.")

@client.on(events.NewMessage(pattern='/help'))
async def help_handler(event):
    sender_id = event.sender_id
    if is_owner(sender_id):
        await event.reply(
            "**Owner Commands:**\n"
            "/new - Start creating a new post.\n"
            "/cancel - Cancel current post creation.\n"
            "/cancel <user_id> - Cancel post creation for a specific user.\n"
            "/pmon - Allow bot interaction in PM.\n"
            "/pmoff - Disallow bot interaction in PM.\n"
            "/addchat <chat_id> - Allow bot usage in a specific chat.\n"
            "/delchat <chat_id> - Disallow bot usage in a specific chat.\n"
            "/listchats - List allowed chat IDs.\n"
            "/setowner <user_id> - Manually set a new owner (use with caution).\n"
            "\n**Preset Management:**\n"
            "/addpreset - Add/Update a ROM preset interactively.\n"
            "/delpreset <name> - Delete a ROM preset.\n"
            "/listpresets - List all ROM presets with details.\n"
            "/showpreset <name> - Show details for a specific preset.\n"
            "\n**Device Index:**\n"
            "/listdevices - Browse available ROMs by device.\n"
            "/search <term> - Search for ROMs by device or ROM name.\n"
            "/updateindex - Check and remove missing posts from the index.\n"
            "\n**User Management:**\n"
            "/ban <user_id> [reason] - Ban a user from using the bot.\n"
            "/unban <user_id> - Unban a user.\n"
            "/listbanned - List all banned users.\n"
            "/topusers - Show most active users.\n"
            "/botstats - Show bot statistics."
        )
    else:
        await event.reply(
            "**Available Commands:**\n"
            "/new - Create a new ROM post (limit: 2 per day).\n"
            "/cancel - Cancel current post creation.\n"
            "/listdevices - Browse available ROMs by device.\n"
            "/search <term> - Search for ROMs by device or ROM name.\n"
            "/topusers - Show most active users."
        )

# --- Owner Commands ---
async def owner_command(event, command_func, success_msg, error_msg="An error occurred.", requires_arg=False):
    sender_id = event.sender_id
    if not is_owner(sender_id):
        await event.reply("⛔️ You are not authorized to use this command.")
        return

    if requires_arg:
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await event.reply(error_msg)
            return
        arg_str = args[1]
    else:
        arg_str = None

    try:
        result = command_func(arg_str) if requires_arg else command_func()
        if isinstance(result, str):
            await event.reply(result)
        elif result:
            await event.reply(success_msg)
        else:
            await event.reply(error_msg)
    except Exception as e:
        logger.error(f"Error executing owner command: {e}")
        await event.reply(error_msg)

@client.on(events.NewMessage(pattern='/pmon'))
async def pmon_handler(event):
    await owner_command(event, lambda: set_pm_setting(True), "PM interactions enabled.")

@client.on(events.NewMessage(pattern='/pmoff'))
async def pmoff_handler(event):
     await owner_command(event, lambda: set_pm_setting(False), "PM interactions disabled.")

@client.on(events.NewMessage(pattern='/addchat'))
async def addchat_handler(event):
    def add_chat_wrapper(chat_id_str):
        try:
            chat_id = int(chat_id_str)
            add_chat(chat_id)
            return True
        except ValueError:
            return False
    await owner_command(event, add_chat_wrapper, "Chat added to allowed list.", "Invalid Chat ID.", requires_arg=True)

@client.on(events.NewMessage(pattern='/delchat'))
async def delchat_handler(event):
    def remove_chat_wrapper(chat_id_str):
        try:
            chat_id = int(chat_id_str)
            remove_chat(chat_id)
            return True
        except ValueError:
            return False
    await owner_command(event, remove_chat_wrapper, "Chat removed from allowed list.", "Invalid Chat ID.", requires_arg=True)

@client.on(events.NewMessage(pattern='/listchats'))
async def listchats_handler(event):
    def list_chats_wrapper():
        chats = get_allowed_chats()
        if not chats:
            return "No chats are currently allowed."
        else:
            return "Allowed Chat IDs:\n" + "\n".join(f"`{chat_id}`" for chat_id in chats)
    await owner_command(event, list_chats_wrapper, "") # Success message is generated by the function

@client.on(events.NewMessage(pattern='/setowner'))
async def setowner_handler(event):
    def set_owner_wrapper(user_id_str):
        try:
            user_id = int(user_id_str)
            set_owner_id(user_id)
            logger.warning(f"Owner manually changed to {user_id} by {event.sender_id}")
            return True
        except ValueError:
            return False
    await owner_command(event, set_owner_wrapper, "Bot owner updated.", "Invalid User ID.", requires_arg=True)

# --- Preset Owner Commands ---
@client.on(events.NewMessage(pattern='/addpreset'))
async def addpreset_handler(event):
    sender_id = event.sender_id
    chat_id = event.chat_id
    conv_key = (chat_id, sender_id)
    
    # Check if owner
    if not is_owner(sender_id):
        await event.reply("⛔️ You are not authorized to use this command.")
        return
    
    # Check if already in conversation
    if conv_key in active_conversations:
        await event.reply("Please finish your current session first or use /cancel.")
        return
    
    # Define a smaller timeout for this command
    PRESET_TIMEOUT = 300
    
    try:
        async with client.conversation(chat_id, timeout=PRESET_TIMEOUT) as conv:
            active_conversations[conv_key] = conv
            
            await conv.send_message("Let's create a new ROM preset!\n\n**Step 1/3**: What is the preset name? (This will be used to select the preset later)")
            preset_name_msg = await conv.get_response()
            preset_name = preset_name_msg.text.strip().lower()
            
            if not preset_name:
                await conv.send_message("Invalid preset name. Command cancelled.")
                return
                
            existing = get_preset(preset_name)
            if existing:
                confirm_msg = await conv.send_message(
                    f"A preset named '{preset_name}' already exists.\nDo you want to update it?",
                    buttons=[Button.inline("Yes", data=b"update_yes"), Button.inline("No", data=b"update_no")]
                )
                response = await conv.wait_event(
                    events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == confirm_msg.id)
                )
                await response.answer()
                
                if response.data == b"update_no":
                    await response.edit("Preset creation cancelled.")
                    return
                await response.edit(f"Updating preset '{preset_name}'")
            
            await conv.send_message("**Step 2/3**: What is the full ROM name? (e.g., 'PixelExperience', 'Axion AOSP')")
            rom_name_msg = await conv.get_response()
            rom_name = rom_name_msg.text.strip()
            
            if not rom_name:
                await conv.send_message("Invalid ROM name. Command cancelled.")
                return
                
            source_cl_msg = await conv.send_message(
                "**Step 3/3**: Provide the source changelog link (or type 'none' if not available)",
                buttons=[Button.inline("None", data=b"no_changelog")]
            )
            
            # Use a similar approach to the main post creation to handle both message and button
            async def get_cl_response():
                try:
                    button_response = asyncio.create_task(
                        conv.wait_event(events.CallbackQuery(
                            func=lambda e: e.sender_id == sender_id and e.message_id == source_cl_msg.id
                        ))
                    )
                    text_response = asyncio.create_task(conv.get_response())
                    
                    done, pending = await asyncio.wait(
                        {button_response, text_response},
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # Cancel pending task
                    for task in pending:
                        task.cancel()
                    
                    # Get result from completed task
                    result = done.pop().result()
                    
                    if isinstance(result, events.CallbackQuery.Event):
                        await result.answer()
                        return "none"
                    else:
                        return result.text.strip()
                except Exception as e:
                    logger.error(f"Error in get_cl_response: {e}")
                    return None
            
            source_cl = await get_cl_response()
            
            if source_cl is None:
                await conv.send_message("Error receiving response. Command cancelled.")
                return
            
            # Add the preset
            add_preset(preset_name, rom_name, source_cl)
            
            # Confirm
            action = "updated" if existing else "saved"
            await conv.send_message(f"✅ Preset '{preset_name}' {action} successfully!\n\n**Details:**\n"
                                    f"- ROM Name: **{rom_name}**\n"
                                    f"- Source Changelog: {source_cl if source_cl.lower() != 'none' else 'None'}")
            
    except asyncio.TimeoutError:
        await event.reply("Command timed out. Please try again.")
    except AlreadyInConversationError:
        await event.reply("You already have an active session. Please finish it first or use /cancel.")
    except Exception as e:
        await event.reply(f"An error occurred: {e}")
        logger.exception(f"Error in addpreset_handler for {sender_id} in {chat_id}: {e}")
    finally:
        if conv_key in active_conversations:
            del active_conversations[conv_key]

@client.on(events.NewMessage(pattern='/delpreset'))
async def delpreset_handler(event):
    def delete_preset_wrapper(preset_name):
        preset_name_lower = preset_name.lower() # Use lowercase
        if not get_preset(preset_name_lower): # Check if preset exists
             return f"Preset '{preset_name_lower}' not found."
        delete_preset(preset_name_lower)
        return f"Preset '{preset_name_lower}' deleted."
    # Use lowercase preset_name for consistency
    await owner_command(event, lambda name: delete_preset_wrapper(name), "", error_msg="Usage: /delpreset <preset_name>", requires_arg=True)


@client.on(events.NewMessage(pattern='/listpresets'))
async def listpresets_handler(event):
    def list_presets_wrapper():
        presets_list = db_fetchall("SELECT preset_name, rom_name, source_changelog FROM rom_presets ORDER BY preset_name")
        if not presets_list:
            return "No presets saved yet. Use /addpreset."
        else:
            lines = ["**Saved Presets:**"]
            for name, rom, cl in presets_list:
                cl_display = f"([Link]({cl}))" if cl else "(None)"
                lines.append(f"- `{name}`: **{rom}** | Source CL: {cl_display}")
            return "\n".join(lines)
    # Use lowercase preset_name for consistency
    await owner_command(event, list_presets_wrapper, "") # Success message generated by wrapper

@client.on(events.NewMessage(pattern='/showpreset'))
async def showpreset_handler(event):
    def show_preset_wrapper(preset_name):
        preset_name_lower = preset_name.lower() # Use lowercase
        preset_data = get_preset(preset_name_lower)
        if not preset_data:
             return f"Preset '{preset_name_lower}' not found."
        rom_name, source_cl = preset_data
        cl_display = f"([Link]({source_cl}))" if source_cl else "(None)"
        return (f"**Preset Details: `{preset_name_lower}`**\n"
                f"- ROM Name: **{rom_name}**\n"
                f"- Source Changelog: {cl_display}")
    # Use lowercase preset_name for consistency
    await owner_command(event, lambda name: show_preset_wrapper(name), "", error_msg="Usage: /showpreset <preset_name>", requires_arg=True)


# --- Cancel Command ---
@client.on(events.NewMessage(pattern='/cancel'))
async def cancel_handler(event):
    sender_id = event.sender_id
    chat_id = event.chat_id
    conv_key = (chat_id, sender_id)
    
    # Check if this is a cancel request for another user's session (owner only)
    args = event.message.text.split(maxsplit=1)
    target_user_id = None
    
    if len(args) > 1 and is_owner(sender_id):
        try:
            target_user_id = int(args[1])
            logger.info(f"Owner {sender_id} attempting to cancel session for user {target_user_id}")
        except ValueError:
            await event.reply("Invalid user ID format. Usage: /cancel [user_id]")
            return
    
    # If owner is cancelling someone else's session
    if target_user_id and is_owner(sender_id):
        # Look for any active conversation with this user
        target_found = False
        for c_key, conv in list(active_conversations.items()):
            chat, user = c_key
            if user == target_user_id:
                try:
                    # First check if the conversation object is valid before attempting to cancel
                    if conv is not None:
                        try:
                            await conv.cancel()
                        except Exception as cancel_err:
                            logger.error(f"Error cancelling conversation: {cancel_err}")
                    else:
                        logger.warning(f"Conversation object is None for user {target_user_id} in chat {chat}")
                    
                    # Remove from active conversations
                    del active_conversations[c_key]
                    await event.reply(f"✅ Cancelled session for user {target_user_id} in chat {chat}")
                    target_found = True
                    logger.info(f"Owner {sender_id} cancelled conversation for user {target_user_id} in chat {chat}")
                except Exception as e:
                    logger.error(f"Error when owner cancelled conversation: {e}")
                    await event.reply(f"❌ Error cancelling session for user {target_user_id}: {e}")
        
        if not target_found:
            await event.reply(f"User {target_user_id} doesn't have any active sessions.")
        return
    
    # Regular self-cancellation
    try:
        if conv_key in active_conversations:
            conv = active_conversations[conv_key]
            # Check if conversation object is valid before attempting to cancel
            if conv is not None:
                try:
                    await conv.cancel()
                    await event.reply("✅ Current post creation process cancelled.")
                    logger.info(f"Conversation cancelled by user {sender_id} in chat {chat_id}")
                except Exception as cancel_err:
                    logger.error(f"Error during conversation cancellation: {cancel_err}")
                    # Still consider it cancelled since we'll remove it anyway
                    await event.reply("✅ Current post creation process cancelled (with warnings).")
            else:
                logger.warning(f"Conversation object is None for user {sender_id} in chat {chat_id}")
                await event.reply("✅ Current post creation process cancelled (conversation was invalid).")
            
            # Remove from active conversations regardless of cancel success
            del active_conversations[conv_key]
        else:
            await event.reply("ℹ️ You don't have an active post creation process to cancel.")
            return
    except Exception as e:
        error_msg = f"Error cancelling conversation: {str(e)}"
        logger.error(f"Error cancelling conversation for {sender_id} in {chat_id}: {e}")
        await event.reply(f"❌ {error_msg}")
        
        # Still try to clean up if possible
        try:
            if conv_key in active_conversations:
                del active_conversations[conv_key]
                await event.reply("✅ Removed from active conversations.")
        except Exception as cleanup_err:
            logger.error(f"Error during cleanup after cancel failure: {cleanup_err}")

# --- /new Command ---
@client.on(events.NewMessage(pattern='/new'))
async def new_post_handler(event):
    sender = await event.get_sender()
    chat_id = event.chat_id
    sender_id = sender.id
    conv_key = (chat_id, sender_id)

    # Check if user is banned
    if is_banned(sender_id):
        await event.reply("⛔️ You are banned from using this bot.")
        return

    # Permission Check
    is_pm = event.is_private
    pm_enabled = get_pm_setting()
    allowed_chat = is_allowed_chat(chat_id)

    if not (allowed_chat or (is_pm and pm_enabled)):
        if not is_owner(sender_id): # Allow owner to use /new anywhere for testing/convenience
             await event.reply("⛔️ This command can only be used in allowed chats or in PM (if enabled).")
             return
    
    # Check daily post limit (skip for owners)
    if not is_owner(sender_id):
        can_post, remaining = check_post_limit(sender_id)
        if not can_post:
            await event.reply("⛔️ You have reached your daily post limit (2 posts per day). Please try again tomorrow.")
            return
        else:
            # Show how many posts they have left
            await event.reply(f"You have {remaining} post(s) remaining today.")

    # Check if already in conversation
    if conv_key in active_conversations:
         await event.reply("You already have an active post creation session. Please finish or cancel it first using /cancel.")
         logger.warning(f"User {sender_id} tried to start /new while already in conversation in chat {chat_id}")
         return

    logger.info(f"/new command initiated by {sender_id} in chat {chat_id}")

    # Update user stats with available info
    update_user_stats(
        sender_id, 
        sender.username if hasattr(sender, 'username') else None,
        sender.first_name if hasattr(sender, 'first_name') else None,
        sender.last_name if hasattr(sender, 'last_name') else None
    )

    # Fetch existing user data and presets
    saved_data = get_user_data(sender_id)
    saved_support = saved_data[0] if saved_data else None
    saved_notes = saved_data[1] if saved_data else None
    saved_credits = saved_data[2] if saved_data else None
    saved_device_cl = saved_data[3] if saved_data else None
    available_presets = list_presets()

    post_data = {
        "variants": [], # List of {"name": str, "link": str, "sha256": str | None}
        "support_group": None,
        "screenshots": None,
        "device_changelog": None, # Link or Text
        "source_changelog": None, # Link or Text
        "notes": None,
        "credits": None,
        "maintainer_name": get_display_name(sender),
        "maintainer_mention": f"[{get_display_name(sender)}](tg://user?id={sender_id})",
        # Parsed from filename or asked
        "rom_name": None,
        "version": None,
        "build_date": None,
        "status": None,
        "device_name": None, # Added for minimal format & indexing
        "android_version": "15" # Default, can be updated
    }
    use_preset_data = None # To store fetched preset data if used

    # Define the timeout value
    CONVERSATION_TIMEOUT = 600
    temp_banner_paths = [] # Keep track of temporary banner files

    # Helper function to wait for message or callback query (REVISED - Pass Timeout)
    async def get_response_or_callback(message_with_buttons, timeout):
        """Waits for either a text response or a callback query related to the message using temporary handlers."""
        # Use an asyncio.Queue to pass the result from the handler
        queue = asyncio.Queue(maxsize=1)
        # Use an asyncio.Event to signal completion/timeout/cancellation
        done_event = asyncio.Event()

        async def handler(evt):
            # Check if the event is relevant (correct user, chat, and message for callbacks)
            if isinstance(evt, events.NewMessage.Event):
                if evt.chat_id == chat_id and evt.sender_id == sender_id:
                    await queue.put(evt.message) # Put the Message object
                    done_event.set() # Signal completion
            elif isinstance(evt, events.CallbackQuery.Event):
                if evt.sender_id == sender_id and evt.message_id == message_with_buttons.id:
                    await queue.put(evt) # Put the CallbackQuery event
                    done_event.set() # Signal completion

        # Add the temporary handler for both event types
        client.add_event_handler(handler, events.NewMessage(incoming=True, chats=chat_id))
        client.add_event_handler(handler, events.CallbackQuery(chats=chat_id))

        result = None
        try:
            # Wait for either the handler to signal completion or the specified timeout
            await asyncio.wait_for(done_event.wait(), timeout=timeout) # Use passed timeout
            # Retrieve the result from the queue
            result = await queue.get()

            # Process the result (similar to before)
            if isinstance(result, events.CallbackQuery.Event):
                await result.answer()
                try:
                    await result.edit(f"Choice: {result.data.decode()}")
                except MessageNotModifiedError: pass
                except Exception as edit_err: logger.warning(f"Could not edit button message: {edit_err}")
                return result # Return the CallbackQuery event
            elif result: # Should be a Message object
                return result # Return the Message object
            else:
                # Should not happen if done_event was set
                logger.error("done_event set but no result in queue in get_response_or_callback")
                return None

        except asyncio.TimeoutError:
            logger.warning(f"Timeout in get_response_or_callback waiting for user {sender_id}")
            raise # Re-raise timeout to be handled by the main conversation block
        except Exception as e:
            logger.error(f"Error in get_response_or_callback: {e}")
            raise # Re-raise other exceptions
        finally:
            # VERY IMPORTANT: Remove the temporary handlers to avoid memory leaks and conflicts
            client.remove_event_handler(handler, events.NewMessage)
            client.remove_event_handler(handler, events.CallbackQuery)
            # Ensure the done_event is set if not already (e.g., due to exception)
            # to prevent potential deadlocks if something else was waiting on it.
            if not done_event.is_set():
                done_event.set()


    try:
        # Use exclusive=True to ensure only one conversation per user/chat
        async with client.conversation(chat_id, timeout=CONVERSATION_TIMEOUT, exclusive=True) as conv:
            # The 'conv' object is still needed for simple get_response/wait_event calls
            active_conversations[conv_key] = conv # Store the conversation

            # --- Preset Selection (Optional) ---
            # This still uses conv.wait_event directly, which is fine as it's only waiting for one type.
            preset_chosen = False
            if available_presets:
                preset_buttons = [[Button.inline(name, data=f"preset_{name}")] for name in available_presets]
                preset_buttons.append([Button.inline("Manual Entry", data="preset_manual")]) # Add manual option

                preset_msg = await conv.send_message(
                    "Do you want to use a saved ROM preset?",
                    buttons=preset_buttons
                )
                preset_choice = await conv.wait_event(
                    events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == preset_msg.id)
                )
                await preset_choice.answer()

                if preset_choice.data != b'preset_manual':
                    preset_name = preset_choice.data.decode().split('_', 1)[1]
                    use_preset_data = get_preset(preset_name)
                    if use_preset_data:
                        post_data["rom_name"] = use_preset_data[0]
                        post_data["source_changelog"] = use_preset_data[1] # Can be None
                        preset_chosen = True
                        await preset_choice.edit(f"Using preset: {preset_name}\nROM: {post_data['rom_name']}\nSource CL: {post_data['source_changelog'] or 'None'}")
                    else:
                        await preset_choice.edit(f"Error: Preset '{preset_name}' not found. Proceeding with manual entry.")
                else:
                    await preset_choice.edit("Proceeding with manual entry.")
            # --- End Preset Selection ---


            # 1. ROM Link
            await conv.send_message("Okay, let's create a new post.\n\nPlease send the link for the main ROM zip file.")
            link_response = await conv.get_response()
            main_link = link_response.text.strip()
            if not main_link.startswith(('http://', 'https://')):
                await conv.send_message("That doesn't look like a valid link. Please start again with /new.")
                return

            # 2. Filename Fetching/Input (Automatic First)
            filename = None
            await conv.send_message("⏳ Attempting to fetch filename automatically...")
            fetched_filename, error_msg = get_filename_from_url(main_link) # Use new function

            if fetched_filename:
                filename = fetched_filename
                await conv.send_message(f"✅ Automatically fetched filename: `{filename}`")
            else:
                await conv.send_message(f"⚠️ Failed to fetch filename automatically. Reason: {error_msg}\nPlease provide the filename manually (e.g., `rom-version-date-variant.zip`).")
                filename_response = await conv.get_response()
                filename = filename_response.text.strip()

            # Validate filename format
            if not filename or not filename.lower().endswith('.zip'):
                 await conv.send_message("Invalid filename (must end with `.zip`). Please start again with /new.")
                 return

            # 3. Parse Filename
            parsed_info = parse_filename(filename)
            if not parsed_info:
                parsed_info = {
                    "version": None,
                    "build_date": datetime.now().strftime('%d/%m/%y'),
                    "status": "Unofficial",
                    "variant_type": None,
                    "device_name": None,
                    "rom_name": None
                }

            # Ask for any missing required information
            if parsed_info.get("version") is None or parsed_info["version"] == "Unknown":
                await conv.send_message("What's the ROM version? (e.g., 1.0, 2.5)")
                version_resp = await conv.get_response()
                parsed_info["version"] = version_resp.text.strip()
            
            # Always ask for device name if not detected
            if not parsed_info.get("device_name"):
                await conv.send_message("What's the device codename for this ROM? (e.g., 'cancunf', 'vayu')")
                device_resp = await conv.get_response()
                parsed_info["device_name"] = device_resp.text.strip().lower()
            
            # If status is missing, ask
            if not parsed_info.get("status"):
                status_buttons = [
                    [Button.inline("Official", data=b"status_Official")],
                    [Button.inline("Unofficial", data=b"status_Unofficial")],
                    [Button.inline("Alpha", data=b"status_Alpha")],
                    [Button.inline("Beta", data=b"status_Beta")]
                ]
                status_msg = await conv.send_message("What's the ROM status?", buttons=status_buttons)
                status_resp = await conv.wait_event(
                    events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == status_msg.id)
                )
                await status_resp.answer()
                parsed_info["status"] = status_resp.data.decode('utf-8').split('_')[1]
            
            # Always ask for variant regardless of whether it was parsed or not
            variant_buttons = [
                [Button.inline("GApps", data=b"variant_GAPPS")],
                [Button.inline("Vanilla", data=b"variant_VANILLA")],
                [Button.inline("Standard", data=b"variant_STANDARD")],
                [Button.inline("Custom", data=b"variant_CUSTOM")]
            ]
            variant_msg = await conv.send_message(
                f"What's the ROM variant?{' (Parsed: '+parsed_info.get('variant_type')+')' if parsed_info.get('variant_type') else ''}",
                buttons=variant_buttons
            )
            variant_resp = await conv.wait_event(
                events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == variant_msg.id)
            )
            await variant_resp.answer()
            
            variant_type = variant_resp.data.decode('utf-8').split('_')[1]
            if (variant_type == "CUSTOM"):
                await conv.send_message("Enter your custom variant name:")
                custom_variant = await conv.get_response()
                variant_type = custom_variant.text.strip().upper()
            
            parsed_info["variant_type"] = variant_type
            
            post_data.update(parsed_info)
            # Store the first variant
            post_data["variants"].append({
                "name": parsed_info["variant_type"],
                "link": main_link,
                "sha256": None
            })

            # 4. ROM Name (Skip if preset used, otherwise ask to confirm or change detected ROM name)
            if not preset_chosen:
                if parsed_info.get("rom_name"):
                    # Ask user to confirm or change the parsed ROM name
                    rom_name_msg = await conv.send_message(
                        f"Detected ROM name: `{parsed_info['rom_name']}`\nDo you want to use this name?",
                        buttons=[
                            [Button.inline("Yes, use this name", data=b"use_parsed_rom_name")],
                            [Button.inline("No, I'll enter a different name", data=b"enter_new_rom_name")]
                        ]
                    )
                    rom_name_resp = await conv.wait_event(
                        events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == rom_name_msg.id)
                    )
                    await rom_name_resp.answer()
                    
                    if rom_name_resp.data == b"enter_new_rom_name":
                        # Show parsed info and ask for ROM name
                        await conv.send_message(f"Parsed Info:\nVersion: `{post_data['version']}`\nDate: `{post_data['build_date']}`\nStatus: `{post_data['status']}`\nDevice: `{post_data['device_name']}`\nInitial Variant: `{post_data['variants'][0]['name']}`\n\nWhat is the **full ROM Name** (e.g., `Axion Aosp`, `PixelExperience`)?")
                        rom_name_response = await conv.get_response()
                        post_data["rom_name"] = rom_name_response.text.strip()
                    else:
                        # Keep the parsed ROM name
                        await rom_name_resp.edit(f"Using ROM name: {parsed_info['rom_name']}")
                else:
                    # No ROM name was parsed, show parsed info and ask for ROM name
                    await conv.send_message(f"Parsed Info:\nVersion: `{post_data['version']}`\nDate: `{post_data['build_date']}`\nStatus: `{post_data['status']}`\nDevice: `{post_data['device_name']}`\nInitial Variant: `{post_data['variants'][0]['name']}`\n\nWhat is the **full ROM Name** (e.g., `Axion Aosp`, `PixelExperience`)?")
                    rom_name_response = await conv.get_response()
                    post_data["rom_name"] = rom_name_response.text.strip()
            else:
                # If preset was chosen, ROM name is already set, but still show device
                await conv.send_message(f"Using ROM Name from preset: `{post_data['rom_name']}`\nDevice: `{post_data['device_name']}`")


            # 5. Additional Variants
            while True:
                variant_msg = await conv.send_message( # Store message object
                    f"Do you want to add another build variant (e.g., VANILLA, GAPPS)?",
                    buttons=[Button.inline("Yes"), Button.inline("No")]
                )
                add_variant_response = await conv.wait_event(
                    events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == variant_msg.id) # Check message ID
                )
                await add_variant_response.answer()
                try:
                    # Edit the message to show choice was made, remove buttons
                    await add_variant_response.edit(f"Add another variant? {add_variant_response.data.decode()}")
                except MessageNotModifiedError:
                    pass
                except Exception as edit_err:
                    logger.warning(f"Could not edit add variant choice message: {edit_err}")

                if add_variant_response.data == b'No':
                    break

                await conv.send_message("What is the name of this variant? (e.g., `Vanilla`, `Gapps`)")
                variant_name_resp = await conv.get_response()
                variant_name = variant_name_resp.text.strip().upper() # Standardize name

                await conv.send_message(f"Please send the download link for the **{variant_name}** variant.")
                variant_link_resp = await conv.get_response()
                variant_link = variant_link_resp.text.strip()
                if not variant_link.startswith(('http://', 'https://')):
                     await conv.send_message("Invalid link. Skipping this variant.")
                     continue

                post_data["variants"].append({
                    "name": variant_name,
                    "link": variant_link,
                    "sha256": None
                })
                await conv.send_message(f"Variant '{variant_name}' added.")

            # 6. Support Group (with Reuse option)
            support_buttons = [Button.inline("Skip", data=b'skip_support')] # Add data payload
            prompt_text = "Please send the link for the **Support Group** (or 'skip')."
            if saved_support:
                support_buttons.insert(0, Button.inline("Reuse Saved", data=b'reuse_support'))
                prompt_text = f"Please send the link for the **Support Group**.\nSaved: `{saved_support}`"

            support_msg = await conv.send_message(prompt_text, buttons=support_buttons)
            support_resp = await get_response_or_callback(support_msg, CONVERSATION_TIMEOUT) # Pass timeout

            if isinstance(support_resp, events.CallbackQuery.Event): # Check if it's a CallbackQuery
                if support_resp.data == b'reuse_support':
                    post_data["support_group"] = saved_support
                elif support_resp.data == b'skip_support': # Handle skip button
                    post_data["support_group"] = None
                # Message already edited by helper
            elif hasattr(support_resp, 'text'): # Check if it's a Message object with text
                 text_input = support_resp.text.strip()
                 if text_input.lower() == 'skip': # Handle text skip
                     post_data["support_group"] = None
                 elif text_input.startswith(('http://', 'https://')):
                     post_data["support_group"] = text_input
                     save_user_data(sender_id, support_group=text_input)
                 else:
                     await conv.send_message("Invalid link format. Skipping support group.")
                     post_data["support_group"] = None


            # 7. Screenshots
            ss_buttons = [Button.inline("Skip", data=b'skip_ss')] # Add Skip button
            ss_msg = await conv.send_message("Please send the link for **Screenshots**.", buttons=ss_buttons)
            ss_resp = await get_response_or_callback(ss_msg, CONVERSATION_TIMEOUT) # Pass timeout

            if isinstance(ss_resp, events.CallbackQuery.Event) and ss_resp.data == b'skip_ss': # Check for CallbackQuery
                post_data["screenshots"] = None
                # Message already edited by helper
            elif hasattr(ss_resp, 'text'): # Check for Message object
                ss_text = ss_resp.text.strip()
                if ss_text.startswith(('http://', 'https://')):
                     post_data["screenshots"] = ss_text
                else:
                     await conv.send_message("Invalid link format. Skipping screenshots.")
                     post_data["screenshots"] = None
            else: # Handle None or unexpected return
                 await conv.send_message("Invalid input. Skipping screenshots.")
                 post_data["screenshots"] = None


            # 8. Device Changelogs (with Reuse/Save option)
            dc_buttons = [Button.inline("None", data=b'none_dc')] # Add data payload
            prompt_text = "Do you have **Device Changelogs**? Send the **text**, a **link**, or choose **None**."
            if saved_device_cl:
                dc_buttons.insert(0, Button.inline("Reuse Saved Link", data=b'reuse_dc'))
                prompt_text = f"Do you have **Device Changelogs**?\nSaved Link: `{saved_device_cl}`\nSend new **text**, a new **link**, or choose an option."

            dc_msg = await conv.send_message(prompt_text, buttons=dc_buttons)
            dc_resp = await get_response_or_callback(dc_msg, CONVERSATION_TIMEOUT) # Pass timeout

            dc_text = None
            dc_link = None
            dc_skipped = False # Flag for skip/none

            if isinstance(dc_resp, events.CallbackQuery.Event): # Check for CallbackQuery
                if dc_resp.data == b'reuse_dc':
                    dc_link = saved_device_cl
                elif dc_resp.data == b'none_dc': # Handle None button
                    dc_skipped = True
                # Message already edited by helper
            elif hasattr(dc_resp, 'text'): # Check for Message object
                resp_text = dc_resp.text.strip()
                if resp_text.lower() == 'none': # Handle text none
                    dc_skipped = True
                elif resp_text.startswith(('http://', 'https://')):
                    dc_link = resp_text
                else:
                    dc_text = resp_text # It's raw text
            else: # Handle None or unexpected return
                 await conv.send_message("Invalid input for Device Changelog. Skipping.")
                 dc_skipped = True

            # Process collected dc_link or dc_text (only if not skipped)
            if not dc_skipped:
                if dc_link:
                    post_data["device_changelog"] = dc_link
                    # Ask to save if it's a new link
                    if dc_link != saved_device_cl:
                        save_dc_msg = await conv.send_message(f"Save this Device Changelog link for future use?\n`{dc_link}`", buttons=[Button.inline("Yes", data=b'save_dc'), Button.inline("No", data=b'no_save_dc')])
                        save_dc_resp = await conv.wait_event(events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == save_dc_msg.id))
                        await save_dc_resp.answer()
                        if save_dc_resp.data == b'save_dc':
                            save_user_data(sender_id, device_changelog=dc_link)
                            await save_dc_resp.edit("Link saved.")
                        else:
                            await save_dc_resp.edit("Link not saved.")

                elif dc_text:
                    paste_link = create_paste(dc_text, f"{post_data.get('rom_name', 'ROM')} Device Changelog")
                    if paste_link:
                        post_data["device_changelog"] = paste_link
                        await conv.send_message(f"Device changelog text uploaded: {paste_link}")
                    else:
                        await conv.send_message("⚠️ Failed to upload device changelog text to Pastebin. Skipping.")
                        post_data["device_changelog"] = None # Ensure it's None on failure
            else:
                 post_data["device_changelog"] = None # Ensure it's None if skipped


            # 9. Source Changelogs (Skip if preset used and provided a link)
            if not preset_chosen or not post_data.get("source_changelog"): # Ask if not using preset OR preset had no source CL
                sc_buttons = [Button.inline("None", data=b'none_sc')] # Add None button
                sc_msg = await conv.send_message("Do you have **Source Changelogs**? Send the **text**, a **link**, or choose **None**.", buttons=sc_buttons)
                sc_resp = await get_response_or_callback(sc_msg, CONVERSATION_TIMEOUT) # Pass timeout

                sc_text = None
                sc_link = None
                sc_skipped = False

                if isinstance(sc_resp, events.CallbackQuery.Event) and sc_resp.data == b'none_sc': # Check for CallbackQuery
                    sc_skipped = True
                    # Message already edited by helper
                elif hasattr(sc_resp, 'text'): # Check for Message object
                    resp_text = sc_resp.text.strip()
                    if resp_text.startswith(('http://', 'https://')):
                        sc_link = resp_text
                    else:
                        sc_text = resp_text # It's raw text
                else: # Handle None or unexpected return
                    await conv.send_message("Invalid input for Source Changelog. Skipping.")
                    sc_skipped = True

                # Process collected sc_link or sc_text (only if not skipped)
                if not sc_skipped:
                    if sc_link:
                        post_data["source_changelog"] = sc_link
                    elif sc_text:
                        paste_link = create_paste(sc_text, f"{post_data.get('rom_name', 'ROM')} Source Changelog") # Use get for rom_name
                        if paste_link:
                            post_data["source_changelog"] = paste_link
                            await conv.send_message(f"Source changelog text uploaded: {paste_link}")
                        else:
                            await conv.send_message("⚠️ Failed to upload source changelog text to Pastebin. Skipping.")
                            post_data["source_changelog"] = None # Ensure None on failure
                else:
                     post_data["source_changelog"] = None # Ensure it's None if skipped

            elif preset_chosen and post_data.get("source_changelog"): # Check preset had a value
                 # If preset was chosen and had a source CL, it's already set
                 await conv.send_message(f"Using Source Changelog from preset: `{post_data['source_changelog']}`")
            else: # Preset chosen but source_changelog was None in preset
                 await conv.send_message("No Source Changelog provided by preset.")
                 post_data["source_changelog"] = None


            # 10. Notes (with Reuse option and display)
            notes_buttons = [Button.inline("Skip", data=b'skip_notes')] # Add data payload
            prompt_text = "Do you have any **Notes**? Send the text (use new lines for bullet points) or type **skip**."
            if saved_notes:
                notes_buttons.insert(0, Button.inline("Reuse Saved", data=b'reuse_notes'))
                # Display a preview (e.g., first 100 chars)
                notes_preview = (saved_notes[:100] + '...') if len(saved_notes) > 100 else saved_notes
                prompt_text = f"Do you have any **Notes**? (Use new lines for bullet points)\n**Saved Preview:**\n```\n{notes_preview}\n```"

            notes_msg = await conv.send_message(prompt_text, buttons=notes_buttons)
            notes_resp = await get_response_or_callback(notes_msg, CONVERSATION_TIMEOUT) # Pass timeout

            if isinstance(notes_resp, events.CallbackQuery.Event): # Check for CallbackQuery
                 if notes_resp.data == b'reuse_notes':
                     post_data["notes"] = saved_notes
                 elif notes_resp.data == b'skip_notes': # Handle skip button
                     post_data["notes"] = None
                 # Message already edited by helper
            elif hasattr(notes_resp, 'text'): # Check for Message object
                notes_text = notes_resp.text.strip()
                if notes_text.lower() == 'skip': # Handle text skip
                    post_data["notes"] = None
                else:
                    post_data["notes"] = format_bullets(notes_text)
                    save_user_data(sender_id, notes=post_data["notes"]) # Save formatted notes
            else: # Handle None or unexpected return
                 await conv.send_message("Invalid input for Notes. Skipping.")
                 post_data["notes"] = None


            # 11. Credits (with Reuse option and display)
            credits_buttons = [Button.inline("Skip", data=b'skip_credits')] # Add data payload
            prompt_text = "Do you have any **Credits**? Send the text (use new lines for bullet points) or type **skip**."
            if saved_credits:
                credits_buttons.insert(0, Button.inline("Reuse Saved", data=b'reuse_credits'))
                credits_preview = (saved_credits[:100] + '...') if len(saved_credits) > 100 else saved_credits
                prompt_text = f"Do you have any **Credits**? (Use new lines for bullet points)\n**Saved Preview:**\n```\n{credits_preview}\n```"

            credits_msg = await conv.send_message(prompt_text, buttons=credits_buttons)
            credits_resp = await get_response_or_callback(credits_msg, CONVERSATION_TIMEOUT) # Pass timeout

            if isinstance(credits_resp, events.CallbackQuery.Event): # Check for CallbackQuery
                 if credits_resp.data == b'reuse_credits':
                     post_data["credits"] = saved_credits
                 elif credits_resp.data == b'skip_credits': # Handle skip button
                     post_data["credits"] = None
                 # Message already edited by helper
            elif hasattr(credits_resp, 'text'): # Check for Message object
                 credits_text = credits_resp.text.strip()
                 if credits_text.lower() == 'skip': # Handle text skip
                     post_data["credits"] = None
                 else:
                     post_data["credits"] = format_bullets(credits_text)
                     save_user_data(sender_id, credits=post_data["credits"]) # Save formatted credits
            else: # Handle None or unexpected return
                 await conv.send_message("Invalid input for Credits. Skipping.")
                 post_data["credits"] = None


            # 12. SHA256/MD5 Checksums
            sha_msg = await conv.send_message( # Store message object
                "Do you want to add MD5/SHA256 checksums for the variants?",
                buttons=[Button.inline("Yes"), Button.inline("No")]
            )
            add_sha_response = await conv.wait_event(
                events.CallbackQuery(func=lambda e: e.sender_id == sender_id and e.message_id == sha_msg.id) # Check message ID
            )
            await add_sha_response.answer()
            try:
                 # Edit the message to show choice was made, remove buttons
                await add_sha_response.edit(f"Add checksums? {add_sha_response.data.decode()}")
            except MessageNotModifiedError:
                pass
            except Exception as edit_err:
                 logger.warning(f"Could not edit checksum choice message: {edit_err}")

            if add_sha_response.data == b'Yes':
                for i, variant in enumerate(post_data["variants"]): # Corrected loop
                    sha_buttons = [Button.inline("Skip", data=b'skip_sha')] # Add skip button
                    sha_prompt_msg = await conv.send_message(
                        f"Please send the MD5 or SHA256 checksum for the **{variant['name']}** variant.",
                        buttons=sha_buttons
                    )
                    sha_resp = await get_response_or_callback(sha_prompt_msg, CONVERSATION_TIMEOUT) # Pass timeout

                    if isinstance(sha_resp, events.CallbackQuery.Event) and sha_resp.data == b'skip_sha': # Check for CallbackQuery
                        post_data["variants"][i]["sha256"] = None
                        # Message already edited by helper
                    elif hasattr(sha_resp, 'text'): # Check for Message object
                        sha_text = sha_resp.text.strip()
                        # Basic validation (check if not empty) - more complex validation could be added
                        if sha_text:
                            post_data["variants"][i]["sha256"] = sha_text
                        else:
                            await conv.send_message("Empty checksum received. Skipping.")
                            post_data["variants"][i]["sha256"] = None
                    else: # Handle None or unexpected return
                        await conv.send_message("Invalid input for checksum. Skipping.")
                        post_data["variants"][i]["sha256"] = None


            # --- Generate ALL Previews ---
            await conv.send_message("Generating previews...")
            
            # Generate text previews for both formats
            text_previews = {
                "default": generate_post_text("default", post_data),
                "minimal": generate_post_text("minimal", post_data)
            }
            
            # Generate banner previews if banner support is enabled
            banner_paths = {}  # Dictionary to store banner paths by style: {1: path1, 2: path2}
            
            if BANNER_SUPPORT:
                try:
                    # Generate Style 1 Preview
                    banner_path_style1 = bgen.generate_banner_file(post_data, style=1, file_format='png')
                    if banner_path_style1:
                        banner_paths[1] = banner_path_style1
                        temp_banner_paths.append(banner_path_style1)  # Add to cleanup list
                    
                    # Generate Style 2 Preview
                    banner_path_style2 = bgen.generate_banner_file(post_data, style=2, file_format='png')
                    if banner_path_style2:
                        banner_paths[2] = banner_path_style2
                        temp_banner_paths.append(banner_path_style2)  # Add to cleanup list
                    
                except Exception as banner_err:
                    logger.error(f"Error generating banner previews: {banner_err}")
            
            # --- Interactive Preview and Selection ---
            selected_format = "default"  # Default format
            selected_banner = 1 if 1 in banner_paths else None  # Default to style 1 if available
            preview_message = None
            
            # Helper function for updating the preview message
            async def update_preview_message():
                nonlocal preview_message
                
                # Get current preview text based on selected format
                current_text = text_previews[selected_format]
                
                # Create buttons for format and banner selection
                buttons = [
                    [
                        Button.inline(f"{'✅ ' if selected_format == 'default' else ''}Default", data=b"preview_format_default"),
                        Button.inline(f"{'✅ ' if selected_format == 'minimal' else ''}Minimal", data=b"preview_format_minimal")
                    ]
                ]
                
                # Add banner selection buttons if banner support is available
                if BANNER_SUPPORT and banner_paths:
                    banner_buttons = []
                    if 1 in banner_paths:
                        banner_buttons.append(Button.inline(f"{'✅ ' if selected_banner == 1 else ''}Style 1", data=b"preview_banner_1"))
                    if 2 in banner_paths:
                        banner_buttons.append(Button.inline(f"{'✅ ' if selected_banner == 2 else ''}Style 2", data=b"preview_banner_2"))
                    banner_buttons.append(Button.inline(f"{'✅ ' if selected_banner is None else ''}No Banner", data=b"preview_banner_none"))
                    buttons.append(banner_buttons)
                
                # Add confirm button
                buttons.append([Button.inline("✅ Confirm & Post", data=b"preview_confirm")])
                
                # Determine caption
                caption = f"**Preview** (Format: {selected_format.capitalize()})\n\n{current_text[:700]}...\n\n*Use buttons to change format or banner style*"
                
                # Handle sending or editing the preview message
                try:
                    current_banner_path = banner_paths.get(selected_banner) if selected_banner else None
                    
                    if preview_message is None:
                        # First time sending message
                        if current_banner_path and os.path.exists(current_banner_path):
                            preview_message = await conv.send_file(
                                current_banner_path,
                                caption=caption,
                                buttons=buttons,
                                parse_mode='md'
                            )
                        else:
                            preview_message = await conv.send_message(
                                caption,
                                buttons=buttons,
                                parse_mode='md'
                            )
                        return preview_message
                    else:
                        # Update existing message
                        if current_banner_path and os.path.exists(current_banner_path):
                            await preview_message.edit(
                                file=current_banner_path,
                                text=caption,
                                buttons=buttons,
                                parse_mode='md'
                            )
                        else:
                            # Remove image if user selects "No Banner"
                            await preview_message.edit(
                                text=caption,
                                buttons=buttons,
                                parse_mode='md',
                                file=None  # Remove media
                            )
                        return preview_message
                
                except MessageNotModifiedError:
                    # Message not modified, ignore
                    return preview_message
                except Exception as e:
                    logger.error(f"Error updating preview message: {e}")
                    if preview_message is None:
                        # If first attempt fails, try simpler message
                        preview_message = await conv.send_message(
                            f"Preview (Format: {selected_format.capitalize()})\n\n"
                            f"[Preview with formatting issues - click buttons to continue]",
                            buttons=buttons
                        )
                    return preview_message
            
            # Initial preview message
            preview_message = await update_preview_message()
            
            # Loop to handle selection changes
            while True:
                try:
                    # Wait for button press
                    button_pressed = await conv.wait_event(
                        events.CallbackQuery(
                            func=lambda e: e.sender_id == sender_id and 
                                          e.message_id == preview_message.id
                        ),
                        timeout=CONVERSATION_TIMEOUT
                    )
                    
                    await button_pressed.answer()  # Acknowledge the button press
                    action = button_pressed.data.decode()
                    
                    # Process user selection
                    if action == "preview_confirm":
                        # User confirmed, break out of loop
                        break
                    elif action == "preview_format_default":
                        selected_format = "default"
                    elif action == "preview_format_minimal":
                        selected_format = "minimal"
                    elif action == "preview_banner_1":
                        selected_banner = 1
                    elif action == "preview_banner_2":
                        selected_banner = 2
                    elif action == "preview_banner_none":
                        selected_banner = None
                    else:
                        logger.warning(f"Unknown preview action: {action}")
                        continue
                    
                    # Update preview with new selections
                    await update_preview_message()
                    
                except asyncio.TimeoutError:
                    await conv.send_message("Selection timed out. Using current options.")
                    break
            
            # Get final selections for posting
            final_post = text_previews[selected_format]
            final_banner_path = banner_paths.get(selected_banner) if selected_banner else None
            
            # Cleanup unused banner files (keep only the selected one)
            for style, path in banner_paths.items():
                if style != selected_banner and path in temp_banner_paths:
                    try:
                        os.remove(path)
                        temp_banner_paths.remove(path)
                        logger.info(f"Removed unused banner preview (Style {style})")
                    except OSError as e:
                        logger.error(f"Failed to delete unused banner: {e}")

            # --- Indexing and Posting ---
            if INDEX_SUPPORT:
                try:
                    if not post_data.get("device_name"): raise ValueError("Device codename missing")
                    if not post_data.get("rom_name"): raise ValueError("ROM name missing")

                    # Use final_banner_path (which might be None)
                    success, message_link = await index.post_to_channel(client, post_data, final_post, final_banner_path)

                    if success:
                        await conv.send_message(f"✅ Posted to channel successfully!\nLink: {message_link}")
                        if await index.add_to_index(
                            post_data["device_name"],
                            post_data["rom_name"],
                            message_link,
                            version=post_data.get("version"),
                            status=post_data.get("status")
                        ):
                            await conv.send_message("✅ Added to device index!")
                        else:
                            await conv.send_message("⚠️ Failed to add to device index.")
                            
                        # Send a copy of the post exactly as it appears in the channel
                        await conv.send_message("Here is your post as it appears in the channel:")
                        if final_banner_path and os.path.exists(final_banner_path):
                            # Send with banner and caption (just like in the channel)
                            await conv.send_file(
                                final_banner_path, 
                                caption=final_post,
                                parse_mode='md',
                                link_preview=False
                            )
                            
                            # Delete all banner files after sending
                            cleanup_banner_files(temp_banner_paths)
                        else:
                            # Text-only post (just like in the channel)
                            await conv.send_message(final_post, parse_mode='md', link_preview=False)
                    else:
                        # message_link might contain error string here
                        await conv.send_message(f"⚠️ Failed to post to channel. Reason: {message_link}. Your post was created but not indexed.")
                        
                        # Still provide the final post preview to the user
                        await conv.send_message("Here's what your post would have looked like:")
                        if final_banner_path and os.path.exists(final_banner_path):
                            await conv.send_file(
                                final_banner_path, 
                                caption=final_post, 
                                parse_mode='md',
                                link_preview=False
                            )
                            
                            # Delete all banner files after sending
                            cleanup_banner_files(temp_banner_paths)
                        else:
                            await conv.send_message(final_post, parse_mode='md', link_preview=False)

                except ValueError as ve:
                    logger.error(f"Missing data for indexing: {ve}")
                    await conv.send_message(f"Error: {ve}. Cannot post to channel.")
                except Exception as e:
                    logger.error(f"Error in indexing/posting process: {e}")
                    await conv.send_message(f"⚠️ Error during channel posting: {str(e)}")
            else:
                logger.warning("Index module not available, skipping channel posting.")
                await conv.send_message("Note: Channel posting/indexing is not available. Here is your generated post text:")
                # Send the final post preview to the user as it would appear
                if final_banner_path and os.path.exists(final_banner_path):
                    await conv.send_file(
                        final_banner_path,
                        caption=final_post,
                        parse_mode='md',
                        link_preview=False
                    )
                    
                    # Delete all banner files after sending
                    cleanup_banner_files(temp_banner_paths)
                else:
                    await conv.send_message(final_post, parse_mode='md', link_preview=False)

            # Increment post count
            increment_post_count(
                sender_id,
                sender.username if hasattr(sender, 'username') else None,
                sender.first_name if hasattr(sender, 'first_name') else None,
                sender.last_name if hasattr(sender, 'last_name') else None
            )

            logger.info(f"Post created successfully by {sender_id} in chat {chat_id} using format '{selected_format}'")

    except asyncio.TimeoutError:
        await event.reply("Command timed out. Please try again.")
    except AlreadyInConversationError:
        await event.reply("You already have an active session. Please finish it first or use /cancel.")
    except Exception as e:
        await event.reply(f"An error occurred: {e}")
        logger.exception(f"Error in new_post_handler for {sender_id} in {chat_id}: {e}")
    finally:
        if conv_key in active_conversations:
            del active_conversations[conv_key]

# --- List Devices Command ---
@client.on(events.NewMessage(pattern='/listdevices'))
async def list_devices_handler(event):
    try:
        import index
        devices = await index.get_all_devices()
        
        if not devices:
            await event.reply("No devices have been indexed yet.")
            return
        
        # Create buttons for each device
        buttons = []
        for device in sorted(devices):
            buttons.append([Button.inline(device, data=f"device_{device}")])
        
        await event.reply("Select a device to see available ROMs:", buttons=buttons)
    
    except ImportError:
        await event.reply("Device indexing is not available. Contact the bot owner.")
    except Exception as e:
        logger.error(f"Error in list_devices_handler: {e}")
        await event.reply(f"Error listing devices: {str(e)}")

# --- Device ROMs Callback Handler ---
@client.on(events.CallbackQuery(pattern=r"device_"))
async def device_roms_handler(event):
    try:
        import index # Ensure index is imported
        # Extract device name from callback data
        device_name = event.data.decode('utf-8').split('_', 1)[1]
        
        # Get all ROMs for this device, including version
        roms = await index.get_roms_for_device(device_name)
        
        if not roms:
            await event.answer(f"No ROMs found for {device_name}")
            await event.edit(f"No ROMs found for device: {device_name}")
            return
        
        # Create message with ROM links and versions
        message = f"**Available ROMs for {device_name}:**\n\n"
        
        for rom_name, link, version in roms:
            version_str = f" (v{version})" if version else "" # Add version if available
            message += f"• [{rom_name}{version_str}]({link})\n"
        
        await event.answer()
        await event.edit(message)
    
    except ImportError:
        await event.answer("Device indexing is not available.", alert=True)
    except Exception as e:
        logger.error(f"Error in device_roms_handler: {e}")
        await event.answer(f"Error: {str(e)}", alert=True)

@client.on(events.NewMessage(pattern='/search'))
async def search_roms_handler(event):
    try:
        # Get search term from command
        args = event.message.text.split(maxsplit=1)
        if len(args) < 2:
            await event.reply("Usage: `/search <device or rom name>`") # Updated usage message
            return
            
        search_term = args[1].strip().lower()
        
        # Import index module
        import index # Ensure index is imported
        
        # Search for ROMs, including version
        results = await index.search_roms(search_term)
        
        if not results:
            await event.reply(f"No ROMs found matching '{search_term}'")
            return
            
        # Group results by device
        devices = {}
        for device, rom, link, version in results: # Unpack version
            if device not in devices:
                devices[device] = []
            devices[device].append((rom, link, version)) # Store version
        
        # Format results
        message = f"**Search Results for '{search_term}':**\n\n"
        
        for device, roms in devices.items():
            message += f"📱 **{device}**\n"
            for rom_name, link, version in roms: # Unpack version
                version_str = f" (v{version})" if version else "" # Add version if available
                message += f"• [{rom_name}{version_str}]({link})\n"
            message += "\n"
        
        await event.reply(message)
    
    except ImportError:
        await event.reply("Search functionality is not available. Contact the bot owner.")
    except Exception as e:
        logger.error(f"Error in search_roms_handler: {e}")
        await event.reply(f"Error searching for ROMs: {str(e)}")

@client.on(events.NewMessage(pattern='/updateindex'))
async def update_index_handler(event):
    """Check if all indexed posts still exist and remove any that don't from the index database."""
    sender_id = event.sender_id
    
    # Only allow owner to run this command
    if not is_owner(sender_id):
        await event.reply("⛔️ You are not authorized to use this command.")
        return
    
    try:
        import index
        
        # Send initial status message
        status_msg = await event.reply("🔍 Checking index database for missing posts...")
        
        # Get all posts from the index
        all_posts = await index.get_all_indexed_posts()
        
        if not all_posts:
            await status_msg.edit("No posts found in the index database.")
            return
        
        total_posts = len(all_posts)
        await status_msg.edit(f"Found {total_posts} indexed posts. Checking status...")
        
        # Track our progress
        posts_checked = 0
        posts_removed = 0
        errors = 0
        
        # Process in batches to avoid rate limiting
        batch_size = 20
        
        for i in range(0, total_posts, batch_size):
            batch = all_posts[i:i+batch_size]
            batch_results = []
            
            # Update status periodically
            if i % 50 == 0 and i > 0:
                await status_msg.edit(f"Progress: {posts_checked}/{total_posts} checked, {posts_removed} removed, {errors} errors...")
            
            for post_id, device_name, rom_name, message_link in batch:
                posts_checked += 1
                
                # Extract necessary info to check if the post exists
                try:
                    # Parse the message link to get channel and message ID
                    if not message_link or not isinstance(message_link, str):
                        logger.warning(f"Invalid message link for post ID {post_id}: {message_link}")
                        batch_results.append((post_id, False, "Invalid link format"))
                        errors += 1
                        continue
                    
                    # Extract message ID from different link formats
                    if "/c/" in message_link:
                        # Format: https://t.me/c/channel_id/message_id
                        parts = message_link.split("/")
                        if len(parts) < 5:
                            batch_results.append((post_id, False, "Invalid link format"))
                            errors += 1
                            continue
                        
                        channel_id = int("-100" + parts[-2])  # Convert to proper format
                        message_id = int(parts[-1])
                    else:
                        # Format: https://t.me/channel_name/message_id
                        parts = message_link.split("/")
                        if len(parts) < 5:
                            batch_results.append((post_id, False, "Invalid link format"))
                            errors += 1
                            continue
                        
                        channel_name = parts[-2]
                        message_id = int(parts[-1])
                        
                        # Get the channel entity to convert username to ID
                        try:
                            channel = await client.get_entity(channel_name)
                            channel_id = channel.id
                        except Exception as ch_err:
                            logger.error(f"Error getting channel entity: {ch_err}")
                            batch_results.append((post_id, False, f"Channel not found: {channel_name}"))
                            errors += 1
                            continue
                    
                    # Try to get the message
                    try:
                        message = await client.get_messages(channel_id, ids=message_id)
                        if message is None:
                            # Message doesn't exist
                            batch_results.append((post_id, True, "Message not found"))
                        else:
                            # Message exists, keep in index
                            batch_results.append((post_id, False, "Message exists"))
                    except Exception as msg_err:
                        logger.error(f"Error getting message {message_id} from {channel_id}: {msg_err}")
                        batch_results.append((post_id, True, f"Error: {str(msg_err)}"))
                        errors += 1
                
                except Exception as e:
                    logger.error(f"Error processing post {post_id}: {e}")
                    batch_results.append((post_id, False, f"Processing error: {str(e)}"))
                    errors += 1
            
            # Process batch results
            for post_id, should_remove, reason in batch_results:
                if should_remove:
                    # Remove from index
                    if await index.remove_from_index_by_id(post_id):
                        posts_removed += 1
                        logger.info(f"Removed post ID {post_id} from index: {reason}")
                    else:
                        logger.error(f"Failed to remove post ID {post_id} from index")
            
            # Add a small delay between batches
            if i + batch_size < total_posts:
                await asyncio.sleep(2)
        
        # Final status update
        await status_msg.edit(f"✅ Index cleanup complete!\n\n"
                             f"• Total posts checked: {posts_checked}\n"
                             f"• Missing posts removed: {posts_removed}\n"
                             f"• Errors encountered: {errors}")
        
    except ImportError:
        await event.reply("⚠️ Index module not available. Contact the bot owner.")
    except Exception as e:
        logger.error(f"Error in update_index_handler: {e}")
        await event.reply(f"❌ Error updating index: {str(e)}")

# --- Ban-related Commands ---
@client.on(events.NewMessage(pattern='/ban'))
async def ban_user_handler(event):
    sender_id = event.sender_id
    
    if not is_owner(sender_id):
        await event.reply("⛔️ You are not authorized to use this command.")
        return
    
    args = event.message.text.split(maxsplit=2)
    if len(args) < 2:
        await event.reply("Usage: `/ban <user_id> [reason]`")
        return
    
    try:
        user_id = int(args[1])
        reason = args[2] if len(args) > 2 else None
        
        if user_id == sender_id:
            await event.reply("⚠️ You cannot ban yourself.")
            return
            
        if is_owner(user_id):
            await event.reply("⚠️ You cannot ban a bot owner.")
            return
        
        # Check if user exists by trying to get entity
        try:
            user = await client.get_entity(user_id)
            user_mention = f"[{user.first_name}](tg://user?id={user_id})"
        except Exception as e:
            logger.warning(f"Could not get entity for user {user_id}: {e}")
            user_mention = f"User {user_id}"
        
        if ban_user(user_id, sender_id, reason):
            await event.reply(f"✅ {user_mention} has been banned.\nReason: {reason or 'No reason specified'}")
        else:
            await event.reply(f"❌ Failed to ban {user_mention}.")
    
    except ValueError:
        await event.reply("Invalid user ID. Please provide a valid numeric ID.")
    except Exception as e:
        logger.error(f"Error in ban_user_handler: {e}")
        await event.reply(f"Error processing ban: {str(e)}")

@client.on(events.NewMessage(pattern='/unban'))
async def unban_user_handler(event):
    sender_id = event.sender_id
    
    if not is_owner(sender_id):
        await event.reply("⛔️ You are not authorized to use this command.")
        return
    
    args = event.message.text.split(maxsplit=1)
    if len(args) < 2:
        await event.reply("Usage: `/unban <user_id>`")
        return
    
    try:
        user_id = int(args[1])
        
        # Check if user is banned
        if not is_banned(user_id):
            await event.reply(f"User {user_id} is not banned.")
            return
        
        if unban_user(user_id):
            await event.reply(f"✅ User {user_id} has been unbanned.")
        else:
            await event.reply(f"❌ Failed to unban user {user_id}.")
    
    except ValueError:
        await event.reply("Invalid user ID. Please provide a valid numeric ID.")
    except Exception as e:
        logger.error(f"Error in unban_user_handler: {e}")
        await event.reply(f"Error processing unban: {str(e)}")

@client.on(events.NewMessage(pattern='/listbanned'))
async def list_banned_handler(event):
    sender_id = event.sender_id
    
    if not is_owner(sender_id):
        await event.reply("⛔️ You are not authorized to use this command.")
        return
    
    try:
        banned_users = get_banned_users()
        
        if not banned_users:
            await event.reply("No users are currently banned.")
            return
        
        message = "**Banned Users:**\n\n"
        for user_id, banned_at, banned_by, reason in banned_users:
            reason_text = f" | Reason: {reason}" if reason else ""
            message += f"• **ID:** `{user_id}` | Banned by: `{banned_by}` | Date: {banned_at}{reason_text}\n"
        
        await event.reply(message)
    
    except Exception as e:
        logger.error(f"Error in list_banned_handler: {e}")
        await event.reply(f"Error retrieving banned users: {str(e)}")

# --- Stats Command Handlers ---
@client.on(events.NewMessage(pattern='/topusers'))
async def top_users_handler(event):
    try:
        top_users = get_top_users(limit=10)
        
        if not top_users:
            await event.reply("No users have made posts yet.")
            return
        
        message = "**🏆 Top Users by Posts:**\n\n"
        
        for i, (user_id, username, first_name, last_name, post_count) in enumerate(top_users, 1):
            # Create user display name
            if first_name:
                display_name = first_name
                if last_name:
                    display_name += f" {last_name}"
            elif username:
                display_name = username
            else:
                display_name = f"User {user_id}"
            
            # Add medal emoji for top 3
            medal = ""
            if i == 1:
                medal = "🥇 "
            elif i == 2:
                medal = "🥈 "
            elif i == 3:
                medal = "🥉 "
            
            message += f"{medal}**{i}.** {display_name} - {post_count} posts\n"
        
        await event.reply(message)
    
    except Exception as e:
        logger.error(f"Error in top_users_handler: {e}")
        await event.reply(f"Error retrieving top users: {str(e)}")

@client.on(events.NewMessage(pattern='/botstats'))
async def bot_stats_handler(event):
    sender_id = event.sender_id
    
    # Only owners can see detailed bot statistics
    if not is_owner(sender_id):
        await event.reply("⛔️ You are not authorized to use this command.")
        return
    
    try:
        stats = get_bot_stats()
        
        message = "**📊 Bot Statistics:**\n\n"
        message += f"**Users:** {stats['total_users']}\n"
        message += f"**Total Posts:** {stats['total_posts']}\n"
        message += f"**Posts (Last 24h):** {stats['posts_last_24h']}\n"
        message += f"**Banned Users:** {stats['banned_users']}\n"
        message += f"**Unique ROMs:** {stats['unique_roms']}\n"
        message += f"**Devices:** {stats['devices_count']}\n"
        message += f"**Most Active Day:** {stats['most_active_day']} ({stats['most_active_day_posts']} posts)\n"
        
        # Get bot uptime
        bot_start_time = getattr(client, 'start_time', None)
        if bot_start_time:
            uptime = datetime.now() - bot_start_time
            days = uptime.days
            hours, remainder = divmod(uptime.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            
            message += f"\n**Bot Uptime:** {days}d {hours}h {minutes}m {seconds}s"
        
        await event.reply(message)
    
    except Exception as e:
        logger.error(f"Error in bot_stats_handler: {e}")
        await event.reply(f"Error retrieving bot statistics: {str(e)}")

# --- User Ban Functions ---
def ban_user(user_id, banned_by, reason=None):
    """Ban a user from using the bot."""
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_execute("INSERT OR REPLACE INTO banned_users (user_id, banned_at, banned_by, reason) VALUES (?, ?, ?, ?)",
                  (user_id, current_time, banned_by, reason))
        logger.info(f"User {user_id} banned by {banned_by}. Reason: {reason}")
        return True
    except Exception as e:
        logger.error(f"Error banning user {user_id}: {e}")
        return False

def unban_user(user_id):
    """Unban a user."""
    try:
        db_execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
        logger.info(f"User {user_id} unbanned")
        return True
    except Exception as e:
        logger.error(f"Error unbanning user {user_id}: {e}")
        return False

def is_banned(user_id):
    """Check if a user is banned."""
    result = db_fetchone("SELECT user_id FROM banned_users WHERE user_id = ?", (user_id,))
    return result is not None

def get_banned_users():
    """Get list of all banned users with ban info."""
    return db_fetchall("SELECT user_id, banned_at, banned_by, reason FROM banned_users ORDER BY banned_at DESC")

# --- Post Limit Functions ---
def check_post_limit(user_id):
    """Check if user has reached the post limit for today."""
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Get current count for today
    result = db_fetchone("SELECT count FROM post_limits WHERE user_id = ? AND post_date = ?", 
                      (user_id, today))
    
    current_count = result[0] if result else 0
    max_posts_per_day = 2  # Limit of 2 posts per day
    
    # Return remaining posts and boolean indicating if limit reached
    remaining = max_posts_per_day - current_count
    return remaining > 0, remaining

def increment_post_count(user_id, username=None, first_name=None, last_name=None):
    """Increment post count for a user and update stats."""
    today = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Initialize or update counter in post_limits
    result = db_fetchone("SELECT count FROM post_limits WHERE user_id = ? AND post_date = ?", 
                      (user_id, today))
    
    if result:
        # Increment existing counter
        new_count = result[0] + 1
        db_execute("UPDATE post_limits SET count = ? WHERE user_id = ? AND post_date = ?", 
                 (new_count, user_id, today))
    else:
        # Create new counter for today
        db_execute("INSERT INTO post_limits (user_id, post_date, count) VALUES (?, ?, 1)", 
                 (user_id, today))
        new_count = 1
    
    # Update user stats
    update_user_stats(user_id, username, first_name, last_name, post_increment=1, last_post_date=current_time)
    
    return new_count

# --- User Stats Functions ---
def update_user_stats(user_id, username=None, first_name=None, last_name=None, post_increment=0, last_post_date=None):
    """Update user statistics."""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Check if user exists in stats
    user = db_fetchone("SELECT user_id, total_posts FROM user_stats WHERE user_id = ?", (user_id,))
    
    if user:
        # User exists, update stats
        if post_increment > 0:
            new_post_count = user[1] + post_increment
            if last_post_date:
                db_execute("UPDATE user_stats SET total_posts = ?, last_post_date = ? WHERE user_id = ?", 
                         (new_post_count, last_post_date, user_id))
            else:
                db_execute("UPDATE user_stats SET total_posts = ? WHERE user_id = ?", 
                         (new_post_count, user_id))
        
        # Update user details if provided
        if username or first_name or last_name:
            # Get current values
            current_data = db_fetchone("SELECT username, first_name, last_name FROM user_stats WHERE user_id = ?", 
                                     (user_id,))
            
            if current_data:
                # Update only provided values
                new_username = username if username else current_data[0]
                new_first_name = first_name if first_name else current_data[1]
                new_last_name = last_name if last_name else current_data[2]
                
                db_execute("UPDATE user_stats SET username = ?, first_name = ?, last_name = ? WHERE user_id = ?", 
                         (new_username, new_first_name, new_last_name, user_id))
    else:
        # New user, create stats record
        total_posts = post_increment if post_increment > 0 else 0
        db_execute("""
            INSERT INTO user_stats 
            (user_id, username, first_name, last_name, total_posts, last_post_date, first_seen) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, username, first_name, last_name, total_posts, last_post_date, current_time))

def get_top_users(limit=10):
    """Get users with the most posts."""
    return db_fetchall("""
        SELECT user_id, username, first_name, last_name, total_posts 
        FROM user_stats 
        ORDER BY total_posts DESC 
        LIMIT ?
    """, (limit,))

def get_bot_stats():
    """Get overall bot statistics."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    stats = {}
    
    # Total users
    cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_stats")
    result = cursor.fetchone()
    stats['total_users'] = result[0] if result else 0
    
    # Total posts
    cursor.execute("SELECT SUM(total_posts) FROM user_stats")
    result = cursor.fetchone()
    stats['total_posts'] = result[0] or 0
    
    # Posts in last 24 hours
    cursor.execute("SELECT COUNT(*) FROM post_limits WHERE post_date >= date('now', '-1 day')")
    result = cursor.fetchone()
    stats['posts_last_24h'] = result[0] if result else 0
    
    # Banned users
    cursor.execute("SELECT COUNT(*) FROM banned_users")
    result = cursor.fetchone()
    stats['banned_users'] = result[0] if result else 0
    
    # ROM stats
    try:
        # Connect to the index database to get ROM stats
        index_db = os.path.join(os.path.dirname(__file__), 'device_index.db')
        if os.path.exists(index_db):
            index_conn = sqlite3.connect(index_db)
            index_cursor = index_conn.cursor()
            
            # ROM counts by device
            index_cursor.execute("SELECT COUNT(DISTINCT rom_name) FROM rom_index")
            result = index_cursor.fetchone()
            stats['unique_roms'] = result[0] if result else 0
            
            # Device count
            index_cursor.execute("SELECT COUNT(DISTINCT device_name) FROM rom_index")
            result = index_cursor.fetchone()
            stats['devices_count'] = result[0] if result else 0
            
            index_conn.close()
        else:
            stats['unique_roms'] = 0
            stats['devices_count'] = 0
    except Exception as e:
        # Handle errors gracefully
        logger.error(f"Error getting ROM stats: {e}")
        stats['unique_roms'] = 0
        stats['devices_count'] = 0
    
    # Most active day
    try:
        cursor.execute("""
            SELECT post_date, SUM(count) as total
            FROM post_limits
            GROUP BY post_date
            ORDER BY total DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        if result:
            stats['most_active_day'] = result[0]
            stats['most_active_day_posts'] = result[1]
        else:
            stats['most_active_day'] = "None"
            stats['most_active_day_posts'] = 0
    except Exception as e:
        logger.error(f"Error getting most active day: {e}")
        stats['most_active_day'] = "None"
        stats['most_active_day_posts'] = 0
    
    conn.close()
    return stats

# Add this new function for cleaning up all banner files
def cleanup_banner_files(banner_paths=None):
    """Deletes specific banner files or all temporary banner files in the directory."""
    try:
        # Delete specific files first if provided
        if banner_paths:
            for path in banner_paths:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                        logger.info(f"Deleted banner file: {path}")
                    except Exception as e:
                        logger.error(f"Failed to delete banner file {path}: {e}")
        
        # Also check for any leftover banner files by pattern
        script_dir = os.path.dirname(__file__)
        banner_pattern = re.compile(r'.*_style\d+\.(png|jpg|jpeg)$', re.IGNORECASE)
        
        for filename in os.listdir(script_dir):
            if banner_pattern.match(filename):
                filepath = os.path.join(script_dir, filename)
                try:
                    os.remove(filepath)
                    logger.info(f"Cleaned up extra banner file: {filepath}")
                except Exception as e:
                    logger.error(f"Failed to delete extra banner file {filepath}: {e}")
    except Exception as e:
        logger.error(f"Error in banner cleanup: {e}")

# --- Main Execution ---
async def main():
    # Record bot start time for uptime tracking
    client.start_time = datetime.now()
    
    # Start the client
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    logger.info(f"Bot started as {me.username} (ID: {me.id})")

    # Ensure owner is set if not done via ENV VAR
    if get_owner_id() is None:
        logger.warning("Owner ID not set. The first user to issue /start will become the owner.")

    logger.info("Bot is running...")
    await client.run_until_disconnected()

if __name__ == '__main__':
    # Basic check for core config
    if not API_ID or not API_HASH or not BOT_TOKEN: # Corrected condition
        logger.error("API_ID, API_HASH, and BOT_TOKEN must be set!")
    else:
        client.loop.run_until_complete(main())

