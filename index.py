import os
import sqlite3
import logging
import re
from datetime import datetime
import asyncio
from telethon import Button
from telethon.errors import FloodWaitError

# Setup logging
logger = logging.getLogger(__name__)

# Database file for the device index
INDEX_DB_FILE = 'device_index.db'

# Channel ID for posting ROMs (set to your desired channel)
CHANNEL_ID = os.environ.get("CHANNEL_ID") # Replace with your actual channel ID

def init_index_db():
    """Initialize the device index database."""
    conn = sqlite3.connect(INDEX_DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rom_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_name TEXT NOT NULL,
            rom_name TEXT NOT NULL,
            message_link TEXT NOT NULL,
            version TEXT,
            status TEXT,
            post_date TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(device_name, message_link)
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Index database initialized")

# Initialize DB on module import
init_index_db()

def db_execute(query, params=()):
    """Execute a database query with parameters"""
    conn = sqlite3.connect(INDEX_DB_FILE)
    cursor = conn.cursor()
    result = None
    try:
        cursor.execute(query, params)
        conn.commit()
        result = True
    except Exception as e:
        logger.error(f"Database error in db_execute: {e}")
        conn.rollback()
        result = False
    finally:
        conn.close()
    return result

def db_fetchall(query, params=()):
    """Execute a query and fetch all results"""
    conn = sqlite3.connect(INDEX_DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        result = cursor.fetchall()
    except Exception as e:
        logger.error(f"Database error in db_fetchall: {e}")
        result = []
    finally:
        conn.close()
    return result

def db_fetchone(query, params=()):
    """Execute a query and fetch one result"""
    conn = sqlite3.connect(INDEX_DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        result = cursor.fetchone()
    except Exception as e:
        logger.error(f"Database error in db_fetchone: {e}")
        result = None
    finally:
        conn.close()
    return result

async def post_to_channel(client, post_data, message_text, image_path=None):
    """
    Post the ROM update to the channel and return the message link
    """
    if not CHANNEL_ID:
        logger.error("CHANNEL_ID not set in environment variables")
        return False, None
    
    try:
        # Parse channel ID (it could be a username or numeric ID)
        channel_id = CHANNEL_ID
        # Handle string or integer properly
        if isinstance(channel_id, str) and channel_id.lstrip('-').isdigit():
            channel_id = int(channel_id)
        # If it's already an integer, don't need to do anything
        
        # Add entry date to message
        current_date = datetime.now().strftime("%d-%m-%Y")
        message_text += f"\n\nPosted: {current_date}"
        
        # Send message with or without image
        if image_path and os.path.exists(image_path):
            try:
                with open(image_path, 'rb') as file:
                    message = await client.send_file(
                        channel_id,
                        file,
                        caption=message_text,
                        parse_mode='md',
                        link_preview=False
                    )
            except Exception as img_err:
                logger.error(f"Error sending message with image: {img_err}")
                # Fallback to text-only message
                message = await client.send_message(
                    channel_id,
                    message_text,
                    parse_mode='md',
                    link_preview=False
                )
        else:
            message = await client.send_message(
                channel_id,
                message_text,
                parse_mode='md',
                link_preview=False
            )
        
        # Generate message link
        if hasattr(message, 'id'):
            # For public channels: https://t.me/channel_name/message_id
            # For private channels: we need to use a deeplink + access hash
            if isinstance(channel_id, str) and channel_id.startswith('@'):
                channel_name = channel_id[1:]  # Remove @ symbol
                message_link = f"https://t.me/{channel_name}/{message.id}"
            else:
                # For numerical IDs, try to get entity
                try:
                    channel_entity = await client.get_entity(channel_id)
                    if hasattr(channel_entity, 'username') and channel_entity.username:
                        message_link = f"https://t.me/{channel_entity.username}/{message.id}"
                    else:
                        # Fallback for private channels - just use ID 
                        message_link = f"https://t.me/c/{str(channel_id).replace('-100', '')}/{message.id}"
                except Exception as e:
                    logger.error(f"Could not get channel entity: {e}")
                    message_link = f"Message ID: {message.id}"
            
            return True, message_link
        
        return False, None
        
    except FloodWaitError as e:
        logger.error(f"Flood wait error when posting to channel: {e}")
        return False, f"Flood wait error: retry after {e.seconds} seconds"
    except Exception as e:
        logger.error(f"Error posting to channel: {e}")
        return False, str(e)

async def add_to_index(device_name, rom_name, message_link, version=None, status=None):
    """Add a ROM post to the index database"""
    if not device_name or not rom_name or not message_link:
        logger.error("Missing required parameters for indexing")
        return False
    
    # Clean and normalize device name
    device_name = device_name.lower().strip()
    device_name = re.sub(r'[^a-z0-9]', '', device_name)  # Remove any non-alphanumeric chars
    
    if not device_name:
        logger.error("Invalid device name after normalization")
        return False
    
    # Use current date for post_date
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    query = """
        INSERT OR REPLACE INTO rom_index 
        (device_name, rom_name, message_link, version, status, post_date) 
        VALUES (?, ?, ?, ?, ?, ?)
    """
    # Pass version and status to the query
    return db_execute(query, (device_name, rom_name, message_link, version, status, current_date))

async def get_all_devices():
    """Get list of all devices in the index"""
    query = "SELECT DISTINCT device_name FROM rom_index ORDER BY device_name"
    result = db_fetchall(query)
    return [row[0] for row in result] if result else []

async def get_roms_for_device(device_name):
    """Get all ROMs for a specific device, including version"""
    query = """
        SELECT rom_name, message_link, version FROM rom_index 
        WHERE device_name = ? 
        ORDER BY post_date DESC
    """
    return db_fetchall(query, (device_name.lower(),))

async def search_roms(search_term):
    """Search for ROMs by device name or ROM name, including version"""
    query = """
        SELECT device_name, rom_name, message_link, version FROM rom_index 
        WHERE device_name LIKE ? OR rom_name LIKE ? 
        ORDER BY post_date DESC
    """
    search_param = f"%{search_term}%"
    return db_fetchall(query, (search_param, search_param))

async def get_recent_posts(limit=10):
    """Get the most recent posts across all devices"""
    query = """
        SELECT device_name, rom_name, message_link, post_date FROM rom_index 
        ORDER BY post_date DESC LIMIT ?
    """
    return db_fetchall(query, (limit,))

async def get_all_indexed_posts():
    """Get all posts from the index database."""
    query = """
        SELECT id, device_name, rom_name, message_link FROM rom_index 
        ORDER BY post_date DESC
    """
    return db_fetchall(query)

async def remove_from_index_by_id(post_id):
    """Remove a post from the index by its ID."""
    if not post_id:
        logger.error("Missing post_id for removal")
        return False
    
    query = "DELETE FROM rom_index WHERE id = ?"
    return db_execute(query, (post_id,))

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
                break
            
        # Check for variant
        variant_found = False
        for variant in variant_keywords:
            if variant in clean_part:
                if result["variant_type"] is None:
                    result["variant_type"] = variant
                    logger.info(f"Found variant: {variant}")
                    variant_found = True
                    break
                    
        if variant_found:
            continue
        
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
    # ...existing code...
    
    # Process device name candidates more carefully
    # ...existing code...
    
    # Fill in missing values and properly capitalize
    # ...existing code...
    
    logger.info(f"Parsed filename data: {result}")
    return result

# Entry point for direct execution (useful for maintenance)
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "init":
        print("Initializing index database...")
        init_index_db()
        print("Done.")
    else:
        print("Available commands:")
        print("  init - Initialize the index database")
