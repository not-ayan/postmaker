import os
import textwrap
from PIL import Image, ImageDraw, ImageFont
import requests
from io import BytesIO
import logging
import re
import random
from datetime import datetime

logger = logging.getLogger(__name__)

# Constants
BANNER_WIDTH = 1600
BANNER_HEIGHT = 1000
PADDING = 40

# Directory paths
ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
FONTS_DIR = os.path.join(os.path.dirname(__file__), "fonts")

# Create necessary directories if they don't exist
os.makedirs(ASSETS_DIR, exist_ok=True)
os.makedirs(FONTS_DIR, exist_ok=True)

# Font paths
AGAINST_FONT = os.path.join(FONTS_DIR, "against.otf")
OUTFIT_FONT = os.path.join(FONTS_DIR, "outfit.ttf")
SINGA_FONT = os.path.join(FONTS_DIR, "singa.ttf")

# Default fonts in case primary fonts aren't available
DEFAULT_FONT = "arial.ttf"

def check_resources():
    """Check if required resources (fonts and backgrounds) are available locally."""
    # Check fonts
    fonts_available = os.path.exists(AGAINST_FONT) and os.path.exists(OUTFIT_FONT)
    
    # Check if at least one background image exists
    bg_images = [f for f in os.listdir(ASSETS_DIR) if f.startswith('bg') and f.lower().endswith(('.jpg', '.jpeg', '.png'))]
    backgrounds_available = len(bg_images) > 0
    
    return fonts_available, backgrounds_available

def download_file_if_needed(url, save_path):
    """Download a file if it doesn't exist."""
    if not os.path.exists(save_path):
        try:
            logger.info(f"Downloading file: {os.path.basename(save_path)}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            with open(save_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"File downloaded: {os.path.basename(save_path)}")
            return True
        except Exception as e:
            logger.error(f"Error downloading file {os.path.basename(save_path)}: {e}")
            return False
    return True

def download_resources_if_needed():
    """Download required fonts and background images if they don't exist."""
    fonts_available, backgrounds_available = check_resources()
    
    # Only download what's missing
    resources = {}
    
    if not os.path.exists(AGAINST_FONT):
        resources[AGAINST_FONT] = 'https://github.com/googlefonts/against/raw/main/fonts/otf/Against-Regular.otf'
    
    if not os.path.exists(OUTFIT_FONT):
        resources[OUTFIT_FONT] = 'https://github.com/Outfitio/Outfit-Fonts/raw/main/fonts/Outfit-Regular.ttf'
    
    # Download specific backgrounds for each style
    if not os.path.exists(os.path.join(ASSETS_DIR, "bg1.jpg")):
        resources[os.path.join(ASSETS_DIR, "bg1.jpg")] = 'https://source.unsplash.com/random/1600x800/?abstract,dark'
    
    if not os.path.exists(os.path.join(ASSETS_DIR, "bg2.jpg")):
        resources[os.path.join(ASSETS_DIR, "bg2.jpg")] = 'https://source.unsplash.com/random/1600x800/?gradient,tech'
    
    if not os.path.exists(os.path.join(ASSETS_DIR, "bg3.jpg")):
        resources[os.path.join(ASSETS_DIR, "bg3.jpg")] = 'https://source.unsplash.com/random/1600x800/?minimal,dark'
    
    # Additional backgrounds for style 2
    if not os.path.exists(os.path.join(ASSETS_DIR, "bg4.jpg")):
        resources[os.path.join(ASSETS_DIR, "bg4.jpg")] = 'https://source.unsplash.com/random/1600x800/?futuristic,tech'
    
    if not os.path.exists(os.path.join(ASSETS_DIR, "bg5.jpg")):
        resources[os.path.join(ASSETS_DIR, "bg5.jpg")] = 'https://source.unsplash.com/random/1600x800/?digital,blur'
    
    if not os.path.exists(os.path.join(ASSETS_DIR, "bg6.jpg")):
        resources[os.path.join(ASSETS_DIR, "bg6.jpg")] = 'https://source.unsplash.com/random/1600x800/?abstract,blue'
    
    if not resources:
        logger.info("All required resources already present locally.")
        return True
    
    logger.info(f"Downloading missing resources: {', '.join(os.path.basename(k) for k in resources.keys())}")
    
    success = True
    for path, url in resources.items():
        if not download_file_if_needed(url, path):
            success = False
    
    return success

def get_background_for_style(style):
    """Get a random background image for the specified style."""
    # Choose background options based on style
    if style == 2:
        bg_prefixes = ["bg4", "bg5", "bg6"]
    else:  # Default to style 1
        bg_prefixes = ["bg1", "bg2", "bg3"]
    
    # Find matching backgrounds in assets directory
    bg_images = []
    for prefix in bg_prefixes:
        matching_images = [f for f in os.listdir(ASSETS_DIR) 
                          if f.startswith(prefix) and 
                          f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        bg_images.extend(matching_images)
    
    if not bg_images:
        logger.warning(f"No background images found for style {style}. Downloading defaults.")
        download_resources_if_needed()
        
        # Try again after downloading
        bg_images = []
        for prefix in bg_prefixes:
            matching_images = [f for f in os.listdir(ASSETS_DIR) 
                              if f.startswith(prefix) and 
                              f.lower().endswith(('.jpg', '.jpeg', '.png'))]
            bg_images.extend(matching_images)
    
    if not bg_images:
        logger.error(f"No background images available for style {style} after download attempt.")
        # Fallback to any available background
        all_backgrounds = [f for f in os.listdir(ASSETS_DIR) 
                          if f.startswith('bg') and 
                          f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        if all_backgrounds:
            bg_path = os.path.join(ASSETS_DIR, random.choice(all_backgrounds))
            logger.info(f"Using fallback background image: {bg_path}")
            return bg_path
        return None
    
    bg_path = os.path.join(ASSETS_DIR, random.choice(bg_images))
    logger.info(f"Using background image for style {style}: {bg_path}")
    return bg_path

def calculate_font_size(text, max_width, max_height, font_path):
    """Calculate the optimal font size to fit text within dimensions."""
    font_size = 300  # Start with a large size
    min_size = 30    # Don't go smaller than this
    
    # Use a binary search approach to find optimal size
    while font_size > min_size:
        try:
            # Create test font
            font = ImageFont.truetype(font_path, font_size)
            
            # Get text dimensions
            left, top, right, bottom = font.getbbox(text)
            text_width = right - left
            text_height = bottom - top
            
            # Check if it fits within our constraints
            if text_width <= max_width and text_height <= max_height:
                return font_size
            
            # Reduce font size
            font_size -= 5
        except Exception as e:
            logger.error(f"Error calculating font size: {e}")
            font_size -= 10
    
    return min_size

def _generate_style1(draw, post_data, width, height):
    """Generates the content for banner style 1 (Original)."""
    rom_name = post_data.get("rom_name", "ROM").lower()
    device_name = post_data.get("device_name", "Unknown").lower()
    maintainer_name = post_data.get("maintainer_name", "Unknown")

    # Load Against font for ROM name
    if os.path.exists(AGAINST_FONT):
        if len(rom_name) > 12:
            name_font_size = 97
        else:
            name_font_size = 133
        rom_name_font = ImageFont.truetype(AGAINST_FONT, name_font_size)
        logger.info(f"Style 1: Using Against font size {name_font_size} for '{rom_name}'")
    else:
        logger.warning("Style 1: Against font not found, using default")
        rom_name_font = ImageFont.load_default()

    # Load Outfit font for subtext
    subtext_font_size = 20
    if os.path.exists(OUTFIT_FONT):
        subtext_font = ImageFont.truetype(OUTFIT_FONT, subtext_font_size)
        logger.info(f"Style 1: Using Outfit font size {subtext_font_size}")
    else:
        logger.warning("Style 1: Outfit font not found, using default")
        subtext_font = ImageFont.load_default()

    # Calculate positions
    bbox_rom = rom_name_font.getbbox(rom_name)
    rom_name_width = bbox_rom[2] - bbox_rom[0]
    rom_name_height = bbox_rom[3] - bbox_rom[1]
    rom_name_x = (width - rom_name_width) // 2
    rom_name_y = (height - rom_name_height) // 2 - 30

    subtext = f"FOR {device_name} | BY {maintainer_name}".upper()
    bbox_sub = subtext_font.getbbox(subtext)
    subtext_width = bbox_sub[2] - bbox_sub[0]
    subtext_x = (width - subtext_width) // 2
    subtext_y = rom_name_y + rom_name_height + 60

    # Draw text
    draw.text((rom_name_x, rom_name_y), rom_name, fill=(255, 255, 255), font=rom_name_font)
    draw.text((subtext_x, subtext_y), subtext, fill=(255, 255, 255), font=subtext_font)

def _generate_style2(draw, post_data, width, height):
    """Generates the content for banner style 2 (Alternative)."""
    rom_name = post_data.get("rom_name", "ROM").lower()  # Lowercase ROM name
    device_name = post_data.get("device_name", "Unknown").lower()
    maintainer_name = post_data.get("maintainer_name", "Unknown").lower() # Lowercase maintainer name

    # Load fonts
    singa_font_path = SINGA_FONT if os.path.exists(SINGA_FONT) else DEFAULT_FONT
    outfit_font_path = OUTFIT_FONT if os.path.exists(OUTFIT_FONT) else DEFAULT_FONT

    if singa_font_path == DEFAULT_FONT:
        logger.warning("Style 2: Singa font not found, using default")
    if outfit_font_path == DEFAULT_FONT:
        logger.warning("Style 2: Outfit font not found, using default")

    # Determine ROM name font size
    if len(rom_name) <= 6:
        rom_name_font_size = 150
    else:
        rom_name_font_size = 95
    
    rom_name_font = ImageFont.truetype(singa_font_path, rom_name_font_size)
    
    # Subtext font
    subtext_font_size = 18
    subtext_font = ImageFont.truetype(outfit_font_path, subtext_font_size)
    
    logger.info(f"Style 2: Using Singa font size {rom_name_font_size} for ROM name, Outfit font size {subtext_font_size} for subtext.")

    # Calculate positions
    # ROM Name
    bbox_rom = rom_name_font.getbbox(rom_name)
    rom_name_width = bbox_rom[2] - bbox_rom[0]
    rom_name_height = bbox_rom[3] - bbox_rom[1]
    rom_name_x = 73
    # Calculate vertical center for ROM name
    rom_name_y = (height - rom_name_height) // 2

    # Subtext
    subtext = f" for {device_name} by {maintainer_name}"
    bbox_sub = subtext_font.getbbox(subtext)
    subtext_width = bbox_sub[2] - bbox_sub[0]
    subtext_height = bbox_sub[3] - bbox_sub[1]
    subtext_x = 73
    subtext_y = rom_name_y + rom_name_height + 10 # Position below ROM name with some padding

    # Draw text
    text_color = (23, 23, 23) # #171717 in RGB
    draw.text((rom_name_x, rom_name_y), rom_name, fill=text_color, font=rom_name_font)
    draw.text((subtext_x, subtext_y), subtext, fill=text_color, font=subtext_font)

def generate_banner(post_data, file_format='png', style=1):
    """Generate a banner image using the post data and specified style."""
    try:
        # Validate format
        file_format = file_format.lower()
        supported_formats = {'png': 'PNG', 'jpg': 'JPEG', 'jpeg': 'JPEG'}
        if (file_format not in supported_formats):
            logger.warning(f"Unsupported format '{file_format}', falling back to png")
            file_format = 'png'
        
        pil_format = supported_formats[file_format]
        
        # Check if resources are already available
        fonts_available, backgrounds_available = check_resources()
        
        # Only download what's missing (if anything)
        if not (fonts_available and backgrounds_available):
            download_resources_if_needed()
        
        # Get style-specific background image
        bg_path = get_background_for_style(style)
        if (bg_path and os.path.exists(bg_path)):
            image = Image.open(bg_path).convert("RGB")
            # Resize to banner dimensions if needed
            if image.size != (BANNER_WIDTH, BANNER_HEIGHT):
                image = image.resize((BANNER_WIDTH, BANNER_HEIGHT), Image.LANCZOS)
        else:
            # Create a plain black background if no image is available
            image = Image.new('RGB', (BANNER_WIDTH, BANNER_HEIGHT), (0, 0, 0))
        
        # Removed the overlay for style 2 - we don't want the dark overlay anymore
        
        draw = ImageDraw.Draw(image)

        # Call the selected style function
        if style == 2:
            _generate_style2(draw, post_data, BANNER_WIDTH, BANNER_HEIGHT)
        else: # Default to style 1
            _generate_style1(draw, post_data, BANNER_WIDTH, BANNER_HEIGHT)

        # Save to BytesIO buffer (in-memory)
        output = BytesIO()
        image.save(output, format=pil_format, quality=95 if pil_format == 'JPEG' else None)
        output.seek(0)
        
        return output, file_format
    
    except Exception as e:
        logger.error(f"Error generating banner (style {style}): {e}")
        return None, None

def generate_banner_file(post_data, output_path=None, file_format='png', style=1):
    """Generate a banner image using the specified style and save to a file."""
    try:
        if output_path is None:
            # Generate a default filename if none provided
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            safe_rom = "".join(c for c in post_data.get("rom_name", "banner") if c.isalnum()).lower()
            safe_dev = "".join(c for c in post_data.get("device_name", "") if c.isalnum()).lower()
            output_path = f"{safe_rom}_{safe_dev}_{timestamp}_style{style}.{file_format}" # Add style to filename

        logger.info(f"Generating banner (style {style}) for {post_data.get('rom_name', 'Unknown ROM')} • {post_data.get('device_name', 'Unknown device')}")

        # Pass style argument to generate_banner
        banner_buffer, actual_format = generate_banner(post_data, file_format, style=style)
        if banner_buffer:
            # Update extension in output_path if format changed during generation
            base_name, current_ext = os.path.splitext(output_path)
            correct_ext = f".{actual_format.lower()}"
            if current_ext.lower() != correct_ext:
                # Ensure style info isn't lost if base_name included it
                if f"_style{style}" not in base_name:
                    base_name += f"_style{style}"
                output_path = f"{base_name}{correct_ext}"

            # Create full path relative to this script's directory
            # Ensure the output directory exists (e.g., if output_path includes subdirs)
            full_path = os.path.join(os.path.dirname(__file__), output_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)

            # Save the image to file
            with open(full_path, 'wb') as f:
                f.write(banner_buffer.getvalue())
            
            logger.info(f"Banner saved to {full_path}")
            return full_path
        else:
            logger.error(f"Failed to generate banner buffer (style {style})")
            return None
    
    except Exception as e:
        logger.error(f"Error saving banner to file (style {style}): {e}")
        return None
