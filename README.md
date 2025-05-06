# Telegram Post Maker Bot

A Telegram bot to automate creating and posting custom ROM update announcements.

## Core Features

-   Interactive post creation (`/new`)
-   Automatic ROM filename parsing from URLs
-   Customizable banner generation (two styles)
-   Pastebin integration for long changelogs/notes
-   ROM presets for quick post generation
-   User data persistence (support groups, notes, credits)
-   Device and ROM indexing with browsing (`/listdevices`) and search (`/search`)
-   Owner controls (PM toggle, chat management, presets, index updates, user bans, stats)
-   Daily post limits for users

## Installation

1.  **Clone:** `git clone https://github.com/not-ayan/postmaker && cd postmaker`
2.  **Install Dependencies:** `pip install telethon Pillow requests`
3.  **Configure Environment Variables:**
    -   `API_ID`: Your Telegram API ID
    -   `API_HASH`: Your Telegram API Hash
    -   `BOT_TOKEN`: Your Telegram Bot Token
    -   `OWNER_ID` (Optional): Your Telegram User ID
    -   `PASTEBIN_API_KEY`: Your Pastebin Developer API Key
    -   `CHANNEL_ID`: Telegram Channel ID for posts (set in `index.py` or as env var if modified)
4.  **Databases:** `settings.db` and `device_index.db` are created automatically.

## Usage

1.  **Run:** `python bot.py`
2.  **Bot Commands:**
    -   `/start`: Initialize bot & set owner if first run.
    -   `/help`: View available commands.
    -   `/new`: Create a new ROM post.
    -   `/search <term>`: Search for ROMs by device or ROM name.
    -   Owner commands provide administrative functions (see `/help` as owner).

## Modules

-   **`bot.py`**: Main bot logic, command handling, user interaction, settings DB (`settings.db`).
-   **`bgen.py`**: Banner image generation.
-   **`index.py`**: ROM indexing, channel posting, index DB (`device_index.db`).

## Contributing

1.  Fork the Project
2.  Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3.  Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4.  Push to the Branch (`git push origin feature/AmazingFeature`)
5.  Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.
(Create a `LICENSE` file if you choose this license).

