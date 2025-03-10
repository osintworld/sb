import asyncio
import discord
from discord.ext import commands
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import time

# Intents and Developer ID setup
intents = discord.Intents.all()
DEVELOPER_USER_ID = 1272961884831875237

# Set up SQLite connections
def setup_database(db_name, table_schema):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(table_schema)
    conn.commit()
    conn.close()

def setup_databases():
    setup_database('blacklist.db', '''CREATE TABLE IF NOT EXISTS blacklist (user_id INTEGER PRIMARY KEY)''')
    setup_database('whitelists.db', '''CREATE TABLE IF NOT EXISTS whitelist (user_id INTEGER PRIMARY KEY)''')
    setup_database('untouchable.db', '''CREATE TABLE IF NOT EXISTS untouchable (user_id INTEGER PRIMARY KEY)''')
    setup_database('rate_limits.db', '''CREATE TABLE IF NOT EXISTS rate_limit (user_id INTEGER, command TEXT, count INTEGER, last_used REAL)''')

# Helper functions for DB checks (with ThreadPoolExecutor)
executor = ThreadPoolExecutor()

async def is_in_database(db_name, table_name, user_id):
    loop = asyncio.get_running_loop()
    def check():
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f'SELECT 1 FROM {table_name} WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    return await loop.run_in_executor(executor, check)

async def check_rate_limit(user_id, command):
    loop = asyncio.get_running_loop()
    def rate_limiter():
        conn = sqlite3.connect('rate_limits.db')
        cursor = conn.cursor()
        
        # Clear entries older than 1 minute
        cursor.execute('DELETE FROM rate_limit WHERE last_used < ?', (time.time() - 60,))
        
        # Check if user and command exist
        cursor.execute('SELECT count, last_used FROM rate_limit WHERE user_id = ? AND command = ?', (user_id, command))
        result = cursor.fetchone()
        
        # Initialize or update command count
        current_time = time.time()
        if result:
            count, last_used = result
            if current_time - last_used < 60:  # within 1 minute
                if count >= 5:
                    conn.close()
                    return True  # rate limited
                else:
                    cursor.execute('UPDATE rate_limit SET count = count + 1, last_used = ? WHERE user_id = ? AND command = ?', (current_time, user_id, command))
            else:
                cursor.execute('UPDATE rate_limit SET count = 1, last_used = ? WHERE user_id = ? AND command = ?', (current_time, user_id, command))
        else:
            cursor.execute('INSERT INTO rate_limit (user_id, command, count, last_used) VALUES (?, ?, 1, ?)', (user_id, command, current_time))
        
        conn.commit()
        conn.close()
        return False  # not rate limited

    return await loop.run_in_executor(executor, rate_limiter)

# Function to load bot tokens from a file
def load_tokens(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

# Function to create and start a bot
async def create_bot(token, bot_list):
    bot = commands.Bot(command_prefix='!', intents=intents)
    bot_list.append(bot)

    @bot.event
    async def on_ready():
        print(f'Bot {bot.user.name} has connected.')
    
    # Set the bot's activity to Streaming on Twitch
        stream_message = "TSUKI RAID TOOL BY LXST: discord.gg/jKmfXzu8Td"  # Replace with your desired message
        twitch_url = "https://twitch.tv/devilbinn"  # Replace with your Twitch channel URL
    
        activity = discord.Streaming(name=stream_message, url=twitch_url)
        await bot.change_presence(activity=activity)


    # Check if the user is authorized
    async def check_authorization(ctx):
        if ctx.author.id == DEVELOPER_USER_ID:
            return False  # Developer is always authorized
        if await is_in_database('blacklist.db', 'blacklist', ctx.author.id):
            await ctx.send("You are blacklisted from using this bot.")
            return True  # Deny access
        if not await is_in_database('whitelists.db', 'whitelist', ctx.author.id):
            await ctx.send("You are not authorized to use this bot.")
            return True  # Deny access
        return False  # Authorized

    # Rate-limited command decorator
    async def rate_limited(ctx):
        if ctx.author.id == DEVELOPER_USER_ID:
            return False  # Developer bypasses rate limits
        return await check_rate_limit(ctx.author.id, ctx.command.name)

    @bot.command()
    async def ping(ctx):
        if await check_authorization(ctx): 
            return  # Exit if user is unauthorized
        if await rate_limited(ctx):
            await ctx.send("You are being rate limited. Please wait a moment before trying again.")
            return

        try:
            latency = bot.latency * 1000  # Convert latency to milliseconds
            embed = discord.Embed(
                title="🏓 Pong!",
                description=f"Latency: {latency:.2f} ms",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
        except Exception as e:
            print(f"Error in ping command: {e}")
            await ctx.send("There was an error running the ping command.")

    @bot.command()
    async def raid(ctx, user_id: int, message: str, times: int):
        if await check_authorization(ctx): 
            return  # Exit if user is unauthorized
        
        if times < 1 or times > 1000000:
            await ctx.send("Please specify a number between 1 and 1000000.")
            return

        if await is_in_database('untouchable.db', 'untouchable', user_id):
            await ctx.send("This user is protected and cannot be targeted by the raid command.")
            return

        user = bot.get_user(user_id)
        if user is None:
            await ctx.send("User not found.")
            return

        for _ in range(times):
            try:
                await user.send(message)
                await asyncio.sleep(1)  # Avoid rate limits
            except discord.errors.HTTPException as e:
                print(f"Failed to send message: {e}")
                break

        await ctx.send(f"Sent '{message}' to {user.name} {times} times!")

    @bot.command()
    async def untouchable(ctx, user_id: int):
        if ctx.author.id != DEVELOPER_USER_ID:
            await ctx.send("You are not authorized.")
            return

        conn = sqlite3.connect('untouchable.db')
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO untouchable (user_id) VALUES (?)', (user_id,))
        conn.commit()
        conn.close()
        await ctx.send(f"User with ID {user_id} has been marked as untouchable.")

    @bot.command()
    async def touchable(ctx, user_id: int):
        if ctx.author.id != DEVELOPER_USER_ID:
            await ctx.send("You are not authorized.")
            return

        try:
            conn = sqlite3.connect('untouchable.db')
            cursor = conn.cursor()
            
            # Check if the user exists in the untouchable table
            cursor.execute('SELECT 1 FROM untouchable WHERE user_id = ?', (user_id,))
            if cursor.fetchone() is None:
                await ctx.send(f"User with ID {user_id} is no longer marked as untouchable.")
                conn.close()
                return

            # Delete the user from the untouchable table
            cursor.execute('DELETE FROM untouchable WHERE user_id = ?', (user_id,))
            conn.commit()
            conn.close()

            await ctx.send(f"User with ID {user_id} is no longer untouchable.")
        except Exception as e:
            await ctx.send("An error occurred while trying to remove the user.")
            print(f"Error in touchable command: {e}")

    @bot.command()
    async def whitelist(ctx, user_id: int):
        if ctx.author.id != DEVELOPER_USER_ID:
            await ctx.send("You are not authorized.")
            return

        conn = sqlite3.connect('whitelists.db')
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO whitelist (user_id) VALUES (?)', (user_id,))
        conn.commit()
        conn.close()
        await ctx.send(f"User with ID {user_id} has been whitelisted.")

    @bot.command()
    async def unwhitelist(ctx, user_id: int):
        if ctx.author.id != DEVELOPER_USER_ID:
            await ctx.send("You are not authorized.")
            return

        conn = sqlite3.connect('whitelists.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM whitelist WHERE user_id = ?', (user_id,))
        conn.commit()
        conn.close()
        await ctx.send(f"User with ID {user_id} has been removed from the whitelist.")

    @bot.command()
    async def blacklist(ctx, user_id: int):
        if ctx.author.id != DEVELOPER_USER_ID:
            await ctx.send("You are not authorized.")
            return

        conn = sqlite3.connect('blacklist.db')
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO blacklist (user_id) VALUES (?)', (user_id,))
        conn.commit()
        conn.close()
        await ctx.send(f"User with ID {user_id} has been blacklisted.")

    @bot.command()
    async def unblacklist(ctx, user_id: int):
        if ctx.author.id != DEVELOPER_USER_ID:
            await ctx.send("You are not authorized.")
            return

        conn = sqlite3.connect('blacklist.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM blacklist WHERE user_id = ?', (user_id,))
        conn.commit()
        conn.close()
        await ctx.send(f"User with ID {user_id} has been removed from the blacklist.")

    await bot.start(token)

# Main function to run the bot with all tokens
async def main():
    setup_databases()
    tokens = load_tokens('tokens.txt')
    bot_list = []

    tasks = [create_bot(token, bot_list) for token in tokens]
    await asyncio.gather(*tasks)

# Run the main function
if __name__ == '__main__':
    asyncio.run(main())