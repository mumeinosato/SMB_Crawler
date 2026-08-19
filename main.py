import asyncio
import discord
import os
from dotenv import load_dotenv
from crawler import Crawler

load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID"))

intents = discord.Intents.default()
intents.message_content = True

client = discord.Client(intents=intents)


def chunk_messages(files, limit=1900):
    chunk = []
    length = 0
    for file_name in files:
        line = str(file_name)
        if chunk and length + len(line) + 1 > limit:
            yield "\n".join(chunk)
            chunk = []
            length = 0

        chunk.append(line)
        length += len(line) + 1

    if chunk:
        yield "\n".join(chunk)


@client.event
async def on_ready():
    print(f'Logged in as {client.user.name} ({client.user.id})')
    
    channel = client.get_channel(CHANNEL_ID)
    
    if not channel:
        print(f"Channel with ID {CHANNEL_ID} not found.")
        await client.close()
        return
    
    try:
        print("Started Crawler")
        await channel.send("Started Crawler")
        crawler = Crawler()
        mp4_files = await asyncio.to_thread(crawler.run)
        
        await channel.send(f"Found {len(mp4_files)} non-H.265 .mp4 files:")
        
        await channel.send("-------------------------------")
        
        for message in chunk_messages(mp4_files):
            await channel.send(message)
        
    except Exception as e:
        print(f"Error sending message: {e}")
        await channel.send(f"Error: {e}")
        
    finally:
        await client.close()

client.run(TOKEN)
