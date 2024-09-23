import discord
import pandas as pd
import aiohttp
import asyncio
from sqlalchemy import create_engine, text
from airflow.models import Variable

# PostgreSQL connection settings
DB_URI = Variable.get('DB_URI')
SCHEMA = Variable.get('DB_SCHEMA')
engine = create_engine(DB_URI)

# Discord bot token и guild_id
token = Variable.get('DISCORD_BOT_TOKEN')
guild_id = Variable.get('GUILD_ID')

headers = {
    'Authorization': f'Bot {token}',
    'Content-Type': 'application/json'
}

class DiscordClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.df_invite = None

    async def on_ready(self):
        print(f'We have logged in as {self.user}')
        await self.fetch_user_invite_codes()
        await self.fetch_user_activity()
        await self.close()

    async def fetch_user_invite_codes(self):
        url = f'https://discord.com/api/v9/guilds/{guild_id}/members-search'
        all_invite_data = []
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url, json={'limit': 1000}) as response:
                if response.status == 200:
                    data = await response.json()
                    for member_info in data.get('members', []):
                        user_info = member_info['member']['user']
                        user_id = user_info['id']
                        invite_code = member_info.get('source_invite_code', 'N/A')
                        all_invite_data.append({
                            'user_id': user_id,
                            'source_invite_code': invite_code
                        })
                else:
                    print(f'Error fetching invite data: {response.status}')

        if all_invite_data:
            self.df_invite = pd.DataFrame(all_invite_data)
            await self.save_to_postgres(self.df_invite, 'discord_user_invite_codes')
        else:
            print('No invite data to save.')

    async def fetch_user_activity(self):
        all_user_data = []
        for guild in self.guilds:
            for channel in guild.text_channels:
                try:
                    async for message in channel.history(limit=None):
                        if message.author.bot:
                            continue
                        all_user_data.append({
                            'guild_name': guild.name,
                            'channel_name': channel.name,
                            'user_id': message.author.id,
                            'name': message.author.name,
                            'discriminator': message.author.discriminator,
                            'message_count': 1,
                            'message_timestamp': message.created_at
                        })
                except discord.Forbidden:
                    print(f"Cannot access channel: {channel.name}")

        if all_user_data:
            df_activity = pd.DataFrame(all_user_data)
            await self.save_to_postgres(df_activity, 'discord_user_activity')
        else:
            print('No activity data to save.')

    async def save_to_postgres(self, df, table_name):
        try:
            with engine.connect() as conn:
                create_table_query = f'''
                CREATE SCHEMA IF NOT EXISTS {SCHEMA};
                CREATE TABLE IF NOT EXISTS {SCHEMA}.{table_name} (
                    user_id TEXT,
                    source_invite_code TEXT,
                    guild_name TEXT,
                    channel_name TEXT,
                    name TEXT,
                    discriminator TEXT,
                    message_count INTEGER,
                    message_timestamp TIMESTAMP
                );
                '''
                conn.execute(text(create_table_query))
                print(f'Table {SCHEMA}.{table_name} created or already exists.')
                
                # Save data to the table
                if not df.empty:
                    df.to_sql(table_name, con=engine, if_exists='append', index=False, schema=SCHEMA)
                    print(f'Data saved to table {SCHEMA}.{table_name}.')
                else:
                    print(f'No data to save to {SCHEMA}.{table_name}.')
        except Exception as e:
            print(f'Error saving data to {SCHEMA}.{table_name}: {e}')

def run_discord_dump():
    # Run the Discord client
    asyncio.run(main())

async def main():
    intents = discord.Intents.default()
    intents.messages = True
    client = DiscordClient(intents=intents)
    await client.start(token)

if __name__ == "__main__":
    run_discord_dump()