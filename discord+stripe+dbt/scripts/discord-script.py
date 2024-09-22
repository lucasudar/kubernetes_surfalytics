import os
import discord
import pandas as pd
import aiohttp
import asyncio
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone

# PostgreSQL connection settings
DB_URI = os.getenv('DB_URI')
engine = create_engine(DB_URI)

# Discord bot token и guild_id
token = os.getenv('DISCORD_BOT_TOKEN')
guild_id = os.getenv('GUILD_ID')

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
        await self.handle_data_loading('discord_user_invite_codes', 'invite')
        await self.handle_data_loading('discord_user_activity', 'activity')
        await self.close()

    async def handle_data_loading(self, table_name, data_type):
        table_exists = await self.check_table_exists(table_name)
        
        # Если таблица существует и в ней есть данные, удаляем записи за последние два дня
        if table_exists:
            await self.delete_last_two_days_data(table_name)
        else:
            print(f'Table raw_new.{table_name} does not exist or is empty. Loading all data.')

        # Загружаем данные в зависимости от типа
        if data_type == 'invite':
            await self.fetch_user_invite_codes()
        elif data_type == 'activity':
            await self.fetch_user_activity()

    async def check_table_exists(self, table_name):
        try:
            with engine.connect() as conn:
                query = f'''
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'raw_new' 
                    AND table_name = '{table_name}'
                );
                '''
                result = conn.execute(text(query)).scalar()
                return result
        except Exception as e:
            print(f'Error checking if table raw_new.{table_name} exists: {e}')
            return False

    async def delete_last_two_days_data(self, table_name):
        try:
            with engine.connect() as conn:
                delete_query = f'''
                DELETE FROM raw_new.{table_name}
                WHERE message_timestamp >= NOW() - INTERVAL '2 days';
                '''
                conn.execute(text(delete_query))
                print(f'Deleted data from last 2 days in table raw_new.{table_name}.')
        except Exception as e:
            print(f'Error deleting data from raw_new.{table_name}: {e}')

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
                            'source_invite_code': invite_code,
                            'etl_timestamp': datetime.now(timezone.utc)
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
                    async for message in channel.history(after=datetime.now(timezone.utc) - timedelta(days=2)):
                        if message.author.bot:
                            continue
                        all_user_data.append({
                            'guild_name': guild.name,
                            'channel_name': channel.name,
                            'user_id': message.author.id,
                            'name': message.author.name,
                            'discriminator': message.author.discriminator,
                            'message_count': 1,
                            'message_timestamp': message.created_at,
                            'etl_timestamp': datetime.now(timezone.utc)
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
                CREATE SCHEMA IF NOT EXISTS raw_new;
                CREATE TABLE IF NOT EXISTS raw_new.{table_name} (
                    user_id TEXT,
                    source_invite_code TEXT,
                    guild_name TEXT,
                    channel_name TEXT,
                    name TEXT,
                    discriminator TEXT,
                    message_count INTEGER,
                    message_timestamp TIMESTAMP,
                    etl_timestamp TIMESTAMP
                );
                '''
                conn.execute(text(create_table_query))
                print(f'Table raw_new.{table_name} created or already exists.')
                
                # Save data to the table
                if not df.empty:
                    df.to_sql(table_name, con=engine, if_exists='append', index=False, schema='raw_new')
                    print(f'Data saved to table raw_new.{table_name}.')
                else:
                    print(f'No data to save to raw_new.{table_name}.')
        except Exception as e:
            print(f'Error saving data to raw_new.{table_name}: {e}')

# Run the Discord client
async def main():
    intents = discord.Intents.default()
    intents.messages = True
    client = DiscordClient(intents=intents)
    await client.start(token)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())