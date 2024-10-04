import os
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# PostgreSQL connection settings
DB_URI = os.getenv('DB_URI')
engine = create_engine(DB_URI)

# Define the last 2 days range
two_days_ago = datetime.now() - timedelta(days=2)
current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def update_full_table(tmp_table, full_table):
    with engine.connect() as conn:
        # Step 1: Delete data from the full table for the last 2 days
        delete_query = f'''
        DELETE FROM raw_new.{full_table}
        WHERE etl_timestamp >= :two_days_ago;
        '''
        conn.execute(text(delete_query), {'two_days_ago': two_days_ago})
        print(f"Deleted last 2 days of data from {full_table}")

        # Step 2: Insert data from the tmp_ table to the full table, adding the etl_timestamp
        insert_query = f'''
        INSERT INTO raw_new.{full_table}
        SELECT *, :current_timestamp as etl_timestamp
        FROM raw_new.{tmp_table};
        '''
        conn.execute(text(insert_query), {
                     'current_timestamp': current_timestamp})
        print(f"Inserted last 2 days of data from {tmp_table} to {full_table}")


if __name__ == "__main__":
    update_full_table('tmp_stripe_all_payments', 'stripe_all_payments')
    update_full_table('tmp_stripe_one_time_customers',
                      'stripe_one_time_customers')
    update_full_table('tmp_discord_user_activity', 'discord_user_activity')
