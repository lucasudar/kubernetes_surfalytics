import os
import stripe
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# PostgreSQL connection settings
DB_URI = os.getenv('DB_URI')
engine = create_engine(DB_URI)

# Stripe API key
stripe.api_key = os.getenv('STRIPE_SECRET_KEY')

# Set the date range for the last 2 days
two_days_ago = datetime.now() - timedelta(days=2)


def fetch_customers_with_one_time_purchases():
    customers = []
    for charge in stripe.Charge.list(limit=100).auto_paging_iter():
        if charge.customer:
            charge_date = datetime.fromtimestamp(charge.created)
            if charge_date >= two_days_ago:
                customer_details = stripe.Customer.retrieve(charge.customer)
                customers.append({
                    'customer_id': charge.customer,
                    'email': customer_details.email,
                    'name': customer_details.name,
                    'amount': charge.amount / 100,
                    'currency': charge.currency,
                    'transaction_timestamp': charge_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'description': charge.description,
                    'payment_status': charge.status
                })
    return customers


def fetch_all_payments_for_customers():
    payments = []
    for customer in stripe.Customer.list(limit=100).auto_paging_iter():
        for charge in stripe.Charge.list(customer=customer.id, limit=100).auto_paging_iter():
            charge_date = datetime.fromtimestamp(charge.created)
            if charge_date >= two_days_ago:
                payments.append({
                    'customer_id': customer.id,
                    'email': customer.email,
                    'name': customer.name,
                    'amount': charge.amount / 100,
                    'currency': charge.currency,
                    'transaction_timestamp': charge_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'description': charge.description,
                    'payment_status': charge.status
                })
    return payments


def save_to_postgres(df, table_name):
    with engine.connect() as conn:
        # Truncate the table before inserting new data
        truncate_table_query = f'''
        TRUNCATE TABLE raw_new.{table_name};
        '''
        conn.execute(text(truncate_table_query))
        print(f'Table raw_new.{table_name} truncated.')

        # Save data to the table
        if not df.empty:
            df.to_sql(table_name, con=engine, if_exists='append',
                      index=False, schema="raw_new")
            print(f'Data saved to table raw_new.{table_name}.')
        else:
            print(f'No data to save to raw_new.{table_name}.')


def run_stripe_dump():
    # Fetch and save customers with one-time purchases
    one_time_customers = fetch_customers_with_one_time_purchases()
    df_one_time = pd.DataFrame(one_time_customers)
    save_to_postgres(df_one_time, 'tmp_stripe_one_time_customers')

    # Fetch and save all payments for customers
    all_payments = fetch_all_payments_for_customers()
    df_payments = pd.DataFrame(all_payments)
    save_to_postgres(df_payments, 'tmp_stripe_all_payments')


if __name__ == '__main__':
    run_stripe_dump()
