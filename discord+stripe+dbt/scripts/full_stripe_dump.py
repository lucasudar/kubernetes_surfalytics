import os
import stripe
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# PostgreSQL connection settings
DB_URI = os.getenv('DB_URI')
engine = create_engine(DB_URI)

# Stripe API key
stripe.api_key = os.getenv('STRIPE_SECRET_KEY')


def fetch_customers_with_subscriptions():
    customers = []
    for customer in stripe.Customer.list(limit=100).auto_paging_iter():
        subscriptions = stripe.Subscription.list(customer=customer.id, limit=1)
        if subscriptions['data']:
            subscription = subscriptions['data'][0]
            customers.append({
                'customer_id': customer.id,
                'email': customer.email,
                'name': customer.name,
                'subscription_status': subscription['status'],
                'subscription_amount': subscription['items']['data'][0]['plan']['amount'] / 100,
                'subscription_currency': subscription['items']['data'][0]['plan']['currency']
            })
    return customers


def fetch_customers_with_one_time_purchases():
    customers = []
    for charge in stripe.Charge.list(limit=100).auto_paging_iter():
        if charge.customer:
            customer_details = stripe.Customer.retrieve(charge.customer)
            customers.append({
                'customer_id': charge.customer,
                'email': customer_details.email,
                'name': customer_details.name,
                'amount': charge.amount / 100,
                'currency': charge.currency,
                'transaction_timestamp': datetime.fromtimestamp(charge.created).strftime('%Y-%m-%d %H:%M:%S'),
                'description': charge.description,
                'payment_status': charge.status
            })
    return customers


def fetch_all_payments_for_customers():
    payments = []
    for customer in stripe.Customer.list(limit=100).auto_paging_iter():
        for charge in stripe.Charge.list(customer=customer.id, limit=100).auto_paging_iter():
            payments.append({
                'customer_id': customer.id,
                'email': customer.email,
                'name': customer.name,
                'amount': charge.amount / 100,
                'currency': charge.currency,
                'transaction_timestamp': datetime.fromtimestamp(charge.created).strftime('%Y-%m-%d %H:%M:%S'),
                'description': charge.description,
                'payment_status': charge.status
            })
    return payments


def save_to_postgres(df, table_name, schema):
    with engine.connect() as conn:
        # Разные схемы для таблиц в зависимости от типа данных
        if schema == 'subscriptions':
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS raw_new.{table_name} (
                customer_id TEXT,
                email TEXT,
                name TEXT,
                subscription_status TEXT,
                subscription_amount REAL,
                subscription_currency TEXT
            );
            '''
        elif schema == 'one_time':
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS raw_new.{table_name} (
                customer_id TEXT,
                email TEXT,
                name TEXT,
                amount REAL,
                currency TEXT,
                transaction_timestamp TIMESTAMP,
                description TEXT,
                payment_status TEXT
            );
            '''
        elif schema == 'all_payments':
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS raw_new.{table_name} (
                customer_id TEXT,
                email TEXT,
                name TEXT,
                amount REAL,
                currency TEXT,
                transaction_timestamp TIMESTAMP,
                description TEXT,
                payment_status TEXT
            );
            '''
        else:
            raise ValueError(f'Unknown schema: {schema}')

        conn.execute(text(create_table_query))
        print(f'Table raw_new.{table_name} created or already exists.')

        # Сохранение данных в таблицу
        if not df.empty:
            df.to_sql(table_name, con=engine, if_exists='append',
                      index=False, schema="raw_new")
            print(f'Data saved to table raw_new.{table_name}.')
        else:
            print(f'No data to save to raw_new.{table_name}.')


def run_stripe_dump():
    # Fetch and save customers with subscriptions
    subscription_customers = fetch_customers_with_subscriptions()
    df_subscriptions = pd.DataFrame(subscription_customers)
    save_to_postgres(df_subscriptions,
                     'stripe_subscription_customers', schema='subscriptions')

    # Fetch and save customers with one-time purchases
    one_time_customers = fetch_customers_with_one_time_purchases()
    df_one_time = pd.DataFrame(one_time_customers)
    save_to_postgres(df_one_time, 'stripe_one_time_customers',
                     schema='one_time')

    # Fetch and save all payments for customers
    all_payments = fetch_all_payments_for_customers()
    df_payments = pd.DataFrame(all_payments)
    save_to_postgres(df_payments, 'stripe_all_payments', schema='all_payments')


if __name__ == '__main__':
    run_stripe_dump()
