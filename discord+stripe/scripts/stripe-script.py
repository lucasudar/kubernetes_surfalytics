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
                'created': datetime.fromtimestamp(charge.created).strftime('%Y-%m-%d %H:%M:%S'),
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
                'created': datetime.fromtimestamp(charge.created).strftime('%Y-%m-%d %H:%M:%S'),
                'description': charge.description,
                'payment_status': charge.status
            })
    return payments

def save_to_postgres(df, table_name):
    with engine.connect() as conn:
        # Drop the table if it exists
        conn.execute(text(f'DROP TABLE IF EXISTS {table_name};'))

        # Create the table with the new schema
        create_table_query = f'''
        CREATE TABLE {table_name} (
            customer_id TEXT,
            email TEXT,
            name TEXT,
            subscription_status TEXT,
            subscription_amount REAL,
            subscription_currency TEXT,
            amount REAL,
            currency TEXT,
            created TIMESTAMP,
            description TEXT,
            payment_status TEXT
        );
        '''
        conn.execute(text(create_table_query))

        df.to_sql(table_name, con=engine, if_exists='append', index=False, schema='raw_new')
        print(f'Data saved to table {table_name}')

if __name__ == '__main__':
    # Fetch and save customers with subscriptions
    subscription_customers = fetch_customers_with_subscriptions()
    df_subscriptions = pd.DataFrame(subscription_customers)
    save_to_postgres(df_subscriptions, 'stripe_subscription_customers')

    # Fetch and save customers with one-time purchases
    one_time_customers = fetch_customers_with_one_time_purchases()
    df_one_time = pd.DataFrame(one_time_customers)
    save_to_postgres(df_one_time, 'stripe_one_time_customers')

    # Fetch and save all payments for customers
    all_payments = fetch_all_payments_for_customers()
    df_payments = pd.DataFrame(all_payments)
    save_to_postgres(df_payments, 'stripe_all_payments')