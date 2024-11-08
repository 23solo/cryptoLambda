import json
import pymysql
import requests
import os

# Database connection settings
RDS_HOST = os.getenv('RDS_HOST')
PASSWORD = os.getenv('DB_PASSWORD')
USERNAME = os.getenv('DB_USERNAME')
DATABASE = os.getenv('DB_NAME')

def lambda_handler(event, context):
    # Connect to the MySQL database
    connection = pymysql.connect(
        host=RDS_HOST,
        user=USERNAME,
        password=PASSWORD,
        database=DATABASE
    )
    
    try:
        with connection.cursor() as cursor:
            # Query to get all price alerts with status 0
            sql = "SELECT user_id, crypto_symbol, target_price, alert_type FROM price_alerts WHERE status = 0"
            cursor.execute(sql)
            results = cursor.fetchall()

            # Create a set to avoid duplicate API calls for the same crypto symbol
            crypto_symbols = set()
            alerts = []

            for row in results:
                user_id, crypto_symbol, target_price, alert_type = row
                crypto_symbols.add(crypto_symbol)
                alerts.append({
                    'user_id': user_id,
                    'crypto_symbol': crypto_symbol,
                    'target_price': target_price,
                    'alert_type': alert_type
                })

            # Fetch current prices for the unique crypto symbols
            if crypto_symbols:
                response = requests.get(
                    'https://api.coingecko.com/api/v3/simple/price',
                    params={
                        'ids': ','.join(crypto_symbols),
                        'vs_currencies': 'usd'
                    }
                )
                current_prices = response.json()
                # Check if target prices are met
                for alert in alerts:
                    current_price = current_prices.get(alert['crypto_symbol'], {}).get('usd')
                    if current_price is not None:
                        if (alert['alert_type'] == 'above' and current_price >= alert['target_price']) or \
                           (alert['alert_type'] == 'below' and current_price <= alert['target_price']):
                            # Here you can implement the logic to notify the user
                            print(f"Alert for user {alert['user_id']}: {alert['crypto_symbol']} has met the target price of {alert['target_price']} (Current: {current_price})")
            else:
                print("No active alerts found.")

    finally:
        connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Function executed successfully!')
    }