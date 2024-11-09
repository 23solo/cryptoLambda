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
            sql = """
                    SELECT 
                        pa.userId, 
                        u.username, 
                        pa.cryptoSymbol, 
                        pa.targetPrice, 
                        pa.alertType 
                    FROM 
                        priceAlerts pa
                    JOIN 
                        user u ON pa.userId = u.id 
                    WHERE 
                        pa.status = 0
                    """
            cursor.execute(sql)
            results = cursor.fetchall()

            # Create a set to avoid duplicate API calls for the same crypto symbol
            crypto_symbols = set()
            alerts = []
            for row in results:
                user_id, email, crypto_symbol, target_price, alert_type = row
                crypto_symbols.add(crypto_symbol)
                alerts.append({
                    'userId': user_id,
                    'userEmail': email,
                    'crypto_symbol': crypto_symbol,
                    'target_price': target_price,
                    'alert_type': alert_type
                })

            print(f"Results is {results} & {crypto_symbols} {user_id}")
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
                print(f"current prices are {current_prices}")
                # Check if target prices are met
                for alert in alerts:
                    current_price = current_prices.get(alert['crypto_symbol'], {}).get('usd')
                    if current_price is not None:
                        if (alert['alert_type'] == 'above' and current_price >= alert['target_price']) or \
                           (alert['alert_type'] == 'below' and current_price <= alert['target_price']):
                            # Here you can implement the logic to notify the user
                            email_data = {
                                "to": alert['userEmail'],
                                "subject": "Price Alert Notification",
                                "text": f"The target price for {alert['crypto_symbol']} has been met. Current price: {current_price}. Target price: {alert['target_price']}."
                            }

                            # Send the email request
                            email_response = requests.post(
                                'http://ecs-nest-lb-2027848750.ap-south-1.elb.amazonaws.com/user/send-email',
                                json=email_data,
                                headers={"Content-Type": "application/json"}
                            )

                            # Check the response from the email service
                            if email_response.status_code == 200 or email_response.status_code == 201:
                                update_sql = "UPDATE priceAlerts SET status = 0 WHERE userId = %s AND cryptoSymbol = %s"
                                cursor.execute(update_sql, (alert['userId'], alert['crypto_symbol']))
                                connection.commit() 
                                print(f"Email sent successfully to {alert['userEmail']}.")
                            else:
                                print(f"Failed to send email to {alert['userEmail']}: {email_response.text}")
            else:
                print("No active alerts found.")

    finally:
        connection.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Function executed successfully!')
    }