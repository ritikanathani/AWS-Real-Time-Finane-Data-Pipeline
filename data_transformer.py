import yfinance as yf
import json
import boto3
from datetime import datetime, timedelta
import time

stream_name = 'kinesis-datastream-finance-data'
region = 'us-east-2'
kinesis_client = boto3.client('kinesis', region_name=region)

stocks = ['AMZN', 'BABA', 'WMT', 'EBAY', 'SHOP', 'TGT', 'BBY', 'HD', 'COST', 'KR']

def collect_stock_data():
    total_row_count = 0
    
    try:
        for stock_symbol in stocks:
            data = fetch_stock_data(stock_symbol)
            if data:
                send_to_kinesis(data)
                total_row_count += len(data)
        
        print(f"All data streamed. Total row count: {total_row_count}. Done.")
        return 'done'
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return 'error'

def fetch_stock_data(stock_symbol):
    try:
        end_date = datetime(2023, 12, 15)
        start_date = datetime(2023, 12, 4)
        
        stock = yf.Ticker(stock_symbol)
        
        total_data = []
        current_date = start_date
        
        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)
            historical_data = stock.history(start=current_date, end=next_date, interval='5m')

            if not historical_data.empty:
                for index, row in historical_data.iterrows():
                    high = row['High']
                    low = row['Low']
                    volatility = high - low
                    timestamp = index.strftime('%Y-%m-%d %H:%M:%S%z')

                    data_point = {
                        'high': float(high),
                        'low': float(low),
                        'volatility': float(volatility),
                        'ts': timestamp,
                        'name': stock_symbol
                    }

                    total_data.append(data_point)

            current_date = next_date

        return total_data
    
    except Exception as e:
        print(f"Error fetching data for {stock_symbol}: {e}")
        return None

def send_to_kinesis(data):
    try:
        for data_point in data:
            total_data = json.dumps(data_point) + "\n"
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=total_data.encode('utf-8'),
                PartitionKey=data_point['name']
            )
            print(f"Record sent to Kinesis at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. "
                  f"Data collected for {data_point['ts']}. "
                  f"SequenceNumber: {response['SequenceNumber']}"
            )
            time.sleep(0.05)
    
    except Exception as e:
        print(f"Error sending data to Kinesis: {e}")

if __name__ == "__main__":
    result = collect_stock_data()
    print(f"Result: {result}")

