import requests
import time
import json
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import os

def fetch_cryptoquant_data(api_key, exchange="okx", window="hour", start_date="2020-04-01", end_date="2024-01-01"):

    start_timestamp = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple()) * 1000)
    end_timestamp = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple()) * 1000)
    
    url = "https://api.datasource.cybotrade.rs/cryptoquant/btc/exchange-flows/inflow"
    
    params = {
        "exchange": exchange,
        "window": window,
        "start_time": start_timestamp,
        "end_time": end_timestamp
    }
    
    headers = {"X-API-Key": api_key}
    
    print(f"Fetching data for {exchange} from {start_date} to {end_date}...")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Process the response
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully received data with {len(data.get('data', []))} records")
            return data
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        return None

def parse_cryptoquant_data(data_input):

    if data_input is None:
        print("No data to parse")
        return pd.DataFrame()
    
    if isinstance(data_input, dict) and 'data' in data_input:
        data_items = data_input['data']
        print(f"Parsing {len(data_items)} data items from complete response")
    else:

        if isinstance(data_input, str):

            import re
            pattern = r'\{[^{}]*\}'
            matches = re.findall(pattern, data_input)
            data_items = []
            for match in matches:
                try:
                    data_items.append(json.loads(match))
                except json.JSONDecodeError:
                    continue
            print(f"Extracted {len(data_items)} data items from string")
        else:

            data_items = data_input
            print(f"Processing {len(data_items)} data items from list")
    
    if not data_items:
        print("No data items were found")
        return pd.DataFrame()
    
    # Create DataFrame
    df = pd.DataFrame(data_items)
    
    if 'start_time' in df.columns:
        df['start_time'] = pd.to_datetime(df['start_time'], unit='ms')
    
    # Make sure datetime column is properly formatted
    if 'datetime' not in df.columns and 'start_time' in df.columns:
        # Format the start_time as a datetime string
        df['datetime'] = df['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    numeric_columns = ['inflow_mean', 'inflow_mean_ma7', 'inflow_top10', 'inflow_total']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].round(4)
    
    # Reorder columns for better readability
    ordered_columns = ['datetime', 'start_time']
    for col in numeric_columns:
        if col in df.columns:
            ordered_columns.append(col)
    
    available_columns = [col for col in ordered_columns if col in df.columns]
    df = df[available_columns]
    
    print(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df

def analyze_btc_inflow(df):

    if df.empty:
        return {"error": "No data available for analysis"}
    
    if 'datetime' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['datetime']):
        df['datetime'] = pd.to_datetime(df['datetime'])
    
    results = {
        'total_records': len(df),
        'date_range': {
            'start': str(df['datetime'].min()),
            'end': str(df['datetime'].max())
        },
        'summary_stats': {}
    }
    

    numeric_columns = ['inflow_mean', 'inflow_mean_ma7', 'inflow_top10', 'inflow_total']
    for col in numeric_columns:
        if col in df.columns:

            stats_dict = df[col].describe().to_dict()

            results['summary_stats'][col] = {k: float(v) for k, v in stats_dict.items()}
    
    # Find top inflow events
    if 'inflow_total' in df.columns:
        top_events = df.nlargest(5, 'inflow_total')
        results['top_inflow_events'] = []
        for _, row in top_events.iterrows():
            event = {
                'datetime': str(row['datetime']),
                'inflow_total': float(row['inflow_total'])
            }
            results['top_inflow_events'].append(event)
    
    return results

def visualize_data(df, output_dir="output"):

    if df.empty:
        print("No data available for visualization")
        return
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    if 'datetime' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['datetime']):
        df['datetime'] = pd.to_datetime(df['datetime'])
    elif 'start_time' in df.columns:
        df['datetime'] = df['start_time']
    
    df_plot = df.set_index('datetime')
    
    if 'inflow_total' in df.columns:
        plt.figure(figsize=(12, 6))
        df_plot['inflow_total'].plot(title='Bitcoin Total Inflow Over Time')
        plt.ylabel('Total Inflow (BTC)')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'total_inflow_over_time.png'))
        plt.close()
        
        if 'inflow_mean_ma7' in df.columns:
            plt.figure(figsize=(12, 6))
            df_plot['inflow_mean_ma7'].plot(title='Bitcoin Inflow 7-Day Moving Average')
            plt.ylabel('7-Day MA Inflow (BTC)')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'inflow_7day_ma.png'))
            plt.close()
        
        if 'inflow_mean' in df.columns and 'inflow_top10' in df.columns:
            plt.figure(figsize=(12, 6))
            df_plot[['inflow_mean', 'inflow_top10']].plot(title='Mean vs Top 10 Inflow Comparison')
            plt.ylabel('Inflow (BTC)')
            plt.grid(True)
            plt.legend(['Mean Inflow', 'Top 10 Inflow'])
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'mean_vs_top10_inflow.png'))
            plt.close()
    
    print(f"Visualizations saved to {output_dir}")

def main():
    # Define your API key
    api_key = input("Enter your CryptoQuant API key: ")
    
    # Define parameters
    exchange = input("Enter exchange name (default: okx): ") or "okx"
    start_date = input("Enter start date (YYYY-MM-DD) (default: 2020-04-01): ") or "2020-04-01"
    end_date = input("Enter end date (YYYY-MM-DD) (default: 2024-01-01): ") or "2024-01-01"
    
    # Fetch data
    data = fetch_cryptoquant_data(api_key, exchange, "hour", start_date, end_date)
    
    # Parse data
    df = parse_cryptoquant_data(data)
    
    if not df.empty:
        # Save raw data to CSV
        output_file = f"btc_{exchange}_inflow_{start_date}_to_{end_date}.csv"
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        
        # Analyze data
        analysis = analyze_btc_inflow(df)
        
        # Save analysis to JSON
        analysis_file = f"btc_{exchange}_analysis_{start_date}_to_{end_date}.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"Analysis saved to {analysis_file}")
        
        visualize_data(df)
        
        print("\n===== Summary =====")
        print(f"Total records: {analysis['total_records']}")
        print(f"Date range: {analysis['date_range']['start']} to {analysis['date_range']['end']}")
        
        if 'inflow_total' in analysis['summary_stats']:
            print("\nTotal Inflow Statistics:")
            stats = analysis['summary_stats']['inflow_total']
            print(f"  Mean: {stats['mean']:.2f} BTC")
            print(f"  Min: {stats['min']:.2f} BTC")
            print(f"  Max: {stats['max']:.2f} BTC")
            print(f"  25%: {stats['25%']:.2f} BTC")
            print(f"  50%: {stats['50%']:.2f} BTC")
            print(f"  75%: {stats['75%']:.2f} BTC")
        
        if 'top_inflow_events' in analysis:
            print("\nTop 5 Inflow Events:")
            for i, event in enumerate(analysis['top_inflow_events'], 1):
                print(f"  {i}. {event['datetime']}: {event['inflow_total']:.2f} BTC")
    else:
        print("No data to analyze.")

if __name__ == "__main__":
    main()
