import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import random
import time
import os 
from pathlib import Path
import boto3 as aws
import json

stock_df=pd.DataFrame()

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
    'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'
]


download_dir="E:\\FeinAI\\NEW\\data\\selenium\\"
dir=Path(download_dir)

prefs = {
    "download.default_directory": download_dir,  # Custom download folder
    "download.prompt_for_download": False,       # No download prompt
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}


chrome_options = Options()
chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
chrome_options.add_experimental_option("prefs", prefs)
#chrome_options.add_argument("--headless")  # Run in headless mode
#chrome_options.add_argument("--disable-gpu")
#chrome_options.add_argument("--no-sandbox")



s3=aws.client('s3') 
bucket_name="feinai-aws-datapipeline"


def scrape(url,symbol):
    service = Service("D:\\chromedriver\\chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    try:
        driver.get(url)
        time.sleep(1)
        driver.find_element(By.ID,"loadHistoricalData").click()
        time.sleep(2)
        driver.find_element(By.ID,"oneW").click()
        driver.find_element(By.ID,"tradeDataFilter").click()
        time.sleep(2)
        download_block=driver.find_element(By.ID,"dwldcsv")
        download_block.click()
        time.sleep(1)
        driver.close()
        rename(symbol)
    except Exception as e:
        print(str(e))

def rename(symbol):
    files = [f for f in dir.iterdir() if f.is_file()]
    latest = max(files, key=lambda f: f.stat().st_mtime)
    same_folder = latest.parent
    new_name=same_folder /f"{symbol}.csv"
    if Path(new_name).exists():
                Path(new_name).unlink()
    os.rename(latest,new_name)
    concat_data(symbol)


def concat_data(symbol):
    global stock_df
    print("concating")
    df=pd.read_csv(f"{download_dir}{symbol}.csv")
    df.columns=df.columns.str.strip()
   

    df['Symbol']=symbol
    old_df=stock_df
    stock_df=pd.concat([old_df,df])
    print("concated")
   

"""
def uploadfiles():
    for file in dir.glob('*.csv'):
        key=f'stocks/{file.name}'
        s3.upload_file(str(file), bucket_name,key)
        print(f"Uploaded: {file.name}")
    """

def save_csv():
    print("saving")
    stock_df.to_csv(f'{download_dir}Stock_Data_combined.csv',index=False)
    print("saved")

"""
def club_and_upload():
    response = s3.list_objects_v2(Bucket=bucket_name)

    
    all csv files in csv_file
    

    csv_files = []
    if 'Contents' in response:
         for obj in response['Contents']:
              key=obj['Key']
              if key.endswiht('.csv'):
                   csv_files.append(key)


    if len(csv_files) == 0:
        uploadfiles()
"""

def csv_to_jsonl(filepath):
    """
    Args:
        filepath (str): The path to the Excel file.
    """
    try:
        df_csv = pd.read_csv(filepath)
        df_csv.columns=df_csv.columns.str.strip()

        instruction_text = "Provide the historical data of stocks for the given date"
        
        # Construct the JSONL file path
        base_name = os.path.splitext(os.path.basename(filepath))[0]  # Get filename without extension
        jsonl_file_path = os.path.join(os.path.dirname(filepath), f"{base_name}.jsonl")
        mf_name=base_name.replace('_',' ')
        with open(jsonl_file_path, "w", encoding="utf-8") as jsonl_file:
            for _, row in df_csv.iterrows():
                try:
                    # Extract stock symbol from the filename (remove extension)
                   # stock_symbol = row['symbol'][:-3]

                    entry = {
                        "instruction": instruction_text,
                        "input": f"Stock Symbol : {row['Symbol']}, Date: {row['Date']}",
                        "output": f"The OPEN and CLOSE prices for the stock were {row['OPEN']} and {row['close']} with a high and low of {row['HIGH']} and {row['LOW']}. The total number of trades were : {row['No of trades']} and volume was : {row['VOLUME']} with a Volume Weighted Average Price : {row['vwap']}. The total traded value for the day was : {row['VALUE']}"
                    }
                    jsonl_file.write(json.dumps(entry) + "\n")
                except KeyError as e:
                    print(f"Error: Column not found in row: {e}. Skipping this row in {filepath}.")
                except Exception as e:
                    print(f"An error occurred while processing a row in {filepath}: {e}")

        print(f"JSONL file saved as {jsonl_file_path}")

    except FileNotFoundError:
        print(f"Error: Excel file not found at '{filepath}'")
    except pd.errors.EmptyDataError:
        print(f"Warning: Excel file is empty: '{filepath}'")
    except Exception as e:
        print(f"An error occurred while reading or processing '{filepath}': {e}")



def main():
    df=pd.read_csv("NSE Symbols.csv")
    i=1
    for _,row in df.iterrows():
        symbol=row['Scrip']
        url=f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
        scrape(url,symbol)
        i=i+1
        if(i==5):
            save_csv()
            filepath=f"{download_dir}Stock_Data_combined.csv"
            csv_to_jsonl(filepath)
    
main()