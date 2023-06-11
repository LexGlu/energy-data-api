import datetime
import pandas as pd
import xlrd
import aiohttp
import asyncio
import os
import schedule
import time
from database import table_exists, create_table, insert_data

# to retrieve data from oree.com.ua we could have used web scraping that can handle javascript (selenium, puppeteer, etc.)
# but we can also use the url that is used by javascript to download the file directly (better solution)
# the url is https://www.oree.com.ua/index.php/PXS/downloadxlsx/09.06.2023/DAM/2
# to get the desired file we need to change the date parameter in the url {09.06.2023} to any date (historical data is also available)
# if there is no data for the date, the file will contain only header 
# xlrd is needed to read the xls file (pandas is raising an error due to the corrupted xls file)

workdir = os.path.dirname(__file__)

# check if the file was already downloaded and processed
def data_downloaded(date):
    with open(f'{workdir}/status_log.txt', 'r') as f:
        if date in f.read():
            return True
        else:
            return False
        

def log_message(message):
    with open(f'{workdir}/logs.txt', 'a') as f:
        timestamp = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        f.write(f'{timestamp}: {message}\n')
        
    
async def download_file(url, xls_file):
    # async function to download xls file
    async with aiohttp.ClientSession() as session:
        async with session.get(url, verify_ssl = False) as resp:
            if resp.status == 200:
                with open(xls_file, 'wb') as f:
                    f.write(await resp.read())
            else:
                log_message(f'error while downloading the file. Status code: {resp.status}')
                    

def delete_file(file):
    # used to delete the file it we don't need it anymore (xls file)
    if os.path.exists(file):
        os.remove(file)
        

# create a function to check if the data is available (if the file contains more than 1 row, not only header)
def data_available(xls_file, date):
    workbook = xlrd.open_workbook(xls_file, ignore_workbook_corruption=True)
    sheet = workbook.sheet_by_index(0)
    if sheet.nrows > 1:
        log_message(f'data for {date} is available and ready to be processed')
        delete_file(xls_file)
        return workbook
    else:
        log_message(f'data for {date} is not available yet')
        delete_file(xls_file)
        return None        
    

# create a function to process the data and save it to csv file
def process_data(workbook, date):
    try:
        df = pd.read_excel(workbook, sheet_name=0, header=None, usecols=[0, 1, 2], skiprows=1)
        workbook.release_resources()
        df[0] = df[0].str[:2]
        df[1] = df[1].str.replace(' ', '')
        df[2] = df[2].str.replace(' ', '')
        df[1] = df[1].str.replace(',', '.') 
        df[2] = df[2].str.replace(',', '.')
        df[0] = df[0].astype(int)
        df[1] = df[1].astype(float)
        df[2] = df[2].astype(float)
        date = datetime.datetime.strptime(date, '%d.%m.%Y').date()
        df.insert(0, 3, date)
        header = ['date', 'hour', 'price', 'volume']
        df.columns = header
        # log message with date string to avoid confusion
        log_message(f'data for {date.strftime("%d.%m.%Y")} was successfully processed')
    except Exception as e1:
        log_message(f'error while processing data: {e1}')
    try:
        exists = table_exists()
        
        if not exists:
            # create table if it does not exist
            create_table()
            log_message('table dam_data was successfully created')
        
        # insert data to the table
        for i, row in df.iterrows():
            insert_data(row['date'], row['hour'], row['price'], row['volume'])
        log_message(f'data for {date.strftime("%d.%m.%Y")} was successfully saved to the database')
        
        # echo date to the 'status.log' to avoid processing the same data twice
        with open(f'{workdir}/status_log.txt', 'a') as f:
            f.write(f'{date.strftime("%d.%m.%Y")}\n')
        
    except Exception as e2:
        log_message(f'error while saving data to database: {e2}')
    

async def fetch_dam_data(date):
    # set url to download the file
    url = f'https://www.oree.com.ua/index.php/PXS/downloadxlsx/{date}/DAM/2'
    xls_file = os.path.join(os.path.dirname(__file__), f'dam_{date}.xls')
    
    await download_file(url, xls_file)
    workbook = data_available(xls_file, date)
    if workbook:
        process_data(workbook, date)

def job_function(date):
    # download data if it was not downloaded yet (check status_log.txt)
    if not data_downloaded(date):
        asyncio.run(fetch_dam_data(date))
    else:
        log_message(f'data for {date} was already downloaded and processed')
        schedule.cancel_job(job)

if __name__ == '__main__':
    tomorrow_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%d.%m.%Y')
    job = schedule.every(1).minute.do(job_function, tomorrow_date) # run the job every minute (can be modified to run every second, hour etc.)
    
    minute_limit = 30 # limit the number of minutes to run the script (can be modified or removed if needed)
    
    # script will run until the data is downloaded and processed OR until the time limit is reached (not to run forever if something goes wrong)
    while minute_limit > 0:
        n = schedule.idle_seconds() # returns the number of seconds until the next scheduled job
        if n is None:
            break
        elif n > 0:
            time.sleep(n) # sleep until the next job is scheduled to run
        minute_limit -= 1
        print(f'Running script for {tomorrow_date} ... {minute_limit} iteration(s) left')
        schedule.run_pending()
    else:
        log_message(f'Script was stopped due to the time limit. Data for {tomorrow_date} was not downloaded!')
