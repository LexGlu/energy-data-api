import datetime
import pandas as pd
import xlrd
import aiohttp
import asyncio
import os
import schedule
import time

# to retrieve data from oree.com.ua we could have used web scraping that can handle javascript (selenium, puppeteer, etc.)
# but we can also use the url that is used by javascript to download the file directly (better solution)
# the url is https://www.oree.com.ua/index.php/PXS/downloadxlsx/09.06.2023/DAM/2
# to get the desired file we need to change the date parameter in the url {09.06.2023} to any date (historical data is also available)
# if there is no data for the date, the file will contain only header 
# xlrd is needed to read the xls file (pandas is raising an error due to the corrupted xls file)


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
    # used to delete the file it we don't need it anymore (xls, csv)
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
def process_data(workbook, date, csv_file):
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
        df.insert(0, 3, date)
        header = ['date', 'hour', 'price', 'volume']
        df.columns = header
        log_message(f'data for {date} was successfully processed')
    except Exception as e1:
        log_message(f'error while processing data: {e1}')
    try:
        df.to_csv(csv_file, index=False, header=header)
        log_message(f'processed data for {date} was successfully saved to csv file')
        # echo date to the 'status.log' to avoid processing the same data twice
        with open(f'{workdir}/status_log.txt', 'a') as f:
            f.write(f'{date}\n')
        
    except Exception as e2:
        log_message(f'error while saving data to csv file: {e2}')
        delete_file(csv_file)
    

async def fetch_dam_data(date):
    # set url to download the file
    url = f'https://www.oree.com.ua/index.php/PXS/downloadxlsx/{date}/DAM/2'
    xls_file = os.path.join(os.path.dirname(__file__), f'dam_{date}.xls')
    csv_file = os.path.join(os.path.dirname(__file__), f'dam_{date}.csv')
    
    await download_file(url, xls_file)
    workbook = data_available(xls_file, date)
    if workbook:
        process_data(workbook, date, csv_file)

def job_function(date):
    # get tomorrow date in format 'dd.mm.yyyy'

    # download data if it was not downloaded yet (check status_log.txt)
    if not data_downloaded(date):
        asyncio.run(fetch_dam_data(date))
    else:
        log_message(f'data for {date} was already downloaded and processed')
        schedule.cancel_job(job)

if __name__ == '__main__':
    workdir = os.path.dirname(__file__)
    day_ahead_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%d.%m.%Y')
    job = schedule.every(1).minute.do(job_function, day_ahead_date) # run the job every minute (can be modified to run every second, hour etc.)
    
    minute_limit = 30 # limit the number of minutes to run the script (can be modified or removed if needed)
    
    # script will run until the data is downloaded and processed OR until the time limit is reached (not to run forever if something goes wrong)
    while minute_limit > 0:
        n = schedule.idle_seconds() # returns the number of seconds until the next scheduled job
        if n is None:
            break
        elif n > 0:
            time.sleep(n) # sleep until the next job is scheduled to run
        print(f'Running script for {day_ahead_date} ... {minute_limit} tries left')
        schedule.run_pending()
    else:
        log_message(f'Script was stopped due to the time limit. Data for {day_ahead_date} was not downloaded!')
