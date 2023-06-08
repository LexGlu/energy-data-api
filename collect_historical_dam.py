import datetime
import pandas as pd
import xlrd
import aiohttp
import asyncio
import os


# to retrieve data from oree.com.ua we could have used web scraping that can handle javascript (selenium, puppeteer, etc.)
# but we can also use the url that is used by javascript to download the file directly (better solution)
# the url is https://www.oree.com.ua/index.php/PXS/downloadxlsx/09.06.2023/DAM/2
# to get the desired file we need to change the date parameter in the url {09.06.2023} to any date (historical data is also available)
# if there is no data for the date, the file will contain only header 
# xlrd is needed to read the xls file (pandas is raising an error due to the corrupted xls file)


async def download_file(url, xls):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, verify_ssl = False) as resp:
            if resp.status == 200:
                with open(xls, 'wb') as f:
                    f.write(await resp.read())
            else:
                print(f'Error: {resp.status}')
                

def delete_file(file):
    if os.path.exists(file):
        os.remove(file)
        

# create a function to check if the file exists and has data in it (not only header)
def check_file(xls_file, day_ahead):
    workbook = xlrd.open_workbook(xls_file, ignore_workbook_corruption=True)
    sheet = workbook.sheet_by_index(0)
    if sheet.nrows > 1:
        print(f'Data for {day_ahead} is available')
        delete_file(xls_file)
        return workbook
    else:
        print(f'Data for {day_ahead} is not available yet')
        # delete file if it exists
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
        print('Data was successfully processed')
    except Exception as e1:
        print(f'Error while processing data: {e1}')
    try:
        # append data to csv file or create a new file if it doesn't exist
        with open(csv_file, 'a+') as f:
            # check if the file is empty
            if os.stat(csv_file).st_size == 0:
                df.to_csv(f, index=False, header=header)
            else:
                df.to_csv(f, index=False, header=False)
        print(f'Processed data for {date} was successfully saved to csv file')
    except Exception as e2:
        print(f'Error while saving data to csv file: {e2}')
        

def dates_range(start_date, end_date):
    # create a generator to iterate over dates range
    start = datetime.datetime.strptime(start_date, '%d.%m.%Y')
    end = datetime.datetime.strptime(end_date, '%d.%m.%Y')
    step = datetime.timedelta(days=1)
    while start <= end:
        yield start.strftime('%d.%m.%Y')
        start += step


async def fetch_dam_data(start_date, end_date):
    # set url to download the file

    
    for i in dates_range(start_date, end_date):
        print(f'Processing data for {i}')
        url = f'https://www.oree.com.ua/index.php/PXS/downloadxlsx/{i}/DAM/2'
        xls_file = f'dam_{i}.xls'
        csv_file = f'dam_data.csv'
        await download_file(url, xls_file)
        workbook = check_file(xls_file, i)
        if workbook:
            process_data(workbook, i, csv_file)
            

if __name__ == '__main__':
    start_date = '01.06.2023'
    end_date = '09.06.2023'
    asyncio.run(fetch_dam_data(start_date, end_date))
    