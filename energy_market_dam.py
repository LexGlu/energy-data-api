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


async def download_file(url, xls_file):
    # async function to download xls file
    async with aiohttp.ClientSession() as session:
        async with session.get(url, verify_ssl = False) as resp:
            if resp.status == 200:
                with open(xls_file, 'wb') as f:
                    f.write(await resp.read())
            else:
                print(f'Error: {resp.status}')
                

def delete_file(file):
    # used to delete the file it we don't need it anymore (xls, csv)
    if os.path.exists(file):
        os.remove(file)
        

# create a function to check if the file exists and has data in it (not only header)
def check_file(xls_file, date):
    workbook = xlrd.open_workbook(xls_file, ignore_workbook_corruption=True)
    sheet = workbook.sheet_by_index(0)
    if sheet.nrows > 1:
        print(f'Data for {date} is available and ready to be processed')
        delete_file(xls_file)
        return workbook
    else:
        print(f'Data for {date} is not available yet')
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
        df.to_csv(csv_file, index=False, header=header)
        print(f'Processed data for {date} was successfully saved to csv file')
    except Exception as e2:
        print(f'Error while saving data to csv file: {e2}')
        delete_file(csv_file)
    

async def fetch_dam_data(date):
    # set url to download the file
    url = f'https://www.oree.com.ua/index.php/PXS/downloadxlsx/{date}/DAM/2'
    xls_file = f'dam_{date}.xls'
    csv_file = f'dam_{date}.csv'
    
    await download_file(url, xls_file)
    workbook = check_file(xls_file, date)
    if workbook:
        process_data(workbook, date, csv_file)


if __name__ == '__main__':
    # get tomorrow date in format 'dd.mm.yyyy'
    day_ahead_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%d.%m.%Y')
    day_ahead_date = '09.06.2023'
    asyncio.run(fetch_dam_data(day_ahead_date))
    