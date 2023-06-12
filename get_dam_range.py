import get_dam
import datetime
import asyncio

def get_dam_for_date_range(start_date, end_date):
    # convert dates to datetime objects
    start_date = datetime.datetime.strptime(start_date, '%d.%m.%Y')
    end_date = datetime.datetime.strptime(end_date, '%d.%m.%Y')
    
    for i in range((end_date - start_date).days + 1):
        date = start_date + datetime.timedelta(days=i)
        
        if not get_dam.data_downloaded(date.strftime('%d.%m.%Y')):
            asyncio.run(get_dam.fetch_dam_data(date.strftime('%d.%m.%Y')))
            print(f'trying to get data for {date.strftime("%d.%m.%Y")}')
        else:
            print(f'data for {date.strftime("%d.%m.%Y")} was already downloaded and processed')
    print('finished: check logs.txt for details')
    

if __name__ == '__main__':
    start_date = input('Enter start date in format dd.mm.yyyy: ')
    end_date = input('Enter end date in format dd.mm.yyyy: ')
    # validate dates
    try:
        datetime.datetime.strptime(start_date, '%d.%m.%Y')
        datetime.datetime.strptime(end_date, '%d.%m.%Y')
    except ValueError:
        raise ValueError("Incorrect data format, should be dd.mm.yyyy")
    get_dam_for_date_range(start_date, end_date)
        