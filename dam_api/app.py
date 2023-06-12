from flask import Flask, request, jsonify
import pandas as pd
import datetime
import os

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))  # add parent directory to sys.path
from database import get_data_for_date, get_data_for_date_range, table_exists, create_table


app = Flask(__name__)


@app.route('/dam_data', methods=['GET'])
def get_dam():
    
    def get_tomorrow_date():
        # get tomorrow date in output_format 'dd.mm.yyyy'
        tomorrow_date = (datetime.date.today() + datetime.timedelta(days=1))
        return tomorrow_date
    
    def str_to_date(date):
        return pd.to_datetime(date, format='%d.%m.%Y')
    
    def validate_date(date):
        try:
            str_to_date(date)
        except Exception as e:
            return jsonify({'error': f'Invalid date format for {date}: {e}\n Indicate date in format dd.mm.yyyy'}), 400
    
    # check if table exists, if not create it
    if not table_exists():
        create_table()
    
    # get parameters from the request
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    date = request.args.get('date')
    output_format = request.args.get('format')
    
    if output_format:
        if output_format.lower() not in ['json', 'csv']:
            return jsonify({'error': 'Invalid output format. Please use json or csv'}), 400
    if not output_format:
        output_format = 'json'
    
    if (start_date or end_date) and date:
        return jsonify({'error': 'Please use either date or date range'}), 400
    
    if start_date and not end_date:
        return jsonify({'error': 'Please provide end_date parameter'}), 400
    elif end_date and not start_date:
        return jsonify({'error': 'Please provide start_date parameter'}), 400
    
    if start_date and end_date:
        error = validate_date(start_date)
        if error:
            return error
        error = validate_date(end_date)
        if error:
            return error
    
        if str_to_date(start_date) > str_to_date(end_date):
            return jsonify({'error': 'start_date must be less than end_date'}), 400
        
        if str_to_date(end_date) > str_to_date(get_tomorrow_date()):
            return jsonify({'error': 'end_date must be not later than tomorrow date'}), 400
        
        # get data from the database for the date range
        df = get_data_for_date_range(start_date, end_date)
    
    if date:
        # check if date is in correct format, if not return error
        error = validate_date(date)
        if error:
            return error
        
        if str_to_date(date) > str_to_date(get_tomorrow_date()):
            return jsonify({'error': 'date must be not later than tomorrow date'}), 400
        
        # connect to the database and get data from there for the specified date
        df = get_data_for_date(date)
    
    # set default date to tomorrow if nor date nor date range is provided
    if not start_date and not end_date and not date:
        date = get_tomorrow_date()
        # connect to the database and get data from there for tomorrow date
        df = get_data_for_date(date)
    
    if df.empty:
        if date:
            try:
                date_str = date.strftime('%d.%m.%Y')
            except Exception as e:
                date_str = date
        else:
            date_str = f'{start_date} - {end_date}'
        return jsonify({'error': f'No data available for {date_str}'}), 404
    
    # change date format to string 'dd.mm.yyyy' for json and csv output (to be readable and consistent with input format)
    df['date'] = df['date'].apply(lambda x: x.strftime('%d.%m.%Y'))
    
    return df.to_json(orient='records') if output_format.lower() == 'json' else df.to_csv(index=False)
    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5555, debug=True)
    