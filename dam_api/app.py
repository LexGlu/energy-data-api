from flask import Flask, request, jsonify
import pandas as pd
import datetime
import os

app = Flask(__name__)
dam_data = os.path.join(os.path.dirname(__file__), 'dam_data.csv')

@app.route('/dam_data', methods=['GET'])
def get_dam():
    
    def filter_by_date(df, date):
        df = df[df['date'] == date]
        return df
    
    def filter_by_date_range(df, start_date, end_date):
        start_date = str_to_date(start_date)
        end_date = str_to_date(end_date)
        # i need to convert date column to datetime to be able to filter by date range
        df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
        # filter by date range
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        # convert date column back to string
        df['date'] = df['date'].dt.strftime('%d.%m.%Y')
        return df
    
    def get_tomorrow_date():
        # get tomorrow date in output_format 'dd.mm.yyyy'
        tomorrow_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%d.%m.%Y')
        return tomorrow_date
    
    def str_to_date(date):
        return pd.to_datetime(date, format='%d.%m.%Y')
    
    def validate_date(date):
        try:
            str_to_date(date)
        except Exception as e:
            return jsonify({'error': f'Invalid date format for {date}: {e}\n Indicate date in format dd.mm.yyyy'}), 400
    
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
        
        df = pd.read_csv(dam_data, header=0)
        df = filter_by_date_range(df, start_date, end_date)
    
    if date:
        # check if date is in correct format, if not return error
        error = validate_date(date)
        if error:
            return error
        
        if str_to_date(date) > str_to_date(get_tomorrow_date()):
            return jsonify({'error': 'date must be not later than tomorrow date'}), 400
        
        df = pd.read_csv(dam_data, header=0)
        df = filter_by_date(df, date)
    
    # set default date to tomorrow if nor date nor date range is provided
    if not start_date and not end_date and not date:
        date = get_tomorrow_date()
        df = pd.read_csv(dam_data, header=0)
        df = filter_by_date(df, date)
    
    if df.empty:
        return jsonify({'error': f'Data is not available.'}), 404
    
    # some logging for debugging purposes
    app.logger.info(f'Format: {output_format}')
    app.logger.info(f'Start date: {start_date}')
    app.logger.info(f'End date: {end_date}')
    app.logger.info(f'Date: {date}')
    app.logger.info(f'Number of rows: {df.shape[0]}')
    
    return df.to_json(orient='records') if output_format == 'json' else df.to_csv(index=False)
    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5555, debug=True)
    