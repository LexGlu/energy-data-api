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
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
        return df
    
    def get_tomorrow_date():
        # get tomorrow date in output_format 'dd.mm.yyyy'
        tomorrow_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%d.%m.%Y')
        tomorrow_date = datetime.date.today().strftime('%d.%m.%Y') # for testing
        return tomorrow_date
        
    
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
        if start_date > end_date:
            return jsonify({'error': 'start_date must be less than end_date'}), 400
        
        df = pd.read_csv(dam_data, header=0)
        df = filter_by_date_range(df, start_date, end_date)
    
    if date:
        df = pd.read_csv(dam_data, header=0)
        df = filter_by_date(df, date)
    
    # set default date to tomorrow if nor date nor date range is provided
    if not start_date and not end_date and not date:
        date = get_tomorrow_date()
        df = pd.read_csv(dam_data, header=0)
        df = filter_by_date(df, date)
    
    if df.empty:
        return jsonify({'error': 'Data is not available yet. Wait until 12:30 - 13:00'}), 400
    
    # some logging for debugging purposes
    app.logger.info(f'Format: {output_format}')
    app.logger.info(f'Start date: {start_date}')
    app.logger.info(f'End date: {end_date}')
    app.logger.info(f'Date: {date}')
    app.logger.info(f'Number of rows: {df.shape[0]}')
    
    return df.to_json(orient='records') if output_format == 'json' else df.to_csv(index=False)
    
    
if __name__ == '__main__':
    app.run(debug=True)
    