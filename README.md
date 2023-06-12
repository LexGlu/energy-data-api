# Test task for the position of Software Engineer

## Task description

### Task 1
Prepare a script that collects hourly market closing data (price and volume) every day after the day-ahead electricity market closes.
The market closes by 13:00, usually at 12:30.
Offer a method of data collection as quickly as possible after closing (no fixed time).
Propose a data storage structure in a relational database.

### Task 2
Implement an API endpoint to receive data on the closing of the DAM for the given date (using Flask).
For simplicity, you can take the data from task 1 and put it locally in a file that the API will read.

## Deployment
To run both scripts, you need to have Docker and Docker Compose installed on your machine. You can check if you have them installed by running the following commands:
```bash
docker -v
docker-compose -v
```
If you don't have them installed, please follow the instructions on the official Docker website: https://docs.docker.com/get-docker/

To run the solution, please follow the steps below:
1. Clone the repository to your machine:
```bash
git clone https://github.com/LexGlu/energy-data-api.git
```
2. Go to the project directory:
```bash
cd energy-data-api
```
3. Run the following command:
```bash
docker-compose up -d
```
4. Wait for the containers to be built and started. You can check the status of the containers with the following command:
```bash
docker-compose ps
```
5. When the containers are up and running, you can load some data manually for specified date range (to load some historical data and test the API endpoint), by running command, where you will prompted to enter start date and end date in the format 'dd.mm.yyyy' (e.g. '01.06.2023'), as follows:
```bash
docker exec -it flask_api python get_dam_range.py
```
6. Now you can make requests to the API endpoint. The endpoint is available at http://localhost:5555/dam_data. You can make requests to the endpoint using curl or any other tool for making HTTP requests. Examples of the requests are provided in the section [Solution description](#solution-description).

7. You can check the data in the database using GUI for the database. It is available at http://localhost:8080. You can use credentials from the file [.env](./.env) to login to the database and check the data.


## Solution description
1. Container 'flask_api' will run python [script](./get_dam.py) to fetch market closing data from https://www.oree.com.ua every day at 12:30. This script will make requests every 1 minute until it gets the data or until some defined time limit. The interval for making requests (1 minute by default) or time limit (30 minutes by default) can be changed in the [script](./get_dam.py) file (line 121 and 123).
2. [Script](./get_dam.py) will save the data to the database. The database is running in the container 'postgres_db'.
3. Database schema for storing the data is the following:
- table 'dam_data' with columns: 'id' (primary key), 'date' (date), 'hour' (integer), 'price' (numeric), 'volume' (numeric).
4. Container 'flask_api' will run Flask application that will provide API endpoint to get the data from the database. The endpoint is '/dam_data' and it accepts GET requests. The endpoint accepts the following query parameters:
- 'date' - date in format 'dd.mm.yyyy' (e.g. '13.06.2023'). If the date is not specified, the data for tomorrow date will be returned.
- 'start_date' and 'end_date' - dates in format 'dd.mm.yyyy' (e.g. '13.06.2023'). If both 'start_date' and 'end_date' are specified, the data for the specified period will be returned.
- 'format' - format of the response. The following formats are supported: 'json' (default), and 'csv'.
Examples of the requests:
```bash
curl -X GET http://localhost:5555/dam_data
curl -X GET http://localhost:5555/dam_data?date=13.06.2023
curl -X GET http://localhost:5555/dam_data?start_date=01.06.2023&end_date=13.06.2023
curl -X GET http://localhost:5555/dam_data?start_date=01.06.2023&end_date=13.06.2023&format=csv
```

## Files description
- [docker-compose.yml](./docker-compose.yml) - docker-compose file to run the solution as multi container application: 'flask_api', 'postgres_db' and 'adminer' (GUI for database).
- [Dockerfile](./Dockerfile) - Dockerfile to build the image for the container 'flask_api'.
- [requirements.txt](./requirements.txt) - file with python dependencies.
- [get_dam.py](./get_dam.py) - python script to fetch market closing data from https://www.oree.com.ua and save it to the database.
- [get_dam_range.py](./get_dam_range.py) - python script to fetch market closing data for the date range and save it to the database.
- [app.py](./dam_api/app.py) - python script to run Flask App with API endpoint to get the data from the database.
- [.env](./.env) - file with environment variables for database connection.
- [wrapper.sh](./wrapper.sh) - bash script to run python script [get_dam.py](./get_dam.py) in the container 'flask_api' (used to load environment variables from the file [.env](./.env) to cron job).
- [crontab](./crontab) - file with cron job to run the script [get_dam.py](./get_dam.py) (via wrapper.sh) every day at 12:30. Time indicated in the file is UTC time (9:30 UTC = 12:30 Kyiv time).
- [logs.txt](./logs.txt) - file with logs of execution of the script [get_dam.py](./get_dam.py).
- [status_log.txt](./status_log.txt) - file with dates for which data was successfully fetched (in order not to make queries to the database if data is already there).