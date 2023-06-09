FROM python:3.11.4-slim-buster

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /usr/src/app

RUN apt-get update && pip install --upgrade pip && apt-get -y install cron
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
