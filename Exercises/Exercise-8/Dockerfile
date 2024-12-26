FROM python:3.12.4-bookworm

RUN apt-get update && \
    apt-get install -y vim software-properties-common python3-pip libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

WORKDIR app
COPY . /app

RUN pip3 install -r requirements.txt
