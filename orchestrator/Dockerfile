FROM python:3.12-slim

WORKDIR /home/flask-app

COPY orchestrator/requirements.txt .

RUN pip install -r requirements.txt

COPY orchestrator/ .
COPY common/ ./common/

EXPOSE 5000
