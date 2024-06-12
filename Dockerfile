FROM python:3.10
RUN apt update && apt upgrade -y
WORKDIR /app
COPY ./requirements.txt /app/
COPY ./main.py /app/
RUN pip install -r requirements.txt