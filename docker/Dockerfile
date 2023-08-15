FROM --platform=linux/arm64/v8 python:3.10-slim-buster as build

WORKDIR /app 

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY main.py /app

RUN chmod +x /app/main.py

CMD ["python3", "main.py"]

