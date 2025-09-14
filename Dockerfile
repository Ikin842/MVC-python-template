FROM python:3.10-slim-buster

WORKDIR /app

COPY source/. /app

RUN pip install -r /app/requirements.txt

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "main.py"]

CMD ["engine", "run"]
