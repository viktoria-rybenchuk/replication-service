FROM python:3.11-slim

WORKDIR /app

COPY secondary_service /app

COPY ../requirements.txt /app

RUN pip install  -r requirements.txt


ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000",  "--reload"]

