FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY event_server.py /app/

EXPOSE 8082

CMD ["python", "event_server.py"]