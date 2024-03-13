FROM python:3.9.7


COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY ingestion_files ingestion_files

ENTRYPOINT [ "python", "ingest_data.py" ]