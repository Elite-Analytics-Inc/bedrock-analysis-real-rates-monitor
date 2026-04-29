FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir duckdb pandas
COPY bedrock_sdk/ /bedrock_sdk/
COPY analysis.py .
COPY dashboard/ dashboard/
CMD ["python", "analysis.py"]
