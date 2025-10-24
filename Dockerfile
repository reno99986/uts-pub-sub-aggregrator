FROM python:3.11-slim
WORKDIR /app
COPY . /app/
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 5000
CMD ["python", "-m", "src.app"]