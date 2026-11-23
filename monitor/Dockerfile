FROM python:3.12-slim
WORKDIR /srv
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY monitor.py ./
COPY static ./static
EXPOSE 9000
CMD ["uvicorn", "monitor:app", "--host", "0.0.0.0", "--port", "9000", "--no-access-log"]
