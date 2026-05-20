FROM python:3.12

WORKDIR /app

ENV PYTHONPATH=/app

# Install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# gRPC server port
EXPOSE 8000

# Run server
CMD ["python", "server.py"]