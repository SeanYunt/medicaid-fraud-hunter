FROM eclipse-temurin:17-jdk-jammy

# Install Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-venv \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Make python3.11 the default python
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
 && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

WORKDIR /app

# Install dependencies first (layer-cached until requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source (data/ and output/ are excluded via .dockerignore)
COPY . .

# Runtime data is mounted — create the directories so the app can write to them
RUN mkdir -p data/raw data/processed output

ENTRYPOINT ["python", "cli.py"]
CMD ["--help"]
