FROM python:3.11

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*


# Upgrade pip
RUN pip install --upgrade pip

# Copy requirements
COPY requirements.txt .

# Install Python dependencies without --only-binary, except for lxml
RUN pip install --no-cache-dir -r requirements.txt


# Clean up unnecessary packages
RUN apt-get purge -y --auto-remove gcc libxml2-dev libxslt1-dev zlib1g-dev
