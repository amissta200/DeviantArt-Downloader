FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy script
COPY downloader.py .

# Install required packages
RUN pip install requests

# Create persistent data folder
RUN mkdir -p /data

# Set environment variable for data folder
ENV SAVE_DIR=/data

# Run the script
CMD ["python", "downloader.py"]
