# Use the official Python base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

ENV GOOGLE_APPLICATION_CREDENTIALS gcpauth.json

# Copy the Python script to the container
COPY GA4.py .
COPY functions.py .
COPY requirements.txt .
COPY gaauth.json .
COPY gcpauth.json .
COPY properties.json .

# Install any necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the default command to run the Python script with command line arguments
CMD ["python", "GA4.py", "Views", "bello"]
