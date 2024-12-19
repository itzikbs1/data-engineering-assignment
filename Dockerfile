FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apscheduler

# Copy the src directory into the container
COPY src/ ./src/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the scheduled_service.py script from src directory
CMD ["python", "./src/scheduled_service.py"]

### Dockerfile
##FROM postgres:17
## Use an official Python image
#FROM python:3.9-slim
#
## Set the working directory inside the container
#WORKDIR /app/src
#
## Copy the application files from the src directory
#COPY . /app/src/
#
## Install Python dependencies
#RUN pip install --no-cache-dir -r requirements.txt
##RUN ls /app/src # install --no-cache-dir -r /app/src/requirements.txt
#RUN pip install apscheduler
## Set environment variables (optional)
#ENV PYTHONUNBUFFERED=1
#
#
#
## Run the scheduled_service.py script
#CMD ["python", "scheduled_service.py"]
