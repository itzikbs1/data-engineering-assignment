[//]: # (# ride-hailing-platform)

# Home Assignment - Junior Data Engineer Position
### Project Overview

This project contains my solution for a Data Engineering home assignment. The project consists of two main sections: data modeling and ETL process implementation.

## Assignment Structure
### Section 1: Data Model Design
Design of a relational data model for a ride-hailing company. Tasks include:

- Describing expected raw data for the system
- Designing a relational data model
- Identifying and describing meaningful outcomes from the data


### Section 2: Technical Skills (ETL Implementation)
Implementation of an ETL process using Python and Docker, focusing on air quality data processing:

#### Dataset

- **Source**: Open Air Quality Data API
- **Type**: Real-time air quality measurements
- **Data Collection**: API requests to fetch current air quality metrics
- **Metrics**: Air quality indices, pollutant levels, and related environmental data

#### Implementation
- Automated data extraction via API requests (pagination limited for demo performance)
- Raw data storage in PostgreSQL
- Data warehouse implementation with fact and dimension tables
- Scheduled ETL process using APScheduler


### Project Setup

#### Prerequisites
- Python
- Air Quality API credentials
- PostgreSQL
- Docker and Docker Compose

### Installation
1. Clone the repository
    git clone https://github.com/itzikbs1/data-engineering-assignment
2. Navigate to the project directory
    cd data-engineering-assignment
3. Start the containers:
   - `docker-compose build --no-cache`
   - `docker-compose up`
4. Connect to db: 
   - `docker exec -it openaq_postgres psql -U postgres -d openaq_db`

### Technologies Used

- Python for ETL implementation
- Air Quality Data API
- PostgreSQL for data storage
- Docker and Docker Compose for containerization
- APScheduler for task scheduling
- Draw.io for ER diagram creation
- Documentation in Markdown format
