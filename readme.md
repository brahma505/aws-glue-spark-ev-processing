# AWS Lakehouse Architecture for EV Population Data

This repository contains the implementation details and code for processing and analyzing Electric Vehicle (EV) population data using AWS Lakehouse architecture. This project utilizes a combination of AWS services including S3, AWS Glue, Amazon EMR, Amazon Redshift, and Amazon Athena to create a scalable, efficient, and powerful data platform.

## Project Overview

This project aims to demonstrate the capability of AWS Lakehouse architecture to handle large-scale EV population data, providing insights into EV usage patterns, efficiency, and market trends.

## Architecture

The following AWS services form the backbone of our data platform:

- **Amazon S3**: Used as the primary data lake storage to store raw and processed data.
- **AWS Glue**: Manages the data catalog and provides ETL capabilities.
- **Amazon EMR**: Processes large datasets using big data frameworks like Spark and Hadoop.
- **Amazon Athena**: Allows SQL querying directly on data stored in S3.
- **Amazon Redshift**: Acts as our data warehouse solution for complex queries and aggregations.

![image](https://github.com/brahma505/aws-glue-spark-ev-processing/assets/43395721/f9ceb3c7-c36c-406d-9971-973f2693982e)

## Dataset

The EV population data includes information about:
- Vehicle make and model
- Year of manufacture
- Electric vehicle type (BEV, PHEV)
- Usage statistics

## Getting Started

### Prerequisites

- AWS Account
- Access to AWS Management Console
- Basic understanding of AWS services (S3, Glue, EMR, Athena, Redshift)
- Familiarity with SQL and Python

### Deployment

#### Step 1: Setting Up S3 Buckets

1. Log into the AWS Management Console.
2. Navigate to the S3 service and create a new bucket:
   - **Bucket Name**: `your-bucket-name`
   - **Region**: Choose the closest region

#### Step 2: Configuring AWS Glue

1. Create a new Glue Data Catalog:
   - Catalog Name: `EVDataCatalog`
2. Define Crawlers to populate the catalog from S3 data.

#### Step 3: Launch an EMR Cluster

1. Go to the EMR service and set up a new cluster:
   - Choose Spark as the application.
   - Configure instance types based on the data volume.

#### Step 4: Data Processing with Spark

1. Submit Spark jobs to transform and aggregate data.
2. Output processed data back to S3.

#### Step 5: Analyze with Athena and Redshift

1. Set up Athena to query processed data directly on S3.
2. Load aggregated data into Redshift for complex querying.

### Usage Examples

```sql
-- Query to find the most popular EV model
SELECT model, COUNT(*) AS count
FROM ev_data
GROUP BY model
ORDER BY count DESC;
