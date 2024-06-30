[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/1lXY_Wlg)

## Who we are

![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/79658792/6fa52c9c-8930-4561-9d88-35ccb6640581)

# The Sentiment Analysis of Sneakers: 

This capstone project focuses on performing sentiment analysis on customer reviews and social media discussions about sneakers. The goal is to analyze public sentiment towards different sneaker brands, models, and releases, providing insights into consumer preferences and trends.

[![Sneaker Heads](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/79658792/a1e4aee9-b06e-4b66-a098-ae075bd78c80)](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/79658792/a1e4aee9-b06e-4b66-a098-ae075bd78c80)

# Diagrams

### Data Architecture stack:
![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/6039558c-276b-4931-95c1-d4f1a6f9dc33)

### Data Pipelines Diagram:

![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/f1198bd4-3848-4eab-a16d-e0ed7ad6fb00)

### Conceptual Design Diagram:
![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/79658792/4eae2a7d-58dc-49ed-884c-c4b272a18b22)

### Data Diagrams:

#### Sources:
![Architecture Diagram - Sources Schemas](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/dda834ae-ee2e-49db-b61b-c8c13b5d6dec)

#### Dimensional Model:

![Architecture Diagram - Dimensional Model](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/281de38a-7afa-4abb-9a41-d9714269f635)


# Capstone Project: The Sentiment Analysis of Sneakers

## Project Overview
This capstone project focuses on performing sentiment analysis on customer reviews and social media discussions about sneakers. The goal is to analyze public sentiment towards different sneaker brands, models, and releases, providing insights into consumer preferences and trends.

## Objectives
1. **Data Collection:**
   - **Tweets:** Use the Twitter API or any Tweets extractor alternative to gather tweets mentioning various sneaker brands and models.
   - **Sneaker Website:** Collect data by web scraping sneaker a website which includes detailed information about different sneaker models, release dates, and other relevant data.

2. **Data Cleaning and Preprocessing:** Clean and preprocess the collected data to remove noise, handle missing values, and prepare it for analysis. This includes tokenizing text, removing stop words, and handling special characters and hashtags.

3. **Sentiment Analysis:** Use natural language processing (NLP) techniques and sentiment analysis models to classify the sentiment of the tweets and sneaker reviews as positive, negative, or neutral.

4. **Visualization:** Create visualizations to present the findings, such as sentiment distribution and trend graphs.

## Methodology
- **Data Collection:** Scrape a sneakers catalog for detailed sneaker information and gather tweets related to each of these sneakers.
- **Preprocessing:** Use PySpark to clean scraped sneakers catalog.
- **Sentiment Analysis:** Implement sentiment analysis for each tweet obtained for each sneaker, create a key to join both datasets.
- **Data Modeling:** Model data in a star schema, to get insights efficiently.
- **Visualization:** Use a data viz tool to create dynamic dashboards based on modeled data.

## Expected Outcomes
- A comprehensive analysis of consumer sentiment towards various sneaker brands and models based on Twitter data and sneaker website information.
- Insights into what features or aspects drive positive or negative sentiment.
- Identification of emerging trends and consumer preferences in the sneaker market.

## Current Outcomes:

### Data Pipeline overview:

#### DAG 1: Exctact Load Sneakers Catalog to S3:
![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/d8f9315f-61b7-42c1-8202-3a9c4c03a236)
1. Clean destination S3 folder.
2. Convert local JSON files to PARQUET using Polars.
3. Load PARQUETS to S3 destination folder.

#### DAG 2: Clean Sneakers Catalog data from S3 and load to Data Expert Data Warehouse:
![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/b45d6baa-35eb-4d9c-aae2-cce055cd0ae1)

1. Load Glue PySpark job to S3
2. Submit Glue PySpark job which read sneakers catalog data from S3, cleans it to a user friendly format and loads it to Data Expert Data Warehouse. Also, loads distinct sneakers models to S3 that will be used in the main pipeline as input.

#### DAG 3: Main Tweets Extractor and Sentiment Analyzer Pipeline:

![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/49d24435-c866-46c6-aeeb-72592c38ca51)

1. Get distinct sneakers models from S3. Take a slice of 20 sneakers that will be used downstream.
2. For each sneaker, extracts tweets related to it. If tweets are found they're uploaded to S3.
3. A glue PySpark is submitted, this will read found tweets from S3 and perform a sentiment analysis for each of them (which is a value from -1 to 1). Then load result to Data Expert's Data Warhouse.
4. New slice from sneakers catalog is calculated, to be able to read a different slice of sneakers each DAG run. Parallely, dbt staging models are created.
5. DBT tests are run, to make sure staging data is consistent to run the marts models (dimensions, facts, snapshots). If test fails will block marts runs.
6. DBT marts are run, this include dimension tables and fact tables.
7. DBT snapshots are run, this include an user scd.

### Data Quality:

DBT tests and dbt expectations are used to submit DQ checks on our data sources, this will help to evaluate input data for our marts. Helps to make sure that marts get consistent data from our expected Data Quality rules.

### Data Viz:
- Preset data viz with different aggregation levels and multiple metrics and filters:
![image](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-central-1/assets/16787672/670d4b50-96dc-4d6c-8ead-9100df8524df)
- We can visualize the relationship between different metrics with the sentiment of a specific brand, model and year:
- Metrics: Total retweets, total likes, total positive tweets, total negative tweets, total neutral tweets, and more... 






