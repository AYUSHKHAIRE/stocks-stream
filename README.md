# Stocks-Stream

## 1. Project Description
Stocks-Stream is an end-to-end financial market prediction pipeline covering real-time and daily stock data. It includes full ETL, storage, processing, model training, and prediction workflows. Real-time data is captured at a per-minute frequency, while daily-level datasets are used for long-range modeling.

## 2. Components
- Data sourcing: real-time and daily
- Data storage: databases and file-based datasets
- Data provider API: Django + MongoDB Atlas
- Data processing: real-time and batch using Apache Spark
- Model training and prediction: deep learning and ensemble methods

### 2.1 Data Sourcing (Real-Time and Daily)
Two scheduled Kaggle notebooks handle automated ingestion:
- **Real-Time Stock Updates (Weekly):** Collects minute-level data for the past week and publishes to Kaggle.
- **Crypto Daily Updates:** Collects and publishes daily historical data for top crypto assets.

**Notebook Performance**

Notebook - Kaggle| link | likes | Views | forks 
--- | --- | --- | --- | --- 
Crypto dataset - ( daily updates ) - top 2000 | [Notebook link](https://www.kaggle.com/code/ayushkhaire/crypto-dataset-daily-updates-top-2000) | 18 | 950 | 105 
real time data update | [Notebook link](https://www.kaggle.com/code/ayushkhaire/real-time-data-update) | 17 | 750 | 48

### 2.2 Data Storage (DB and Filesystem)
A Python script loads datasets into MongoDB, MySQL, and publishes to Kaggle/HuggingFace.

Platform | Dataset | Dataset link | views | likes | Downloads 
--- | --- | --- | --- | --- | --- 
Kaggle | top 2000 crypto historical data ( Daily updates ) | [Dataset link](https://www.kaggle.com/datasets/ayushkhaire/top-1000-cryptos-historical) | 7200+ | 47 | 1600+ 
Kaggle | Real Time Stocks Data ( Weekly Updates ) | [Dataset link](https://www.kaggle.com/datasets/ayushkhaire/real-time-stocks-data) | 1700+ | 8 | 230+ 
HuggingFace | Real Time Stocks Data ( Weekly Updates ) | [Dataset link](https://www.kaggle.com/datasets/ayushkhaire/real-time-stocks-data) | Not Provided | 5 | avg 100 + downloads / month

### 2.3 Data Provider API
A Django API connects to MongoDB Atlas and exposes stock/crypto data over HTTP.  
Repository: https://github.com/AYUSHKHAIRE/stocks-stream/tree/main/stocksly

### 2.4 Data Processing (Batch and Real-Time)
A complete simulation pipeline streams data through Kafka or WebSockets and processes it using Apache Spark.

Code includes:
- Stream simulator
- Kafka/WebSocket streamers
- Real-time Spark analysis
- Batch Spark pipelines

Repository: https://github.com/AYUSHKHAIRE/stocks-stream/tree/main/stream-processing  
Batch processing: https://github.com/AYUSHKHAIRE/stocks-stream/tree/main/batch-processing

### 2.5 Model Training and Prediction
The pipeline generates rolling and windowed features and trains:
- LSTM
- GRU
- DNN
- XGBoost as meta-learner for stacking

Training and evaluation notebooks:  
https://github.com/AYUSHKHAIRE/stocks-stream/tree/main/Notebooks

Model example:

![](https://github.com/AYUSHKHAIRE/stocks-stream/blob/main/images/download.png?raw=true)

## 3. Architecture

### 3.1 ETL
![](https://github.com/AYUSHKHAIRE/stocks-stream/blob/main/images/projects-architectures%20-%20Page%201.jpeg?raw=true)

### 3.2 Data Availability and Processing
![](https://github.com/AYUSHKHAIRE/stocks-stream/blob/main/images/projects-architectures%20-%20Page%201%20(1).jpeg?raw=true)

### 3.3 Model Training and Evaluation
![](https://github.com/AYUSHKHAIRE/stocks-stream/blob/main/images/projects-architectures%20-%20Page%201%20(2).jpeg?raw=true)

## Conclusion
Stocks-Stream demonstrates a complete and scalable workflow for financial data engineering and predictive modeling. It integrates automated dataset generation, real-time and batch processing, database-backed APIs, and a robust ML pipeline combining deep learning and ensemble techniques. The project is production-oriented, modular, and suitable as a foundation for advanced financial analytics systems.
