# Bikeshare-Data-ETL-Analysis-Visualization-Pipeline

## Welcome to My Business Intelligence Project Repository!

### Hello there! 👋 

This repository showcases my journey from a coding novice to a budding Business Intelligence and Data Analytics. 

Here, you'll find the code for my Bikeshare Data ETL and Analysis project, which I completed during the intensive Business Intelligence Bootcamp at Binar Academy.

### About This Project

This comprehensive project demonstrates my newly acquired skills in:
* Data Engineering with Python and Prefect
* Database Management using PostgreSQL
* SQL Querying
* Cloud Technology (Google BigQuery)
* End-to-End Project Management

**Dataset** 

The dataset used in this project is the Bikeshare Dataset, which can be downloaded from the following link: 

* [Bikeshare Dataset (RAR file)](https://bikesharedataset.s3.ap-southeast-2.amazonaws.com/Bikeshare_Dataset/Bikeshare_Dataset.rar) 

Please note that you will need to extract the RAR file to access the dataset.


### Project Overview

This project involves processing and analyzing bikeshare data using various tools and technologies. The main steps of the project are:

1. Data Extraction and Loading
2. Data Transformation
3. Data Analysis
4. Visualization

Let's go through each step in detail:

### 1. Data Extraction and Loading

The project starts by extracting data from CSV files and loading it into a PostgreSQL database. This process is managed using Python and the Prefect workflow management tool.

![alt text](https://github.com/AnggitaLestari/End-to-end-pipeline-and-visualization-for-bikeshare/blob/main/Images/system%20diagram.JPG?raw=true)

The ETL process is implemented using Python and the Prefect library. The `create_db_engine()` and `load_csv_to_postgresql()` functions are responsible for connecting to the PostgreSQL database and loading the CSV data into it.

### 2. Data Transformation

After loading the data into PostgreSQL, several transformation steps are performed:
* Processing regions data
* Cleaning and transforming station information
* Processing trip data and creating fact and dimension tables

These transformations are implemented in the `process_regions()`, `process_station_info()`, and `process_trips()` functions.

### 3. Data Analysis

The project includes several SQL queries to analyze the transformed data. These queries cover various aspects of the bikeshare system, such as:
* Total trips by region and year
* Average trip duration by region, year, and member type
* Most popular travel routes
* Station utilization analysis
* Age group and gender distribution of trips

The queries are executed using the `execute_query()` function and the results are stored for further processing.

### 4. Prefect Implementation in the Project

![alt text](https://github.com/AnggitaLestari/End-to-end-pipeline-and-visualization-for-bikeshare/blob/main/Images/prefect%20bikeshare.JPG?raw=true)

The image above shows the Prefect flow visualization for this project, demonstrating the complexity and efficiency of the data pipeline I built.

Prefect played a crucial role in this project, enabling me to:

   **1. Define Complex Workflows:** Using Prefect's @flow and @task decorators, I was able to break down the ETL logic into manageable components.
   
   **2. Manage Task Dependencies:** Prefect allowed me to easily define the execution order of tasks, ensuring each step in the pipeline runs at the right time.

   **3. Monitoring and Logging:** With Prefect, I could easily track the status of each task and workflow, enabling efficient troubleshooting.
   
   **4. Error Handling:** Prefect provided built-in mechanisms for error handling and retries, enhancing the reliability of the data pipeline.
  
   **5. Workflow Visualization:** As shown in the image, Prefect offers intuitive workflow visualization, aiding in understanding and communicating the pipeline structure.

### 5. Visualization

The final result of this project is a comprehensive dashboard visualizing various aspects of the bikeshare system:

![alt text](https://github.com/AnggitaLestari/End-to-end-pipeline-and-visualization-for-bikeshare/blob/main/Images/San_Francisco_Ford_GoBike_Share_Monitoring_Dashboard.jpeg?raw=true)

This dashboard provides insights into various aspects of the bikeshare system, including:
* Total trips by region
* Regional bikeshare trends
* Top 10 most popular travel routes
* Trip duration by region and member type
* Bicycle station utilization per region
* Age group trip distribution
* Busiest days and hours per region

## Let's Connect!

I'm always excited to connect with fellow data enthusiasts, professionals in the BI field, or anyone interested in my journey. 

I'm passionate to learn more about BI, continuously learning and improving to deliver impactful data-driven insights. 

Your messages, questions, or feedback are warmly welcome!

* 💼 Connect with me on [LinkedIn](www.linkedin.com/in/4nggitalestari)
* 📧 Email me at [anggitalestari@gmail.com]


Thank you for visiting my repository. Happy coding! 🚀📊
