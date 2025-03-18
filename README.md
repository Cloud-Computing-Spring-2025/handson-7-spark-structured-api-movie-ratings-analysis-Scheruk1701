# Movie Ratings Analysis

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
MovieRatingsAnalysis/
├── input/
│   └── movie_ratings_data.csv
├── outputs/
│   ├── binge_watching_patterns.csv
│   ├──churn_risk_users.csv
│   └── movie_watching_trends.csv
├── src/
│   ├── task1_binge_watching_patterns.py
│   ├── task2_churn_risk_users.py
│   └── task3_movie_watching_trends.py
├── docker-compose.yml
└── README.md
```









- **input/**: Contains the `movie_ratings_data.csv` dataset.
- **outputs/**: Directory where the results of each task will be saved.
- **src/**: Contains the individual Python scripts for each task.
- **docker-compose.yml**: Docker Compose configuration file to set up Spark.
- **README.md**: Assignment instructions and guidelines.

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **a. Running Locally**

1. **Navigate to the Project Directory**:
   ```bash
   cd workspaces/handson-7-spark-structured-api-movie-ratings-analysis-Scheruk1701
   ```

2. **Execute Each Task Using `spark-submit`**:
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

3. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```
   You should see:
   - `binge_watching_patterns.csv`
   - `churn_risk_users.csv`
   - `movie_watching_trends.csv`

#### **b. Running with Docker (Optional)**

1. **Start the Spark Cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Access the Spark Master Container**:
   ```bash
   docker exec -it spark-master bash
   ```

3. **Navigate to the Spark Directory**:
   ```bash
   cd /opt/bitnami/spark/
   ```

4. **Run Your PySpark Scripts Using `spark-submit`**:
   ```bash
   spark-submit src/task1_binge_watching_patterns.py
   spark-submit src/task2_churn_risk_users.py
   spark-submit src/task3_movie_watching_trends.py
   ```

5. **Exit the Container**:
   ```bash
   exit
   ```

6. **Verify the Outputs**:
   On your host machine, check the `outputs/` directory for the resulting files.

7. **Stop the Spark Cluster**:
   ```bash
   docker-compose down
   ```

## **Overview**

In this assignment, you will leverage Spark Structured APIs to analyze a dataset containing user information about movie ratings and streaming trends. Your goal is to extract meaningful insights related to user behavior, engagement, and movie-watching trends over time. This exercise is designed to enhance your data manipulation and analytical skills using Spark's powerful APIs.

## **Objectives**

By the end of this assignment, you should be able to:

1. **Data Loading and Preparation**: Import and preprocess data using Spark Structured APIs.
2. **Data Analysis**: Perform complex queries and transformations to address specific business questions.
3. **Insight Generation**: Derive actionable insights from the analyzed data.

## **Dataset**

## **Dataset: Advanced Movie Ratings & Streaming Trends**

You will work with a dataset containing information about **100+ users** who rated movies across various streaming platforms. The dataset includes the following columns:

| **Column Name**         | **Data Type**  | **Description** |
|-------------------------|---------------|----------------|
| **UserID**             | Integer       | Unique identifier for a user |
| **MovieID**            | Integer       | Unique identifier for a movie |
| **MovieTitle**         | String        | Name of the movie |
| **Genre**             | String        | Movie genre (e.g., Action, Comedy, Drama) |
| **Rating**            | Float         | User rating (1.0 to 5.0) |
| **ReviewCount**       | Integer       | Total reviews given by the user |
| **WatchedYear**       | Integer       | Year when the movie was watched |
| **UserLocation**      | String        | User's country |
| **AgeGroup**          | String        | Age category (Teen, Adult, Senior) |
| **StreamingPlatform** | String        | Platform where the movie was watched |
| **WatchTime**        | Integer       | Total watch time in minutes |
| **IsBingeWatched**    | Boolean       | True if the user watched 3+ movies in a day |
| **SubscriptionStatus** | String        | Subscription status (Active, Canceled) |

---



### **Sample Data**

Below is a snippet of the `movie_ratings_data.csv` to illustrate the data structure. Ensure your dataset contains at least 100 records for meaningful analysis.

```
UserID,MovieID,MovieTitle,Genre,Rating,ReviewCount,WatchedYear,UserLocation,AgeGroup,StreamingPlatform,WatchTime,IsBingeWatched,SubscriptionStatus
1,101,Inception,Sci-Fi,4.8,12,2022,US,Adult,Netflix,145,True,Active
2,102,Titanic,Romance,4.7,8,2021,UK,Adult,Amazon,195,False,Canceled
3,103,Avengers: Endgame,Action,4.5,15,2023,India,Teen,Disney+,180,True,Active
4,104,The Godfather,Crime,4.9,20,2020,US,Senior,Amazon,175,False,Active
5,105,Forrest Gump,Drama,4.8,10,2022,Canada,Adult,Netflix,130,True,Active
...
```

## **Assignment Tasks**

You are required to complete the following three analysis tasks using Spark Structured APIs. Ensure that your analysis is well-documented, with clear explanations and any relevant visualizations or summaries.

## **1. Detect Binge-Watching Patterns**

### **Objective**
Identify which age groups binge-watch movies the most by analyzing users who watched 3 or more movies in a single day.

### **Approach**
1. **Filter Users**: Select users who have `IsBingeWatched` set to `True`, indicating they watched 3 or more movies in a day.
2. **Group by Age Group**: Count the number of binge-watchers in each age category (e.g., Teen, Adult, Senior).
3. **Calculate Proportions**: Determine the percentage of binge-watchers within each age group based on the total number of users in that group.

### **Code Explanation**
```python
# Filter users who binge-watched
binge_watchers_df = movie_ratings_df.filter(movie_ratings_df["IsBingeWatched"] == True)

# Group by Age Group and count binge watchers
binge_watchers_by_age_df = binge_watchers_df.groupBy("AgeGroup").count()

# Calculate the total number of users per age group
total_users_by_age_df = movie_ratings_df.groupBy("AgeGroup").count()

# Join the dataframes to calculate the proportion of binge watchers
binge_watchers_proportion_df = binge_watchers_by_age_df.join(total_users_by_age_df, "AgeGroup") \
    .withColumn("Percentage", (binge_watchers_by_age_df["count"] / total_users_by_age_df["count"]) * 100)

binge_watchers_proportion_df.show()
```

### **Findings**
The analysis revealed the following binge-watching trends by age group:

| **Age Group** | **Binge Watchers** | **Percentage** |
|---------------|--------------------|----------------|
| Senior        | 21                 | 58.33%         |
| Teen          | 20                 | 54.05%         |
| Adult         | 16                 | 59.26%         |

**Interpretation**: All age groups exhibited a high proportion of binge-watchers, with adults having the highest percentage of binge-watchers (59.26%), followed by seniors (58.33%) and teens (54.05%).

---

## **2. Identify Churn Risk Users**

### **Objective**
Find users who are at risk of churn by identifying those with canceled subscriptions and low watch time (<100 minutes).

### **Approach**
1. **Filter Users**: Select users who have `SubscriptionStatus` set to `'Canceled'`.
2. **Analyze Watch Time**: Identify users with `WatchTime` less than 100 minutes.
3. **Count At-Risk Users**: Compute the total number of users who have both canceled subscriptions and low watch time.

### **Code Explanation**
```python
# Filter users with canceled subscriptions and low watch time
churn_risk_users_df = movie_ratings_df.filter((movie_ratings_df["SubscriptionStatus"] == "Canceled") &
                                               (movie_ratings_df["WatchTime"] < 100))

# Count churn-risk users
churn_risk_users_count = churn_risk_users_df.count()
print(f"Total churn-risk users: {churn_risk_users_count}")
```

### **Findings**
The analysis revealed that there were **8 churn-risk users** who had canceled their subscriptions and had low engagement. These users represent a potential churn risk.

---

## **3. Trend Analysis Over the Years**

### **Objective**
Analyze how movie-watching trends have changed over the years and identify peak years for movie consumption.

### **Approach**
1. **Group by Watched Year**: Count the number of movies watched each year.
2. **Analyze Trends**: Identify patterns and compare year-over-year growth in movie consumption.
3. **Find Peak Years**: Highlight the years with the highest number of movies watched.

### **Code Explanation**
```python
# Group data by WatchedYear and count the number of movies watched
yearly_trends_df = movie_ratings_df.groupBy("WatchedYear").agg({"MovieID": "count"})

# Sort the results to identify the peak years
yearly_trends_df = yearly_trends_df.orderBy("WatchedYear")
yearly_trends_df.show()
```

### **Findings**
The analysis showed the following trends in movie consumption over the years:

| **Watched Year** | **Movies Watched** |
|------------------|--------------------|
| 2018             | 19                 |
| 2019             | 17                 |
| 2020             | 9                  |
| 2021             | 17                 |
| 2022             | 20                 |
| 2023             | 18                 |

**Interpretation**: Movie consumption fluctuated over the years, with the highest number of movies watched in 2022 (20 movies). The trend does not show a strong growth pattern, indicating that consumption may have been affected by external factors.

---

## **Conclusion**

This report provides valuable insights into movie-watching patterns and trends:

1. **Binge-Watching Patterns**: All age groups exhibited a high proportion of binge-watchers, with adults leading the percentage, followed closely by seniors and teens.
2. **Churn Risk Users**: A total of 8 churn-risk users were identified, representing a potential opportunity for targeted retention efforts.
3. **Trend Analysis**: Movie consumption showed fluctuations over the years, with 2022 marking the highest number of movies watched, indicating that consumption might be influenced by various factors, such as global events or platform-specific changes.

These insights are important for shaping retention strategies, content recommendations, and marketing efforts.

---
