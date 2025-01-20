# Hadoop Movies Project

## Overview
This project uses Hadoop to process a movie dataset (https://files.grouplens.org/datasets/movielens/ml-25m.zip) to:
1. Identify the highest-rated movie for each user.
2. Map the highest-rated movie IDs to their titles.
3. Count and group movies by the number of users who rated them highest.

## Prerequisites
1. Set up Hadoop using the provided Docker Compose file:
```bash
docker-compose up -d
```

2. Upload movies.csv and ratings.csv to HDFS:
```bash
hadoop fs -put movies.csv /movies.csv
```
```bash
hadoop fs -put ratings.csv /ratings.csv
```

3. Compile the source code with Maven:
```bash
mvn clean package
```
or use the precompiled .jar available under Releases.

4. Ensure the HDFS directories /output1, /output2, /output3, /output4, and /output5 do not exist.

## Execution
Inside the namenode container, go to /hadoop/labs/target.
```bash
docker exec -it namenode bash
```
```bash
cd /hadoop/labs/target
```

1. Identify Highest-Rated Movie for Each User:
```bash
hadoop jar hadoop-1.0-SNAPSHOT.jar org.ziyad.hadoop.HighestRatedMovieIdPerUser /ratings.csv
```
Output: /output1/part-r-00000

2. Map Movie IDs to Titles:
```bash
hadoop jar hadoop-1.0-SNAPSHOT.jar org.ziyad.hadoop.HighestRatedMovieTitlePerUser /movies.csv
```
Output: /output3/part-r-00000

3. Count and Group Movies by User Likes:
```bash
hadoop jar hadoop-1.0-SNAPSHOT.jar org.ziyad.hadoop.MovieTitleListByLikedCount
```
Output: /output5/part-r-00000

Executions must be in sequential order, as a step's output is another's input.

To visualize the outputs, run: 
```bash
hadoop fs -text /output<X>/part-r-00000
```
