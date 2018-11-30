# AgendaEndorsement
Sanjay has newly moved into town and has a number of errands to run around the area. He wants to get a few of them done when he goes to grab lunch today. He consults a few of his favourite apps to decide where to do each of them, but he finds he would have to travel all around town to get them done. He wishes he had an application that would recommend a feasible plan of action to complete all his errands together in a timely fashion, and of course enjoy a sumptuous meal. To help people like Sanjay, our objective is to build a recommendation system.
The yelp dataset which contains data about various businesses like restaurants, movers, gyms, pubs etc. is the data source for our system. Using this data, we recommend an optimized plan based on his to-do list.

# Getting Started

Download the Yelp dataset from [https://www.yelp.com/dataset/challenge] and unzip it 	

# Prerequisites					

Python: 2.7 Scala: 2.11 Spark: 2.3.1				
# Running 

## Running the preprocessing code:

### JSON to CSV converter for each of the following files from the Yelp Dataset:

  - yelp_academic_dataset_business.json
  - yelp_academic_dataset_review.json
  - yelp_academic_dataset_user.json 

```
python jsonToCSVConverter.py
```
**The files produced are:**
  - Converted_Business.csv
  - Converted_Users.csv
  - Converted_Review.csv

### To count businesses per city and select city with maximum number of businesses (Las Vegas). After that, unique category tags for businesses in Las Vegas are extracted.
```
python categoryextract.py
```
**The files produced are:**
  - Category_List.csv
  - LasVegas_Biz.csv

### We have a file which contains annotated umbrella term categories for business tags e.g: Gym, Martial Arts, F45 are all annotated as Health & Wellness. The LasVegas_Biz.csv file generated from the previous step is modified to include the umbrella terms based on the businesses’ tags.
```
python AnnotateUmbrellaTerms.py
```
**The files produced are:**
  - Annotated_LasVegas_Biz.csv

### Using Annotated_LasVegas_Biz.csv, we pick four top umbrella terms to cater to- Restaurants, Shopping, Health & Wellness, Home Finishings.  All of the subcategories for these umbrella terms are included. Businesses which come under these umbrella terms are filtered out.
```
python BusinessesForCategory.py
```
**The files produced are:**
  - filteredBusinesses.csv

### Using filteredBusinesses.csv and Converted_Reviews.csv, we generate a file that contains all users that have rated businesses present in filteredBusinesses.csv
```
python ExtractUsers.py
```
**The files produced are:**
  - usersForBiz_v3.csv


## Running the model code:	
```
$SPARK_HOME/bin/spark-submit -- class ProjectCF ProjectCF.jar
```

## Running the Recommender System code (which uses the model generated)		
```
$SPARK_HOME/bin/spark-submit -- class AgendaEndorsementEngine AgendaEndorsementEngine.jar
```
# Built With
Maven can be used to install the following required dependencies

**Package Name**
```
org.apache.spark:spark-core_2.11:2.0.0
com.databricks:spark-csv_2.11:1.5.02
net.liftweb:lift-json-ext_2.10:3.0-M12
org.apache.spark:spark-sql_2.11:2.0.0
org.apache.spark:spark-mllib_2.11:2.0.0 
```
 
# Contributing
In general, we follow the "fork-and-pull" Git workflow.

  - Fork the repo on GitHub
  - Clone the project to your own machine
  - Commit changes to your own branch
  - Push your work back up to your fork
  - Submit a Pull request so that we can review your changes

# Acknowledgments
  - “Cartesian Product of two or more lists” https://rosettacode.org/wiki/Cartesian_product_of_two_or_more_lists#Scala
  - “Yelp Analysis”: https://zhiyangzeng.github.io/
  - “Distance between two points based on latitude and longitude” https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
