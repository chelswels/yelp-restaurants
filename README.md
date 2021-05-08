### Introduction

This project seeks to identify factors that potentially predict high ratings for an American restaurant. It examines businesses included in the Yelp Open Dataset.

The code employs the MapReduce and Hive technologies and can be run on a Hadoop cluster.

### Code submission directory structure

### Project step-by-step

1. Import businesses.json from https://www.yelp.com/dataset into HDFS.
1. Clean raw data: Execute cleaning.jar on input file businesses.json. Name output file businesses_cleaned.txt.
2. Sort out data for "successful" restaurants (>=20 reviews and >=4 stars): Execute sortSuccessful.jar on input file businesses_cleaned.txt. Name output file businesses_successful.txt.
3. Sort out data for "unsuccessful" restaurants (closed or <=3 stars): Execute sortUnsuccessful.jar on input file businesses_cleaned.txt. Name output file businesses_unsuccessful.txt.
4. The rest of the steps apply to both businesses_successful.txt and businesses_unsuccessful.txt.
5. Determine distribution by state: Execute stateCount.jar.
6. Determine number of unique categories: Execute categoriesCount.jar.
7. Determine number of unique attributes: Execute attributesCount.jar.
8. Import output files from stateCount.jar, categoriesCount.jar, and attributesCount.jar to Hive tables. For each table, use GROUP BY and JOIN queries to calculate % distribution and compare successful and unsuccessful restaurants.
