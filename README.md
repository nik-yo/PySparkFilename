# PySpark Filename

Due to the distributed nature of Apache Spark, when writing result, we can't specify name for the result file. One way to do that is by utilizing Panda, in this case, AWS wrangler which can take Spark dataframe and output it with a specific name. Per my testing, we need Arrow to speed up the process if data is significantly large.

## Stack
- Apache Spark
- Python
- PySpark
- AWS Wrangler
- Panda
- Arrow

## Data
For this example, I used crime data from data.gov, I do not include it in this repo because of the size.
- https://catalog.data.gov/dataset/crime-data-from-2020-to-present