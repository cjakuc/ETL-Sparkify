## Sparkify: Data Modeling and ETL with Postgres and Python

Sparkify is a fictional music streaming app. They have stored all of their song and user activity data in JSON files and are looking for a way to effectively organize their data and surface it for analysis. This project seeks to do exactly that by programmatically extracting the data, processing it, and storing it in a PostgreSQL database for future analysis.

In order to run this project locally, I used [pgAdmin 4](https://www.pgadmin.org/) to host a local PostgreSQL database. If you would like to do the same, make sure there is the proper role setup within the server with the proper password. Then I ran my script to create the tables/schema with `python create_tables.py`. Lastly, I used `python etl.py` to extract, transform, and load the data from the song and log files within the data folder, and insert it into the databse. 

`sql_queries.py` is where I defined the queries used to create the tables and insert data into the database. `etl.ipynb` is where I performed ETL on a single row of data to refine my methods for `etl.py`. 

## Database Schema and ETL Pipeline

![Database Schema](Sparkify_Diagram.png)

The database schema design, shown above, is a star schema with the *Songplays* fact table surround by the *Users*, *Songs*, *Artists*, and *Time* dimension tables. A star schema design offers the benefits of denormalization, simplified queries, and fast aggregation for analysis. In this case, it is very straightforward to analyze Sparkify's songplays data and augment the analysis with the information in the dimensions tables. To see some examples of basic queries and visualizations for analysis, check out the [analysis file](https://github.com/cjakuc/ETL-Sparkify/blob/master/analysis.ipynb).