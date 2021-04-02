## Sparkify: Data Modeling and ETL with Postgres and Python

Sparkify is a fictional music streaming app. They have stored all of their song and user activity data in JSON files and are looking for a way to effectively organize their data and surface it for analysis. This project seeks to do exactly that by programmatically extracting the data, processing it, and storing it in a PostgreSQL database for future analysis.

## Database Schema and ETL Pipeline

![Database Schema](Sparkify_Diagram.png)

The database schema design, shown above, is a simple star schema with the *Songplays* fact table surround by the *Users*, *Songs*, *Artists*, and *Time* dimension tables. A star schema design offers the benefits of denormalization, simplified queries, and fast aggregation for analysis. In this case, it is very straightforward to analyze Sparkify's songplays data and augment the analysis with the information in the dimensions tables. To see some examples of queries for analysis, check out the [analysis.ipynb file]().