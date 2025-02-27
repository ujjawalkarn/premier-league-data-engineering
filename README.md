# premier-league-data-engeneering
![skysports-premier-league-feature_6211072](https://github.com/user-attachments/assets/0fe43404-fbd5-4061-9bb1-8c55044ff240)

Welcome to the Premier League Data Engineering project. I tackle the challenge of consuming data from multiple sites, processing it, pushing it to an S3 bucket, normalizing the data, and running analytics. 

# Sources
1. League table: https://www.bbc.com/sport/football/premier-league/table
2. Top scorers: https://www.bbc.com/sport/football/premier-league/top-scorers
3. Detailed top scorers: https://www.worldfootball.net/goalgetter/eng-premier-league-2024-2025/
4. Player table: https://www.worldfootball.net/players_list/eng-premierleague-2024-2025/
5. All time table: https://www.worldfootball.net/alltime_table/eng-premier-league/pl-only/
6. All-time winner (clubs): https://www.worldfootball.net/winner/eng-premier-league/
7. Top scorers per season: https://www.worldfootball.net/top_scorer/eng-premier-league/
8. Goals per season: https://www.worldfootball.net/stats/eng-premierleague/1/

# Functional flow diagram
  ![premier-league-flow drawio](https://github.com/user-attachments/assets/b362ace2-341a-4539-add6-a4a480b5fcfb)


# Process description
1. Data Scraping: Developed a Python-based AWS Lambda function (scrape.py) to extract Premier League data from multiple sources.
3. Data Storage: The Lambda function categorizes and stores the scraped data in designated Amazon S3 buckets.
4. Athena Table: Transformed the stored data into Parquet format and created an Athena table for efficient SQL querying.
5. Data Analysis & Visualization: Leveraged Preset for interactive data analysis and visualization.
6. Automation & Scheduling: Integrated an EventBridge rule to automate the process, ensuring it runs every Saturday and Sunday until the season concludes.

# Dashboard
  ![image](https://github.com/user-attachments/assets/d2757db2-8da4-4025-a738-642975c8d58c)

