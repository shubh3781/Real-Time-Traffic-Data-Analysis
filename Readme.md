Real-Time Traffic Data Analysis

PART-1
This part describes the creation of a Python script tasked with reading traffic counter data. The script is designed to read 10 records every 5 seconds, specifically focusing on two counter sites while ignoring test sites. These records are then stored as separate files (named countdata1, countdata2, countdata3, etc.) in a streaming directory, which is monitored by the application.

PART-2
This section outlines the analysis to be performed on the traffic data collected from the M50. It is divided into four queries:

Q1: The task involves displaying the total number of counts by vehicle class for each site on the M50.

Q2: This query computes the average speed by vehicle class for each site on the M50.

Q3: The goal here is to identify the top 3 busiest counter sites on the M50.

Q4: This involves finding the total number of counts for Heavy Goods Vehicles (HGVs) on the M50.

Each query is expected to produce specific outputs, likely to be stored or viewed in Cassandra, as indicated by the mention of "Cassandra Output". 