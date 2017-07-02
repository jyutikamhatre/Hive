# Hive
Proof of concept - Hadoop - HIVE


Project Description
Finding the total hours and miles covered by each driver in a year
Dataset Resource
https://hortonworks.com/tutorial/how-to-process-data-with-apache-hive/#download-the-data
Special operation
1.Read 3 datasets
2.Calculate total hours spent and miles covered using JOIN 
3.Using Partitioned Table to optimize query performance
4.Comparison for time for same query when done on partitioned table and non-partition table
5.External table and bucketing for optimizing query performance
6.Table Sampling
7.String operation on table
8.Using UDF in HIVE
9.Saving output on Local File System
10.Saving output on HDFS
Goal
Finding the total hours and miles covered by each driver in a year
Java class
AutoIncrementRowUDF
UDF jar
AutoIncrementUDF.jar
Input file
/input/drivers.csv
/input/timesheet.csv
/input/truck_event_text_partition.csv
Output file 
/home/notroot/lab/driveroutput - Local file system
/user/notroot/driverslist   - HDFS
