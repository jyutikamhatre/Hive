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





Steps and Commands:

CREATE TABLE drivers (driverid INT, name STRING, ssn BIGINT, location STRING, certified STRING, wageplan STRING) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/notroot/lab/data/drivers.csv' OVERWRITE INTO TABLE drivers;

CREATE TABLE timesheet (driverid INT, week INT, hours_logged INT , miles_logged INT) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/notroot/lab/data/timesheet.csv' OVERWRITE INTO TABLE timesheet;

SELECT driverId, sum(hours_logged), sum(miles_logged) FROM timesheet GROUP BY driverid;
time taken : 24.419 seconds, fetched : 35 rows

>hive (travel)> select d.driverid,d.name,t.total_hours,t.total_miles from drivers d JOIN (select driverid,sum(hours_logged)total_hours,sum(miles_logged)total_miles FROM timesheet group by driverid) t on (d.driverid = t.driverid);

d.driverid      d.name  t.total_hours   t.total_miles
10      George Vetticaden       3232    147150
11      Jamie Engesser  3642    179300
12      Paul Coddin     2639    135962
13      Joe Niemiec     2727    134126
14      Adis Cesir      2781    136624
15      Rohit Bakshi    2734    138750
16      Tom McCuch      2746    137205
17      Eric Mizell     2701    135992
18      Grant Liu       2654    137834
19      Ajay Singh      2738    137968
20      Chris Harris    2644    134564
21      Jeff Markham    2751    138719
22      Nadeem Asghar   2733    137550
23      Adam Diaz       2750    137980
24      Don Hilborn     2647    134461
25      Jean-Philippe Playe     2723    139180
26      Michael Aube    2730    137530
27      Mark Lochbihler 2771    137922
28      Olivier Renault 2723    137469
29      Teddy Choi      2760    138255
30      Dan Rice        2773    137473
31      Rommel Garcia   2704    137057
32      Ryan Templeton  2736    137422
33      Sridhara Sabbella       2759    139285
34      Frank Romano    2811    137728
35      Emil Siemes     2728    138727
36      Andrew Grande   2795    138025
37      Wes Floyd       2694    137223
38      Scott Shaw      2760    137464
39      David Kaiser    2745    138788
40      Nicolas Maillard        2700    136931
41      Greg Phillips   2723    138407
42      Randy Gelhausen 2697    136673
43      Dave Patton     2750    136993

Using Partitioned Table to optimize query performance:

create table timesheet_partition_driverid (week int, hours_logged int, miles_logged int) partitioned by (driverid int) row format delimited fields terminated by ','

set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 400;

insert into table timesheet_partition_driverid partition (driverid ) select week, hours_logged, miles_logged, driverid from timesheet;

The output screenshot filename Hive Partition table output into 34 folders.png



SELECT driverId, sum(hours_logged), sum(miles_logged) FROM timesheet_partition_driverid GROUP BY driverid;

Comparison of time for same query when done on partitioned table and non-partition table:

time taken : 24.419 seconds, fetched : 35 rows, hdfs read : 35618 , total mapreduce CPU time spent : 3 seconds 890 msec
time taken : 22.34 seconds, fetched : 35 rows, hdfs read : 43978, total mapreduce CPU time spent : 3 seconds 130 msec

hive (travel)> show partitions timesheet_partition_driverid;
OK
partition
driverid=10
driverid=11
driverid=12
driverid=13
driverid=14
driverid=15
driverid=16
driverid=17
driverid=18
driverid=19
driverid=20
driverid=21
driverid=22
driverid=23
driverid=24
driverid=25
driverid=26
driverid=27
driverid=28
driverid=29
driverid=30
driverid=31
driverid=32
driverid=33
driverid=34
driverid=35
driverid=36
driverid=37
driverid=38
driverid=39
driverid=40
driverid=41
driverid=42
driverid=43


External table and bucketing for optimizing query performance:

create table truck_event (driverid int, truckid int,event_time string,event_type string,longitude string,latitude string,event_key string,correlationid string,drivername string,routeid bigint,route_name string,event_date string) row format delimited fields terminated by ',' lines terminted by '\n';

load data local inpath '/home/notroot/lab/data/truck_event.csv' overwrite into table truck_event;

create external table truck_event_bucket_ext (driverid int, truckid int,event_time string,event_type string,longitude string,latitude string,event_key string,correlationid string,drivername string,routeid bigint,route_name string,event_date string) clustered by (driverid) sorted by (truckid) into 34 buckets row format delimited fields terminated by ',' lines terminted by '\n'  stored as sequenefile location '/user/notroot/hivetable';

set hive.enforce.bucketing = true;

insert overwrite table truck_event_bucket_ext select * from truck_event;

Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 34
2017-06-23 03:52:09,300 Stage-1 map = 0%,  reduce = 0%
2017-06-23 03:52:18,052 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.57 sec
2017-06-23 03:52:36,999 Stage-1 map = 100%,  reduce = 3%, Cumulative CPU 5.33 sec
2017-06-23 03:52:42,986 Stage-1 map = 100%,  reduce = 6%, Cumulative CPU 7.45 sec
2017-06-23 03:52:46,548 Stage-1 map = 100%,  reduce = 12%, Cumulative CPU 11.2 sec
2017-06-23 03:52:48,919 Stage-1 map = 100%,  reduce = 15%, Cumulative CPU 13.03 sec
2017-06-23 03:52:50,087 Stage-1 map = 100%,  reduce = 18%, Cumulative CPU 14.82 sec
2017-06-23 03:53:04,273 Stage-1 map = 100%,  reduce = 21%, Cumulative CPU 16.78 sec
2017-06-23 03:53:09,184 Stage-1 map = 100%,  reduce = 24%, Cumulative CPU 18.72 sec
2017-06-23 03:53:11,485 Stage-1 map = 100%,  reduce = 26%, Cumulative CPU 20.57 sec
2017-06-23 03:53:12,591 Stage-1 map = 100%,  reduce = 29%, Cumulative CPU 22.58 sec
2017-06-23 03:53:14,964 Stage-1 map = 100%,  reduce = 32%, Cumulative CPU 24.9 sec
2017-06-23 03:53:16,081 Stage-1 map = 100%,  reduce = 35%, Cumulative CPU 26.9 sec
2017-06-23 03:53:29,074 Stage-1 map = 100%,  reduce = 38%, Cumulative CPU 28.88 sec
2017-06-23 03:53:33,867 Stage-1 map = 100%,  reduce = 41%, Cumulative CPU 31.03 sec
2017-06-23 03:53:37,449 Stage-1 map = 100%,  reduce = 44%, Cumulative CPU 33.1 sec
2017-06-23 03:53:38,594 Stage-1 map = 100%,  reduce = 47%, Cumulative CPU 35.62 sec
2017-06-23 03:53:40,892 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 37.9 sec
2017-06-23 03:53:42,015 Stage-1 map = 100%,  reduce = 53%, Cumulative CPU 39.84 sec
2017-06-23 03:53:56,071 Stage-1 map = 100%,  reduce = 56%, Cumulative CPU 42.19 sec
2017-06-23 03:54:00,877 Stage-1 map = 100%,  reduce = 59%, Cumulative CPU 44.45 sec
2017-06-23 03:54:03,186 Stage-1 map = 100%,  reduce = 62%, Cumulative CPU 46.72 sec
2017-06-23 03:54:04,336 Stage-1 map = 100%,  reduce = 65%, Cumulative CPU 48.97 sec
2017-06-23 03:54:05,548 Stage-1 map = 100%,  reduce = 68%, Cumulative CPU 51.14 sec
2017-06-23 03:54:07,807 Stage-1 map = 100%,  reduce = 71%, Cumulative CPU 53.28 sec
2017-06-23 03:54:22,015 Stage-1 map = 100%,  reduce = 74%, Cumulative CPU 55.21 sec
2017-06-23 03:54:27,949 Stage-1 map = 100%,  reduce = 76%, Cumulative CPU 57.46 sec
2017-06-23 03:54:29,064 Stage-1 map = 100%,  reduce = 79%, Cumulative CPU 59.61 sec
2017-06-23 03:54:30,195 Stage-1 map = 100%,  reduce = 82%, Cumulative CPU 61.88 sec
2017-06-23 03:54:31,342 Stage-1 map = 100%,  reduce = 85%, Cumulative CPU 63.79 sec
2017-06-23 03:54:34,779 Stage-1 map = 100%,  reduce = 88%, Cumulative CPU 66.03 sec
2017-06-23 03:54:43,787 Stage-1 map = 100%,  reduce = 91%, Cumulative CPU 68.01 sec
2017-06-23 03:54:47,083 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 73.92 sec
MapReduce Total cumulative CPU time: 1 minutes 13 seconds 920 msec
Ended Job = job_1498198946915_0004
Loading data to table travel.truck_event_bucket_ext
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 34   Cumulative CPU: 73.92 sec   HDFS Read: 2499108 HDFS Write: 2502675 SUCCESS
Total MapReduce CPU Time Spent: 1 minutes 13 seconds 920 msec
OK
truck_event.driverid    truck_event.truckid     truck_event.event_time  truck_event.event_type  truck_event.longitude   truck_event.latitude    truck_event.event_key   truck_event.correlationid t  ruck_event.drivername   truck_event.routeid     truck_event.route_name  truck_event.event_date
Time taken: 172.955 seconds

Screen-shot output of Hive External table and Bucketing into 34 buckets:



Table Sampling:

hive (travel)> select * from truck_event_bucket_ext tablesample(bucket 32 out of 34 on driverid) limit 10;
OK
truck_event_bucket_ext.driverid truck_event_bucket_ext.truckid  truck_event_bucket_ext.event_time       truck_event_bucket_ext.event_type       truck_event_bucket_ext.longitude        truck_event_bucket_ext.latitude    truck_event_bucket_ext.event_key        truck_event_bucket_ext.correlationid    truck_event_bucket_ext.drivername       truck_event_bucket_ext.routeid  truck_event_bucket_ext.route_name  truck_event_bucket_ext.event_date
31      18      59:23.5 Normal  -94.31  37.31   31|18|9223370572464812346       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      59:47.3 Normal  -94.35  38.33   31|18|9223370572464788543       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      59:58.2 Normal  -94.38  38.67   31|18|9223370572464777615       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      00:52.5 Normal  -90.2   38.65   31|18|9223370572464723286       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      59:39.7 Normal  -94.3   37.66   31|18|9223370572464796074       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      59:54.9 Normal  -94.31  37.31   31|18|9223370572464780896       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      00:38.2 Normal  -94.46  37.16   31|18|9223370572464737596       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      00:37.4 Normal  -94.31  37.31   31|18|9223370572464738404       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      00:29.6 Normal  -94.38  38.67   31|18|9223370572464746166       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
31      18      59:52.8 Normal  -94.58  37.03   31|18|9223370572464782961       3.66E+18        Rommel Garcia   1594289134      Memphis to Little Rock Route 2  2016-05-27-22
Time taken: 0.129 seconds, Fetched: 10 row(s)
hive (travel)> select * from truck_event_bucket_ext tablesample(bucket 20 out of 34 on truckid) limit 10;
OK
truck_event_bucket_ext.driverid truck_event_bucket_ext.truckid  truck_event_bucket_ext.event_time       truck_event_bucket_ext.event_type       truck_event_bucket_ext.longitude        truck_event_bucket_ext.latitude    truck_event_bucket_ext.event_key        truck_event_bucket_ext.correlationid    truck_event_bucket_ext.drivername       truck_event_bucket_ext.routeid  truck_event_bucket_ext.route_name  truck_event_bucket_ext.event_date
19      65      38:41.2 Normal  -97.32  37.27   19|65|9223370572419254613       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      38:11.9 Normal  -93.61  35.51   19|65|9223370572419283934       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      38:23.9 Normal  -93.92  35.53   19|65|9223370572419271934       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      38:46.3 Normal  -97.32  37.15   19|65|9223370572419249513       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      35:48.8 Normal  -94.88  35.47   19|65|9223370572419427023       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      35:39.5 Normal  -92.25  34.75   19|65|9223370572419436304       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      35:14.1 Normal  -97.36  37.69   19|65|9223370572419461673       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      35:37.7 Normal  -92.53  35.15   19|65|9223370572419438123       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      38:21.3 Normal  -93.12  35.31   19|65|9223370572419274513       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
19      65      36:08.1 Normal  -97.32  37.27   19|65|9223370572419407704       1000    Ajay Singh      1962261785      Wichita to Little Rock.kml      2016-05-28-11
Time taken: 0.117 seconds, Fetched: 10 row(s)

String operation on table:

select concat(driverid,' ',drivername,' ',route_name) from truck_event_bucket_ext limit 5;
c0
NULL
10 George Vetticaden Saint Louis to Tulsa
10 George Vetticaden Saint Louis to Tulsa
10 George Vetticaden Saint Louis to Tulsa
10 George Vetticaden Saint Louis to Tulsa
Time taken: 0.151 seconds, Fetched: 5 row(s)

hive (travel)> select name, ssn, location, certified from drivers where certified = 'N';
OK
name    ssn     location        certified
George Vetticaden       621011971       244-4532 Nulla Rd.      N
Jamie Engesser  262112338       366-4125 Ac Street      N
Time taken: 0.193 seconds, Fetched: 2 row(s)

Using UDF in HIVE:

UDF in HIVE : /MRLab/src/com/mjyutika/AutoIncrementRowUDF.java
Jar file : AutoIncrementUDF

package com.mjyutika;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(stateful = true)
public class AutoIncrementRowUDF extends UDF{

	int ctr;

	public int evaluate() {
		ctr++;
		return ctr;
	}
}

add jar /home/notroot/lab/programs/AutoIncrementUDF.jar;
create temporary function autoincreserow as 'com.mjyutika.AutoIncrementRowUDF';
select autoincreserow() as inc, driverid, ssn, name from drivers;

OK
inc     driverid        ssn     name
1       NULL    NULL    name
1       10      621011971       George Vetticaden
1       11      262112338       Jamie Engesser
1       12      198041975       Paul Coddin
1       13      139907145       Joe Niemiec
1       14      820812209       Adis Cesir
1       15      239005227       Rohit Bakshi
1       16      363303105       Tom McCuch
1       17      123808238       Eric Mizell
1       18      171010151       Grant Liu
1       19      160005158       Ajay Singh
1       20      921812303       Chris Harris
1       21      209408086       Jeff Markham
1       22      783204269       Nadeem Asghar
1       23      928312208       Adam Diaz
1       24      254412152       Don Hilborn
1       25      913310051       Jean-Philippe Playe
1       26      124705141       Michael Aube
1       27      392603159       Mark Lochbihler
1       28      959908181       Olivier Renault
1       29      185502192       Teddy Choi
1       30      282307061       Dan Rice
1       31      858912101       Rommel Garcia
1       32      290304287       Ryan Templeton
1       33      967409015       Sridhara Sabbella
1       34      391407216       Frank Romano
1       35      971401151       Emil Siemes
1       36      245303216       Andrew Grande
1       37      190504074       Wes Floyd
1       38      386411175       Scott Shaw
1       39      967706052       David Kaiser
1       40      208510217       Nicolas Maillard
1       41      308103116       Greg Phillips
1       42      853302254       Randy Gelhausen
1       43      977706052       Dave Patton

Saving output on local file system:

hive (travel)> insert overwrite local directory '/home/notroot/lab/driveroutput' select driverid, ssn, name from drivers;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. tez, spark) or using Hive 1.X releases.
Query ID = notroot_20170623052842_fad374d5-41c0-44ea-a0fa-f6ecd3d3bc02
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1498198946915_0006, Tracking URL = http://ubuntu:8088/proxy/application_1498198946915_0006/
Kill Command = /home/notroot/lab/software/hadoop-2.7.2/bin/hadoop job  -kill job_1498198946915_0006
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2017-06-23 05:28:50,876 Stage-1 map = 0%,  reduce = 0%
2017-06-23 05:28:57,314 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.48 sec
MapReduce Total cumulative CPU time: 1 seconds 480 msec
Ended Job = job_1498198946915_0006
Copying data to local directory /home/notroot/lab/driveroutput
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1   Cumulative CPU: 1.48 sec   HDFS Read: 6070 HDFS Write: 904 SUCCESS
Total MapReduce CPU Time Spent: 1 seconds 480 msec
OK
driverid        ssn     name
Time taken: 15.6 seconds
hive (travel)>


Saving output on HDFS:

hive (travel)> insert overwrite directory '/user/notroot/driverslist' select driverid, name, ssn, certified from drivers;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. tez, spark) or using Hive 1.X releases.
Query ID = notroot_20170623053919_7c3813ce-d043-4c6f-b9d3-23a765e2759a
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1498198946915_0008, Tracking URL = http://ubuntu:8088/proxy/application_1498198946915_0008/
Kill Command = /home/notroot/lab/software/hadoop-2.7.2/bin/hadoop job  -kill job_1498198946915_0008
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2017-06-23 05:39:27,709 Stage-1 map = 0%,  reduce = 0%
2017-06-23 05:39:34,206 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.24 sec
MapReduce Total cumulative CPU time: 1 seconds 240 msec
Ended Job = job_1498198946915_0008
Stage-3 is selected by condition resolver.
Stage-2 is filtered out by condition resolver.
Stage-4 is filtered out by condition resolver.
Moving data to: hdfs://localhost:9000/user/notroot/driverslist/.hive-staging_hive_2017-06-23_05-39-19_794_6343679769162739796-1/-ext-10000
Moving data to: /user/notroot/driverslist
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1   Cumulative CPU: 1.24 sec   HDFS Read: 6027 HDFS Write: 982 SUCCESS
Total MapReduce CPU Time Spent: 1 seconds 240 msec
OK
driverid        name    ssn     certified
Time taken: 15.629 seconds
hive (travel)>







notroot@ubuntu:~$ hdfs dfs -cat /user/notroot/driverslist/0*
\Nname\Ncertified
10George Vetticaden621011971N
11Jamie Engesser262112338N
12Paul Coddin198041975Y
13Joe Niemiec139907145Y
14Adis Cesir820812209Y
15Rohit Bakshi239005227Y
16Tom McCuch363303105Y
17Eric Mizell123808238Y
18Grant Liu171010151Y
19Ajay Singh160005158Y
20Chris Harris921812303Y
21Jeff Markham209408086Y
22Nadeem Asghar783204269Y
23Adam Diaz928312208Y
24Don Hilborn254412152Y
25Jean-Philippe Playe913310051Y
26Michael Aube124705141Y
27Mark Lochbihler392603159Y
28Olivier Renault959908181Y
29Teddy Choi185502192Y
30Dan Rice282307061Y
31Rommel Garcia858912101Y
32Ryan Templeton290304287Y
33Sridhara Sabbella967409015Y
34Frank Romano391407216Y
35Emil Siemes971401151Y
36Andrew Grande245303216Y
37Wes Floyd190504074Y
38Scott Shaw386411175Y
39David Kaiser967706052Y
40Nicolas Maillard208510217Y
41Greg Phillips308103116Y
42Randy Gelhausen853302254Y
43Dave Patton977706052Y
