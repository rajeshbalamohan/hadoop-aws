Build:
=====
1. mvn clean package

Table Creation:
==============
- This uses s3r as the scheme name. E.g table creation is given below. Notice "s3r".


CREATE TABLE `customer`(
  `c_custkey` int,
  `c_name` string,
  `c_address` string,
  `c_nationkey` int,
  `c_phone` string,
  `c_acctbal` double,
  `c_mktsegment` string,
  `c_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  's3r://x:y@apps/warehouse/tpch_flat_orc_x.db/customer'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false',
  'numFiles'='0',
  'numRows'='-1',
  'rawDataSize'='-1',
  'totalSize'='0',
  'transient_lastDdlTime'='1436942417');



Running:
========
1. Place hadoop-aws-sr3r*.jar in the classpath
2. Start hive metastore with #1 in classpath
		- Start cli as 'hive --hiveconf fs.s3r.impl="org.apache.hadoop.fs.s3r.S3RFileSystem" '
3. In hive cli, "ADD JAR file:///PATH_TO_YOUR_JAR/hadoop-aws-s3r-2.7.1.jar;"
4. Run your queries as normal.

HdfsSeekRead:
============
1. Used mainly for internal debugging and standalone testing
2. For given "s3 file, number of times to try, seq/random", it tries to read it locally for
testing and debugging
   e.g
   for random reads:
   java -cp $LOCATION/hadoop-aws-s3r-2.7.1.jar:$HADOOP_JARS:$HIVE_JARS HdfsSeekRead
   s3r://x:y@hwhive/test/data-m-100k.txt 11

   for sequential reads:
   java -cp $LOCATION/hadoop-aws-s3r-2.7.1.jar:$HADOOP_JARS:$HIVE_JARS HdfsSeekRead
      s3r://x:y@hwhive/test/data-m-100k.txt 11 seq

Issues :
=======
- In case you get unrecognized "s3r" filesystem, you might not have setup the classpath for the jar
properly.