Build:
=====
1. mvn clean package

Running:
========
1. Place hadoop-aws-sr3r*.jar in the classpath
2. Start cli as 'hive --hiveconf fs.s3a.impl="org.apache.hadoop.fs.s3r.S3RFileSystem" '
3. In hive cli, "ADD JAR file:///PATH_TO_YOUR_JAR/hadoop-aws-s3r-2.7.1.jar;"
4. Run your queries as normal.

