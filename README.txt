Build:
=====
1. "mvn clean package".  This should create a hadoop-aws-s3r jar file in target folder.

Running:
========
1. Place target/hadoop-aws-sr3r*.jar in the classpath
2. Start cli as 'hive --hiveconf fs.s3a.impl="org.apache.hadoop.fs.s3r.S3RFileSystem" '
3. In hive cli, "ADD JAR file:///PATH_TO_YOUR_JAR/hadoop-aws-s3r-2.7.1.jar;"
    e.g "ADD JAR file:///home/rajesh/hadoop-aws-s3r-2.7.1.jar;" (in case the jar in home folder).
4. Run your queries as normal.

OOTB:
===
1. Attached here the pre-built hadoop-aws-s3r-2.7.1.jar (in case of checking quickly without having to recompile)
