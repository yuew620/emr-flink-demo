# emr-flink-demo
This is a aws emr flink app . The app read from aws s3 and write to s3.
submit command example:
flink run -m yarn-cluster -yjm 1024 -ytm 4096 /home/hadoop/kinesis-flink-demo-13.0-SNAPSHOT.jar