# apache-spark-csv-sample
Apache Spark standalone script with output CSV file.
Analyze target logs, and counts by url each days.

## Usage

```
$ cd ${THIS_PROJECT_PATH}
$ sbt clean package
```

e.g)

```
$ sudo $SPARK_HOME/bin/spark-submit
  --class com.sample.csv.spark.SparkCSVSample
  --driver-class-path /usr/local/Cellar/apache-spark/2.2.1/libexec/jars/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46-bin.jar
  --master "local[2]"
  --driver-java-options "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
  target/scala-2.11/spark_csv_sample_2.11-1.0.0.jar logs/ tmp/spark_csv_sample.csv /hoge,/piyo
```

