java -cp warmup-1.0-SNAPSHOT-jar-with-dependencies.jar sortparquet.SortParquet hdfs://mycluster:9000/user/sort/ hdfs://mycluster:9000/user/sort/output

hadoop jar warmup-1.0-SNAPSHOT-jar-with-dependencies.jar sortparquet.SortParquet /user/sort/ /user/sort/output