```
./
│  .gitignore
│  pom.xml
│  README.md
│  tree.txt
│  
├─.pic
│      banner-2.1.1.jpg
│      
└─src
    └─main
        ├─java
        │  └─org
        │      └─apache
        │          └─spark
        │              └─examples
        │                  │  JavaHdfsLR.java
        │                  │  JavaLogQuery.java
        │                  │  JavaPageRank.java
        │                  │  JavaSparkPi.java
        │                  │  JavaStatusTrackerDemo.java
        │                  │  JavaTC.java
        │                  │  JavaWordCount.java
        │                  │  
        │                  ├─sql
        │                  │  │  JavaSparkSQLExample.java
        │                  │  │  JavaSQLDataSourceExample.java
        │                  │  │  JavaUserDefinedTypedAggregation.java
        │                  │  │  JavaUserDefinedUntypedAggregation.java
        │                  │  │  
        │                  │  ├─hive
        │                  │  │      JavaSparkHiveExample.java
        │                  │  │      
        │                  │  └─streaming
        │                  │          JavaStructuredKafkaWordCount.java
        │                  │          JavaStructuredNetworkWordCount.java
        │                  │          JavaStructuredNetworkWordCountWindowed.java
        │                  │          
        │                  └─streaming
        │                          JavaCustomReceiver.java
        │                          JavaDirectKafkaWordCount.java
        │                          JavaFlumeEventCount.java
        │                          JavaKafkaWordCount.java
        │                          JavaNetworkWordCount.java
        │                          JavaQueueStream.java
        │                          JavaRecord.java
        │                          JavaRecoverableNetworkWordCount.java
        │                          JavaSqlNetworkWordCount.java
        │                          JavaStatefulNetworkWordCount.java
        │                          
        ├─resources
        │      employees.json
        │      full_user.avsc
        │      kv1.txt
        │      people.json
        │      people.txt
        │      user.avsc
        │      users.avro
        │      users.parquet
        │      
        └─scala
            └─org
                └─apache
                    └─spark
                        └─examples
                            │  BroadcastTest.scala
                            │  DFSReadWriteTest.scala
                            │  DriverSubmissionTest.scala
                            │  ExceptionHandlingTest.scala
                            │  GroupByTest.scala
                            │  HdfsTest.scala
                            │  LocalALS.scala
                            │  LocalFileLR.scala
                            │  LocalKMeans.scala
                            │  LocalLR.scala
                            │  LocalPi.scala
                            │  LogQuery.scala
                            │  MultiBroadcastTest.scala
                            │  SimpleSkewedGroupByTest.scala
                            │  SkewedGroupByTest.scala
                            │  SparkALS.scala
                            │  SparkHdfsLR.scala
                            │  SparkKMeans.scala
                            │  SparkLR.scala
                            │  SparkPageRank.scala
                            │  SparkPi.scala
                            │  SparkTC.scala
                            │  
                            ├─sql
                            │  │  RDDRelation.scala
                            │  │  SparkSQLExample.scala
                            │  │  SQLDataSourceExample.scala
                            │  │  UserDefinedTypedAggregation.scala
                            │  │  UserDefinedUntypedAggregation.scala
                            │  │  
                            │  ├─hive
                            │  │      SparkHiveExample.scala
                            │  │      
                            │  └─streaming
                            │          StructuredKafkaWordCount.scala
                            │          StructuredNetworkWordCount.scala
                            │          StructuredNetworkWordCountWindowed.scala
                            │          
                            └─streaming
                                │  CustomReceiver.scala
                                │  DirectKafkaWordCount.scala
                                │  FlumeEventCount.scala
                                │  FlumePollingEventCount.scala
                                │  HdfsWordCount.scala
                                │  KafkaWordCount.scala
                                │  NetworkWordCount.scala
                                │  QueueStream.scala
                                │  RawNetworkGrep.scala
                                │  RecoverableNetworkWordCount.scala
                                │  SqlNetworkWordCount.scala
                                │  StatefulNetworkWordCount.scala
                                │  StreamingExamples.scala
                                │  
                                └─clickstream
                                        PageViewGenerator.scala
                                        PageViewStream.scala
```