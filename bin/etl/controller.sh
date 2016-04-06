JAVA_HOME=/usr
ETL_HOME=../../lib

CLASSPATH=$ETL_HOME/imdbetl.jar:$ETL_HOME/log4j-1.2.17.jar
CLASSPATH=$CLASSPATH:$ETL_HOME/cassandra-driver-core-3.0.0-rc1.jar:$ETL_HOME/cassandra-driver-dse-3.0.0-rc1.jar:$ETL_HOME/cassandra-driver-extras-3.0.0-rc1.jar:$ETL_HOME/cassandra-driver-mapping-3.0.0-rc1.jar
CLASSPATH=$CLASSPATH:$ETL_HOME/lib/guava-16.0.1.jar:$ETL_HOME/lib/HdrHistogram-2.1.4.jar:$ETL_HOME/lib/jackson-annotations-2.6.0.jar:$ETL_HOME/lib/jackson-core-2.6.3.jar:$ETL_HOME/lib/jackson-databind-2.6.3.jar
CLASSPATH=$CLASSPATH:$ETL_HOME/lib/javax.json-api-1.0.jar:$ETL_HOME/lib/joda-time-2.9.1.jar:$ETL_HOME/lib/lz4-1.2.0.jar:$ETL_HOME/lib/metrics-core-3.1.2.jar:$ETL_HOME/lib/netty-buffer-4.0.33.Final.jar
CLASSPATH=$CLASSPATH:$ETL_HOME/lib/netty-codec-4.0.33.Final.jar:$ETL_HOME/lib/netty-common-4.0.33.Final.jar:$ETL_HOME/lib/netty-handler-4.0.33.Final.jar:$ETL_HOME/lib/netty-transport-4.0.33.Final.jar:$ETL_HOME/lib/slf4j-api-1.7.7.jar:$ETL_HOME/lib/snappy-java-1.0.5.jar

OPTIONS=$@

$JAVA_HOME/bin/java -cp $CLASSPATH -Xms256m -Xmx2048m org.jude.bigdata.recroom.movies.etl.ETLController $OPTIONS --props=controller.properties --log4j=log4j.properties 

