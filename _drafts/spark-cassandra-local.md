---
layout: post
title: Install Spark and Cassandra locally
---

#Install Cassandra

### Install Java JDK

1. [Download JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
2. Verify java version
  ```
  java -version
  # Expected output: java version "1.8.0_75"

  javac -version
  # Expected output: javac 1.8.0_75
  ```
### Install Cassandra in ~/opt/packages

  ```
  mkdir -p ~/opt/packages && cd $_
  curl -O http://www.apache.org/dyn/closer.cgi?path=/cassandra/2.2.0/apache-cassandra-2.2.0-bin.tar.gz
  gzip -dc apache-cassandra-2.2.2-bin.tar.gz | tar xf -
  ln -s ~/opt/packages/apache-cassandra-2.2.2 ~/opt/cassandra
  ```

### Create data directories for Cassandra
    ```
    mkdir -p ~/opt/cassandra/data
    mkdir -p ~/opt/cassandra/data/data
    mkdir -p ~/opt/cassandra/data/commitlog
    mkdir -p ~/opt/cassandra/data/saved_caches
    mkdir -p ~/opt/cassandra/logs
    ```

### Add Cassandra to your Path
Add the following to your ~/.bash_profile
  ```
  # include locally installed Cassandra in PATH
  if [ -d "$HOME/opt" ]; then
      PATH="$PATH:$HOME/opt/cassandra/bin"
  fi
  ```
Restart your terminal. (Or `source .bash_profile`)

##Install Spark

### Install Scala on Mac

#### Install Homebrew
ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)" 

#### Install Scala
brew install scala 
export SCALA_HOME=/usr/local/bin/scala  
export PATH=$PATH:$SCALA_HOME/bin 

### Install Spark
```
cd ~/opt/packages
curl -O http://d3kbcqa49mib13.cloudfront.net/spark-1.4.1-bin-hadoop2.6.tgz
tar zxvf spark-1.4.1-bin-hadoop2.6.tgz
ln -s ~/opt/packages/spark-1.4.1 ~/opt/spark
```
### Add Spark to your Path
Add the following to your ~/.bash_profile
```
# include locally installed Cassandra in PATH
if [ -d "$HOME/opt" ]; then
    PATH="$PATH:$HOME/opt/spark/bin"
fi
```
Restart your terminal. (Or `source .bash_profile`)

##Spark Cassandra Connector

### Install in the command line
```
cd ~/opt/packages
git clone https://github.com/datastax/spark-cassandra-connector.git
cd spark-cassandra-connector
sbt/sbt assembly #Be patient, it takes a while...
```

# Use it!
## Scala shell with cassandra connector
1. Start the shell by adding the external connector jar
```
spark-shell --jars ~/opt/packages/spark-cassandra-connector/target/scala-2.11/connector-assembly-1.2.5-SNAPSHOT.jar
```

2. Use the scala shell
```
scala> sc.stop
scala> import com.datastax.spark.connector._
scala> import org.apache.spark.SparkContext
scala> import org.apache.spark.SparkContext._
scala> import org.apache.spark.SparkConf
scala> val conf = new SparkConf(true)
scala> conf.set("spark.cassandra.connection.host", "127.0.0.1")
scala> val sc = new SparkContext("local[2]", "Cassandra Connector Test", conf)
scala> val table = sc.cassandraTable("keyspace", "table")
scala> table.first
```

#References
- http://exponential.io/blog/2015/01/28/install-cassandra-2_1-on-mac-os-x/
- http://genomegeek.blogspot.ca/2014/11/how-to-install-apache-spark-on-mac-os-x.html
- http://stackoverflow.com/questions/25837436/how-to-load-spark-cassandra-connector-in-the-shell
- https://github.com/datastax/spark-cassandra-connector
- https://github.com/datastax/spark-cassandra-connector/blob/master/doc/0_quick_start.md
- https://github.com/datastax/spark-cassandra-connector/blob/master/doc/13_spark_shell.md
