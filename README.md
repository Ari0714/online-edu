# Online Education Big Data Project

This project is a Spark Streaming data analysis demo for an online education platform. It reads user event logs from Kafka, processes them in real time with Spark Streaming, and computes key business metrics such as registration trends, video watch time, quiz correctness, and page navigation behavior.

## Project overview

The codebase implements a real-time data analysis pipeline built with Scala and Spark:

- Kafka collects raw user behavior logs.
- Spark Streaming consumes and processes those logs.
- Redis and MySQL are used as auxiliary storage and state management components.
- The actual business logic is implemented in the `app` package as independent streaming jobs.

This is a learning/demo project for big data processing, not a full web application.

## Tech Stack

- Java 8
- Scala 2.12
- Apache Spark 3.0.0
- Spark Streaming
- Kafka 2.4.1
- Redis
- MySQL
- Maven
- Fastjson

## Repository structure

```text
online_edu/
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── config.properties
│   │   │   └── log4j.properties
│   │   └── scala/
│   │       └── com/atguigu/online_edu/
│   │           ├── app/
│   │           │   ├── PageJump.scala
│   │           │   ├── QuestionRight.scala
│   │           │   ├── RealRegister.scala
│   │           │   └── VideoTime.scala
│   │           ├── entity/
│   │           │   └── DataModel.scala
│   │           ├── ods/
│   │           │   ├── learn.scala
│   │           │   ├── page.scala
│   │           │   ├── qz.scala
│   │           │   └── register_topic.scala
│   │           └── utils/
│   │               ├── MyKafkaUtil.scala
│   │               ├── MyKafkaSinkUtil.scala
│   │               ├── MyPropertiesUtil.scala
│   │               ├── MyRedisUtil.scala
│   │               ├── MySQLUtil.scala
│   │               └── OffsetManagerM.scala
│   └── test/
├── ch/
├── pom.xml
├── .gitignore
├── target/
└── README.md
```

## Main streaming jobs

The project currently contains these main classes:

- `com.atguigu.online_edu.app.RealRegister`
- `com.atguigu.online_edu.app.VideoTime`
- `com.atguigu.online_edu.app.PageJump`
- `com.atguigu.online_edu.app.QuestionRight`

These jobs correspond to the actual Scala object names in the project and are the classes to use when launching Spark jobs.

## Configuration

Update the Kafka, Redis, and MySQL settings in `src/main/resources/config.properties` before running the jobs:

```properties
# Kafka
kafka.broker.list=hdp101:9092,hdp102:9092,hdp103:9092

# Redis
redis.host=hdp101
redis.port=6379

# MySQL
url=jdbc:mysql://hdp103:3306/online_edu?characterEncoding=utf-8&useSSL=false
username=root
password=your_password
```

Replace the example hosts, ports, and credentials with the values used in your environment.

## Build

From the project root, run:

```bash
mvn clean package
```

This compiles the Scala sources and packages the project with dependencies into a runnable JAR.

## Run locally

The project uses Spark Streaming with local execution for testing. Example commands:

```bash
spark-submit \
  --class com.atguigu.online_edu.app.RealRegister \
  --master local[*] \
  target/online_edu-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Other classes can be run by changing the `--class` value:

```bash
spark-submit --class com.atguigu.online_edu.app.VideoTime --master local[*] target/online_edu-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --class com.atguigu.online_edu.app.PageJump --master local[*] target/online_edu-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --class com.atguigu.online_edu.app.QuestionRight --master local[*] target/online_edu-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Notes

- Kafka, Redis, and MySQL should be running before execution.
- The project is designed as a learning/demo big data pipeline.
- Some environment-specific settings may need adjustment depending on your cluster or local setup.

## License

No explicit license file is included in this repository. Please confirm the licensing terms before production use or redistribution.

