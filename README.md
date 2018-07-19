# Game App Using Spark & Scala

## Introduction

This application demonstrates a simple application using Spark, Scala and SBT. Players are divided into Teams and their
scores are recorded each day. At the end of the week, teams and players are given prizes who attain maximum points.
Sample files are included for demo purposes in resources folder - teams.dat and scores.dat.

## Setup and Running the App

After cloning this repository, run the following steps:

* Build jar using command: ``` mvn clean package ```
* Use spark-submit to execute the App supplying paths for various inputs required - teams.dat, scores.dat and for storing
RDD & DF outputs
```
spark-submit  target/spark_game-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/teams.dat /path/to/scores.dat /path/to/rddOutput /path/to/dfOutput
```

The output for rdd and df should be in their respective output paths supplied.

## Future Enhancements

Error checking and logging support need to be enhanced. Havent done them due to time constraints.
