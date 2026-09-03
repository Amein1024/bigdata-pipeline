# Big Data Pipeline

Projekt til faget **Big Data II**.

Formålet med projektet er at udvikle en Big Data-pipeline med en ETL-arkitektur (Extract, Transform, Load) ved brug af Apache Hadoop HDFS, Apache Spark og Python.

## Teknologier

- Apache Hadoop 3.5.0
- Apache Spark 4.2.0
- Python
- PySpark
- WSL / Ubuntu

## Krav 1: Kørsel

Start komponenterne i denne rækkefølge:

```bash
start-dfs.sh
schematool -dbType mysql -info
hive --service metastore
hive --service hiveserver2
```

Kontroller HDFS:

```bash
jps
hdfs dfs -mkdir -p /user/amein/Input_dir /user/amein/Output_dir
```

Start derefter real-time-pipelinen i en separat terminal:

```bash
spark-submit realtime_main.py
```

Læg en CSV-fil med header i `/user/amein/Input_dir`. Pipelinen filtrerer
`Iris-setosa`, gemmer resultatet som CSV i `Output_dir` og opretter tabellen
`iris_warehouse.transformed_iris` i Hive.

Vis resultatet med Beeline:

```bash
HADOOP_CLIENT_OPTS="-Dorg.jline.terminal.provider=dumb" \
	beeline -u 'jdbc:hive2://localhost:10000/default'
```

```sql
USE iris_warehouse;
SHOW TABLES;
SELECT * FROM transformed_iris LIMIT 10;
```

Hive-metastore er konfigureret til MySQL i `~/hive/conf/hive-site.xml`.
