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

## Krav 2: Kryptering

Pipelinen bruger AES-256-GCM. GCM er valgt, fordi krypteringen samtidig
kontrollerer, om ciphertext er blevet ændret. Hver record får en ny tilfældig
nonce, og den samme plaintext giver derfor ikke den samme ciphertext.

Sikkerhedsmodulet indeholder også AES-256-CBC med HMAC-SHA256 som alternativ.
CBC kaldes ikke af pipelinen, fordi CBC alene ikke beskytter integriteten.

Generer en nøgle én gang pr. miljø og hold den uden for Git:

```bash
export PIPELINE_AES_KEY="$(python3 -c \
	'import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"
```

Når nøglen er sat, krypterer `load_to_hdfs` CSV-records, og `load_to_hive`
gemmer samme type krypterede records som Parquet i Hive. Beeline viser derfor
ciphertext, mens `decrypt_record` i `modules/security.py` kan bruges af en
senere visualiseringsdel efter læsning fra Hive.
