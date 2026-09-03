import getpass
import os
import posixpath
import re
import subprocess
from urllib.parse import urlparse

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)


def load_to_hdfs(
    dataframe: DataFrame,
    source_url: str,
    hdfs_output_dir: str
) -> str:
    """
    Gemmer en transformeret Spark DataFrame som en CSV-fil i HDFS.

    Outputfilens navn udledes automatisk fra den oprindelige URL.
    Eksempel:
    iris.csv -> transformed_iris.csv

    Eksisterende output overskrives ved en ny kørsel.

    Returnerer HDFS-stien til den færdige CSV-fil.
    """

    original_filename = posixpath.basename(
        urlparse(source_url).path
    )

    if not original_filename:
        raise ValueError(
            "URL'en indeholder ikke et gyldigt filnavn."
        )

    transformed_filename = f"transformed_{original_filename}"

    hdfs_output_dir = hdfs_output_dir.rstrip("/")

    final_hdfs_path = (
        f"{hdfs_output_dir}/{transformed_filename}"
    )

    temp_output_dir = (
        f"{hdfs_output_dir}/.{transformed_filename}_temp"
    )

    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_output_dir],
        check=True
    )

    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", "-f", temp_output_dir],
        check=False
    )

    try:
        (
            dataframe
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(temp_output_dir)
        )

        result = subprocess.run(
            ["hdfs", "dfs", "-ls", temp_output_dir],
            check=True,
            capture_output=True,
            text=True
        )

        part_file = None

        for line in result.stdout.splitlines():
            fields = line.split()

            if not fields:
                continue

            path = fields[-1]
            basename = posixpath.basename(path)

            if (
                basename.startswith("part-")
                and basename.endswith(".csv")
            ):
                part_file = path
                break

        if part_file is None:
            raise RuntimeError(
                "Spark oprettede ikke den forventede CSV part-fil."
            )

        subprocess.run(
            ["hdfs", "dfs", "-rm", "-f", final_hdfs_path],
            check=False
        )

        subprocess.run(
            ["hdfs", "dfs", "-mv", part_file, final_hdfs_path],
            check=True
        )

        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", "-f", temp_output_dir],
            check=True
        )

        return final_hdfs_path

    except Exception:
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", "-f", temp_output_dir],
            check=False
        )

        raise


def _validate_hive_identifier(identifier: str) -> None:
    """
    Validerer Hive database- og tabelnavne.

    Kun bogstaver, tal og underscore accepteres.
    Navnet må ikke starte med et tal.
    """

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(
            f"Ugyldigt Hive-navn: {identifier}"
        )


def _spark_type_to_hive(data_type) -> str:
    """
    Konverterer Spark-datatyper til tilsvarende Hive-datatyper.
    """

    if isinstance(data_type, StringType):
        return "STRING"

    if isinstance(data_type, IntegerType):
        return "INT"

    if isinstance(data_type, LongType):
        return "BIGINT"

    if isinstance(data_type, DoubleType):
        return "DOUBLE"

    if isinstance(data_type, FloatType):
        return "FLOAT"

    if isinstance(data_type, BooleanType):
        return "BOOLEAN"

    if isinstance(data_type, ShortType):
        return "SMALLINT"

    if isinstance(data_type, ByteType):
        return "TINYINT"

    if isinstance(data_type, DateType):
        return "DATE"

    if isinstance(data_type, TimestampType):
        return "TIMESTAMP"

    if isinstance(data_type, DecimalType):
        return (
            f"DECIMAL({data_type.precision},"
            f"{data_type.scale})"
        )

    raise ValueError(
        f"Spark-datatype understøttes ikke: {data_type}"
    )


def load_to_hive(
    dataframe: DataFrame,
    database_name: str,
    table_name: str
) -> str:
    """
    Gemmer den transformerede Spark DataFrame som en Hive-tabel.

    Processen er:

    1. DataFrame gemmes som Parquet på HDFS.
    2. Hive-databasen oprettes automatisk, hvis den ikke findes.
    3. Hive-tabellen oprettes automatisk.
    4. Tabellen forbindes med Parquet-dataene på HDFS.

    Hive SQL udføres gennem HiveServer2 med Beeline.

    Eksisterende tabel erstattes, så schemaet altid svarer
    til den transformerede DataFrame.

    Returnerer HDFS-stien til Hive-dataene.
    """

    _validate_hive_identifier(database_name)
    _validate_hive_identifier(table_name)

    current_user = getpass.getuser()

    hive_data_path = (
        f"/user/{current_user}/hive_warehouse/"
        f"{database_name}/{table_name}"
    )

    # ---------------------------------------------------------
    # 1. Gem transformeret DataFrame som Parquet på HDFS
    # ---------------------------------------------------------

    (
        dataframe
        .write
        .mode("overwrite")
        .parquet(hive_data_path)
    )

    # ---------------------------------------------------------
    # 2. Byg Hive-schema ud fra Spark DataFrame-schema
    # ---------------------------------------------------------

    hive_columns = []

    for field in dataframe.schema.fields:
        _validate_hive_identifier(field.name)

        hive_type = _spark_type_to_hive(
            field.dataType
        )

        hive_columns.append(
            f"`{field.name}` {hive_type}"
        )

    if not hive_columns:
        raise ValueError(
            "DataFrame indeholder ingen kolonner."
        )

    columns_sql = ",\n".join(hive_columns)

    # ---------------------------------------------------------
    # 3. Hive SQL
    # ---------------------------------------------------------

    hive_sql = f"""
CREATE DATABASE IF NOT EXISTS `{database_name}`;

DROP TABLE IF EXISTS `{database_name}`.`{table_name}`;

CREATE EXTERNAL TABLE `{database_name}`.`{table_name}` (
{columns_sql}
)
STORED AS PARQUET
LOCATION '{hive_data_path}';
"""

    # ---------------------------------------------------------
    # 4. Find Hive Beeline
    # ---------------------------------------------------------

    beeline_path = os.path.expanduser(
        "~/hive/bin/beeline"
    )

    if not os.path.isfile(beeline_path):
        raise FileNotFoundError(
            f"Beeline blev ikke fundet: {beeline_path}"
        )

    # ---------------------------------------------------------
    # 5. Hive kræver Java 21
    # ---------------------------------------------------------

    hive_environment = os.environ.copy()

    hive_environment["JAVA_HOME"] = (
        "/usr/lib/jvm/java-21-openjdk-amd64"
    )

    # Hive 4.2 / JLine kan fejle, når Beeline køres
    # som subprocess uden en almindelig terminal.
    #
    # Vi tvinger derfor JLine til at bruge "dumb"
    # terminal-provider.
    hive_environment["HADOOP_CLIENT_OPTS"] = (
        hive_environment.get(
            "HADOOP_CLIENT_OPTS",
            ""
        )
        + " -Dorg.jline.terminal.provider=dumb"
    )

    # ---------------------------------------------------------
    # 6. Kør SQL gennem Hive Beeline
    # ---------------------------------------------------------

    result = subprocess.run(
        [
            beeline_path,
            "-u",
            "jdbc:hive2://localhost:10000",
            "-e",
            hive_sql
        ],
        env=hive_environment,
        check=False
    )

    # ---------------------------------------------------------
    # 7. Kontrollér at Beeline lykkedes
    # ---------------------------------------------------------

    if result.returncode != 0:
        raise RuntimeError(
            "Hive Load fejlede. "
            f"Beeline returnerede exit-kode "
            f"{result.returncode}."
        )

    return hive_data_path