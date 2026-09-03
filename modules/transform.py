from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def transform_iris_data(
    spark: SparkSession,
    hdfs_input_path: str
) -> DataFrame:
    """
    Læser Iris-data fra HDFS med PySpark og filtrerer datasættet,
    så kun rækker med arten 'Iris-setosa' beholdes.

    Returnerer den transformerede Spark DataFrame.
    """

    # Læser CSV-filen direkte fra HDFS.
    #
    # header=True betyder, at første linje anvendes som kolonnenavne.
    # inferSchema=True får Spark til automatisk at bestemme datatyperne
    # for kolonnerne i stedet for at behandle alt som tekst.
    dataframe = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(hdfs_input_path)
    )

    # TRANSFORM:
    # Opgaven kræver, at kun Iris-setosa beholdes.
    # col("species") refererer til species-kolonnen i DataFrame'en.
    transformed_dataframe = dataframe.filter(
        col("species") == "Iris-setosa"
    )

    return transformed_dataframe