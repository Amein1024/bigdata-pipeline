from pyspark.sql import SparkSession

from modules.extract import extract_to_hdfs
from modules.transform import transform_iris_data
from modules.load import load_to_hdfs


SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "jbrownlee/Datasets/master/iris.csv"
)

INPUT_DIR = "/user/amein/Input_dir"
OUTPUT_DIR = "/user/amein/Output_dir"


def main():
    """
    Kører hele ETL-pipelinen:

    1. Extract:
       Henter iris.csv direkte fra HTTPS-kilden til HDFS.

    2. Transform:
       Læser filen med PySpark og beholder kun Iris-setosa.

    3. Load:
       Gemmer det transformerede resultat i Output_dir på HDFS.
    """

    # Opretter en SparkSession til Transform- og Load-delen.
    # Programmet skal køres med spark-submit, så PySpark anvender
    # den installerede Apache Spark-platform.
    spark = (
        SparkSession.builder
        .appName("BigDataETLPipeline")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )

    try:
        print("Starter ETL-pipeline...")

        # EXTRACT
        print("1. Extract starter...")

        input_path = extract_to_hdfs(
            SOURCE_URL,
            INPUT_DIR
        )

        print("Extract færdig:", input_path)

        # TRANSFORM
        print("2. Transform starter...")

        # Spark kan læse HDFS-stien direkte.
        dataframe = transform_iris_data(
            spark,
            input_path
        )

        print(
            "Transform færdig. Antal Iris-setosa:",
            dataframe.count()
        )

        # LOAD
        print("3. Load starter...")

        output_path = load_to_hdfs(
            dataframe,
            SOURCE_URL,
            OUTPUT_DIR
        )

        print("Load færdig:", output_path)

        print("ETL-pipeline gennemført korrekt.")

    finally:
        # SparkSession stoppes altid, også hvis der opstår en fejl
        # under Extract, Transform eller Load.
        spark.stop()


if __name__ == "__main__":
    main()