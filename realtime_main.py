import subprocess
import time

from pyspark.sql import SparkSession

from modules.transform import transform_iris_data
from modules.load import load_to_hdfs


# HDFS-mapper som real-time systemet arbejder med.
INPUT_DIR = "/user/amein/Input_dir"
OUTPUT_DIR = "/user/amein/Output_dir"

# Kilde-URL'en bruges af Load til at udlede outputfilens navn.
SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "jbrownlee/Datasets/master/iris.csv"
)

# Hvor ofte systemet kontrollerer Input_dir for ændringer.
POLL_INTERVAL_SECONDS = 2


def get_csv_file_state(hdfs_dir: str) -> dict[str, tuple]:
    """
    Henter information om CSV-filer i en HDFS-mappe.

    Returnerer et dictionary hvor:
        key   = filens HDFS-sti
        value = metadata om filen
                (størrelse, dato og tidspunkt)

    Metadata bruges til både at opdage:
    - nye CSV-filer
    - eksisterende CSV-filer der bliver ændret/overskrevet
    """

    result = subprocess.run(
        ["hdfs", "dfs", "-ls", hdfs_dir],
        capture_output=True,
        text=True,
        check=True
    )

    file_state = {}

    for line in result.stdout.splitlines():
        parts = line.split()

        if len(parts) >= 8:
            path = parts[-1]

            if path.endswith(".csv"):
                size = parts[4]
                modified_date = parts[5]
                modified_time = parts[6]

                file_state[path] = (
                    size,
                    modified_date,
                    modified_time
                )

    return file_state


def process_file(
    spark: SparkSession,
    file_path: str
):
    """
    Kører Transform og Load på den opdagede CSV-fil.
    """

    print(
        "Transform starter:",
        file_path,
        flush=True
    )

    transformed_df = transform_iris_data(
        spark,
        file_path
    )

    print(
        "Transform færdig.",
        flush=True
    )

    print(
        "Load starter...",
        flush=True
    )

    load_to_hdfs(
        transformed_df,
        SOURCE_URL,
        OUTPUT_DIR
    )

    print(
        "Load færdig.",
        flush=True
    )


def watch_input_dir():
    """
    Overvåger Input_dir i HDFS for nye eller ændrede CSV-filer.

    Når en CSV-fil bliver opdaget eller ændret,
    køres Transform og Load automatisk med Apache Spark.
    """

    spark = (
        SparkSession.builder
        .appName("IrisRealTimePipeline")
        .getOrCreate()
    )

    known_state = get_csv_file_state(INPUT_DIR)

    print(
        "Overvåger Input_dir...",
        flush=True
    )

    print(
        "Eksisterende filer:",
        set(known_state.keys()),
        flush=True
    )

    try:
        while True:
            time.sleep(POLL_INTERVAL_SECONDS)

            try:
                current_state = get_csv_file_state(INPUT_DIR)

                for file_path, metadata in current_state.items():

                    if file_path not in known_state:
                        print(
                            "Ny CSV-fil opdaget:",
                            file_path,
                            flush=True
                        )

                        process_file(
                            spark,
                            file_path
                        )

                    elif known_state[file_path] != metadata:
                        print(
                            "Ændret CSV-fil opdaget:",
                            file_path,
                            flush=True
                        )

                        process_file(
                            spark,
                            file_path
                        )

                # Den aktuelle tilstand gemmes og bruges
                # ved næste kontrol.
                known_state = current_state

            except subprocess.CalledProcessError as error:
                print(
                    "Kunne ikke kontrollere Input_dir:",
                    error,
                    flush=True
                )

    except KeyboardInterrupt:
        print(
            "\nReal-time overvågning stoppet.",
            flush=True
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    watch_input_dir()