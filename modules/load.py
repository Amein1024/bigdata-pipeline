import posixpath
import subprocess
from urllib.parse import urlparse

from pyspark.sql import DataFrame


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

    # Udleder det oprindelige filnavn fra download-URL'en.
    # Dermed hardcodes filnavnet ikke i Load-modulet.
    original_filename = posixpath.basename(
        urlparse(source_url).path
    )

    if not original_filename:
        raise ValueError(
            "URL'en indeholder ikke et gyldigt filnavn."
        )

    # Tilføjer prefixet "transformed_" til det oprindelige filnavn.
    # Eksempel:
    # iris.csv -> transformed_iris.csv
    transformed_filename = f"transformed_{original_filename}"

    # Fjerner et eventuelt afsluttende "/" fra Output_dir,
    # så HDFS-stierne bliver opbygget korrekt.
    hdfs_output_dir = hdfs_output_dir.rstrip("/")

    final_hdfs_path = (
        f"{hdfs_output_dir}/{transformed_filename}"
    )

    # Spark skriver CSV-output som en mappe med en part-fil.
    # Derfor skriver vi først til en midlertidig mappe på HDFS.
    temp_output_dir = (
        f"{hdfs_output_dir}/.{transformed_filename}_temp"
    )

    # Opretter Output_dir på HDFS, hvis den ikke allerede findes.
    #
    # SIKKERHED:
    # subprocess anvender en liste af argumenter og ikke shell=True.
    # Værdierne fortolkes derfor ikke som shell-kommandoer,
    # hvilket reducerer risikoen for command-line injection.
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_output_dir],
        check=True
    )

    # Fjerner et eventuelt gammelt midlertidigt output.
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", "-f", temp_output_dir],
        check=False
    )

    try:
        # coalesce(1) samler DataFrame-outputtet i én partition,
        # så Spark producerer én CSV part-fil.
        #
        # header=True sørger for, at CSV-filen indeholder
        # kolonnenavnene fra DataFrame'en.
        #
        # mode("overwrite") sikrer, at gammelt midlertidigt
        # Spark-output overskrives ved hver kørsel.
        (
            dataframe
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(temp_output_dir)
        )

        # Finder den CSV part-fil, som Spark har oprettet.
        #
        # Vi bruger hdfs dfs -ls i stedet for shell-globbing,
        # så der ikke er behov for shell=True.
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

        # Fjerner den tidligere transformerede CSV-fil.
        # Dermed overskrives gammelt output ved hver ny kørsel.
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-f", final_hdfs_path],
            check=False
        )

        # Flytter Spark-part-filen til det endelige filnavn.
        #
        # Eksempel:
        # part-00000-....csv
        # ->
        # transformed_iris.csv
        subprocess.run(
            ["hdfs", "dfs", "-mv", part_file, final_hdfs_path],
            check=True
        )

        # Den midlertidige Spark-mappe indeholder nu kun
        # metadata som _SUCCESS og kan derfor fjernes.
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", "-f", temp_output_dir],
            check=True
        )

        return final_hdfs_path

    except Exception:
        # ROBUSTHED:
        # Hvis skrivning eller flytning fejler, fjernes det
        # midlertidige output, så der ikke efterlades halvfærdige data.
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", "-f", temp_output_dir],
            check=False
        )

        raise