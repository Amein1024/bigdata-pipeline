"""Generate and store plots from decrypted Hive records."""

import os
import posixpath
import subprocess
import tempfile

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from modules.security import decrypt_record


NUMERIC_COLUMNS = (
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
)


def _values(dataframe):
    required_columns = set(NUMERIC_COLUMNS)
    missing_columns = required_columns.difference(dataframe.columns)

    if missing_columns:
        raise ValueError(
            "DataFrame mangler kolonner: "
            + ", ".join(sorted(missing_columns))
        )

    return [
        {
            column: float(row[column])
            for column in NUMERIC_COLUMNS
        }
        for row in dataframe.select(*NUMERIC_COLUMNS).collect()
    ]


def scatter_plot(dataframe, output_path: str) -> str:
    """Create a scatter plot of sepal length versus petal length."""

    values = _values(dataframe)
    figure, axis = plt.subplots(figsize=(8, 6))
    axis.scatter(
        [row["sepal_length"] for row in values],
        [row["petal_length"] for row in values]
    )
    axis.set_xlabel("Sepal length (cm)")
    axis.set_ylabel("Petal length (cm)")
    axis.set_title("Sepal length vs petal length (Iris-setosa)")
    axis.grid(True, alpha=0.3)
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path


def histogram(dataframe, output_path: str) -> str:
    """Create a ten-bin histogram of petal width."""

    values = _values(dataframe)
    figure, axis = plt.subplots(figsize=(8, 6))
    axis.hist(
        [row["petal_width"] for row in values],
        bins=10,
        edgecolor="black"
    )
    axis.set_xlabel("Petal width (cm)")
    axis.set_ylabel("Frequency")
    axis.set_title("Petal width distribution (Iris-setosa)")
    axis.grid(True, axis="y", alpha=0.3)
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path


def boxplot(dataframe, output_path: str) -> str:
    """Create a 2x2 boxplot layout for the four measurements."""

    values = _values(dataframe)
    figure, axes = plt.subplots(2, 2, figsize=(10, 8))

    for axis, column in zip(axes.flat, NUMERIC_COLUMNS):
        axis.boxplot([row[column] for row in values])
        axis.set_title(column)
        axis.set_ylabel("Value (cm)")
        axis.grid(True, axis="y", alpha=0.3)

    figure.suptitle("Iris-setosa measurement distributions")
    figure.tight_layout()
    figure.savefig(output_path, dpi=150)
    plt.close(figure)
    return output_path


def read_decrypted_hive_data(
    spark,
    database_name: str,
    table_name: str
):
    """Read the Hive table location and return a decrypted Spark DataFrame."""

    user_name = os.environ.get("USER", "amein")
    hive_data_path = (
        f"/user/{user_name}/hive_warehouse/"
        f"{database_name}/{table_name}"
    )
    encrypted_rows = (
        spark.read
        .parquet(hive_data_path)
        .select("encrypted_data")
        .collect()
    )
    records = [decrypt_record(row["encrypted_data"]) for row in encrypted_rows]

    if not records:
        raise ValueError("Hive-tabellen indeholder ingen data.")

    return spark.createDataFrame(records)


def upload_plot_to_hdfs(local_path: str, hdfs_output_dir: str) -> str:
    """Upload one generated image to HDFS and return its HDFS path."""

    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_output_dir],
        check=True
    )
    hdfs_path = (
        f"{hdfs_output_dir.rstrip('/')}/{posixpath.basename(local_path)}"
    )
    subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
        check=True
    )
    return hdfs_path


def generate_plots_from_hive(
    spark,
    database_name: str,
    table_name: str,
    hdfs_output_dir: str
) -> list[str]:
    """Read, decrypt, generate all three plots and upload them to HDFS."""

    dataframe = read_decrypted_hive_data(
        spark,
        database_name,
        table_name
    )

    with tempfile.TemporaryDirectory() as temporary_directory:
        plot_specs = (
            (scatter_plot, "scatter_plot.png"),
            (histogram, "histogram.png"),
            (boxplot, "boxplot.png"),
        )
        hdfs_paths = []

        for plot_method, filename in plot_specs:
            local_path = os.path.join(temporary_directory, filename)
            plot_method(dataframe, local_path)
            hdfs_paths.append(
                upload_plot_to_hdfs(local_path, hdfs_output_dir)
            )

    return hdfs_paths
