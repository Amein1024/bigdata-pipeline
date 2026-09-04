from modules.extract import extract_to_hdfs


SOURCE_URL = (
    "https://raw.githubusercontent.com/"
    "jbrownlee/Datasets/master/iris.csv"
)
INPUT_DIR = "/user/amein/Input_dir"


if __name__ == "__main__":
    output_path = extract_to_hdfs(SOURCE_URL, INPUT_DIR)
    print(f"Extract færdig: {output_path}")
