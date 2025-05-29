import os
import shutil
import yaml
import logging

from edpairflow.utilities.git_util_refactor import GitUtil
from dotenv import load_dotenv

load_dotenv()

dag_folder = 'dags'
dag_repo = os.getenv('GIT_REPO_NAME')
branch = os.getenv('GIT_REPO_BRANCH')
github_token = os.getenv('GITSYNC_PASSWORD')
config_file_path = 'config/dag_paths.yaml'


base_dir = os.getcwd()  # Gets the directory where the script is run from
TEMP_DIR = os.path.join(base_dir, 'tmp', 'dag_git_sync')
TEMP_STATIC = os.path.join(TEMP_DIR, 'static')
TEMP_EXAMPLES = os.path.join(TEMP_DIR, 'examples')
target_example_path = 'src\\main\\resources'
target_static_path = 'src\\main\\resources'


def read_configmap_file():
    logging.warning("Inside read configmap")
    dag_paths = []

    if not os.path.exists(config_file_path):
        logging.error(f"ConfigMap file {config_file_path} not found.")
        return dag_paths

    with open(config_file_path, 'r') as f:
        config = yaml.safe_load(f)

    if 'dags' in config:
        for entry in config['dags']:
            if 'path' in entry:
                dag_paths.append(entry['path'])
    logging.warning(dag_paths)

    return dag_paths


def prepare_temp_folders():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)

    os.makedirs(TEMP_STATIC, exist_ok=True)
    os.makedirs(TEMP_EXAMPLES, exist_ok=True)


def process_files():
    logging.warning("Inside process")
    if not os.path.exists(dag_folder):
        logging.warning(f"DAG folder {dag_folder} does not exist.")
        return

    logging.warning("Calling ")

    dag_paths = read_configmap_file()

    if len(dag_paths) == 0:
        logging.info("No DAG paths found in ConfigMap.")
        return

    prepare_temp_folders()
    logging.info(dag_paths)

    for path in dag_paths: #['dags\\example\\abcd.py', 'dags\\static\\efgh.py']
        path_without_prefix = path.replace("dags\\", "") # 'example\\abcd.py'
        source_file = os.path.join(dag_folder, path_without_prefix) #\opt\airflow\nogit\example\\abcd.py
        logging.warning("SourceFile: " + source_file)

        if os.path.exists(source_file):
            file_name = os.path.basename(source_file) #abcd.py

            if "example" in path:
                destination = os.path.join(TEMP_EXAMPLES, file_name)
            elif "static" in path:
                destination = os.path.join(TEMP_STATIC, file_name)
            else:
                logging.warning(f"Skipping unrecognized DAG type: {path}")
                continue
            logging.warning("Destination: " + destination)

            try:
                shutil.copy2(source_file, destination)  # Copying the file
                logging.info(f"Copied {source_file} to {destination}")
            except Exception as e:
                logging.error(f"Error copying file: {e}")
        else:
            logging.warning(f"Source file {source_file} does not exist.")
            continue

    print(os.getenv('ENABLED_GITSYNC'))
    if os.getenv('ENABLED_GITSYNC') == 'True':
        logging.warning("Inside enabled git sync -> pushing static files")
        
        # Absolute path for static_local_path
        base_dir = os.getcwd()  # gets the directory where the script is run from
        local_path = os.path.join(base_dir, 'tmp', dag_repo, 'local')  # Absolute path
        logging.warning(f"Static local path: {local_path}")
       

        git = GitUtil(
            TEMP_STATIC,
            dag_repo,
            branch,
            target_static_path,
            github_token,
            local_path
        )
        git.git_push_files()
        git.source_path = TEMP_EXAMPLES
        git.target_path = target_example_path
        git.git_push_files()


        logging.info(f"Pushed DAG files from temp folder to repo {dag_repo} on branch {branch}.")
    else:
        logging.info("GitSync is disabled. No files pushed.")



if __name__ == "__main__":
    process_files()
