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

TEMP_DIR = 'tmp\dag_git_sync'
TEMP_STATIC = os.path.join(TEMP_DIR, 'static')
TEMP_EXAMPLES = os.path.join(TEMP_DIR, 'examples')
target_example_path = 'tmp\target\example'
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

    for path in dag_paths:
        path_without_prefix = path.replace("dags\\", "")
        source_file = os.path.join(dag_folder, path_without_prefix)
        logging.warning("SourceFile: " + source_file)

        if os.path.exists(source_file):
            file_name = os.path.basename(source_file)

            if "example" in path:
                destination = os.path.join(TEMP_EXAMPLES, file_name)
            elif "static" in path:
                destination = os.path.join(TEMP_STATIC, file_name)
            else:
                logging.warning(f"Skipping unrecognized DAG type: {path}")
                continue
            logging.warning("Hey " + destination)
            shutil.copy2(source_file, destination)
            logging.info(f"Copied {source_file} to {destination}")
        else:
            logging.warning("Caught")

    print(os.getenv('ENABLED_GITSYNC'))
    if os.getenv('ENABLED_GITSYNC') == 'True':
        logging.warning("Iniside enable git sync -> pushing static files")
        base_dir = os.getcwd()  # gets the directory where the script is run from
        static_local_path = os.path.join(base_dir, dag_repo, 'static')

        git_static = GitUtil(
            source_path=TEMP_STATIC,
            repo_name=dag_repo,
            branch=branch,
            target_path=target_static_path,
            github_token=github_token,
            local_dir=static_local_path
        )
        git_static.git_push_files()
		
        

        # example_local_path = '/tmp/' + dag_repo + '/example'
        # git_examples = GitUtil(
        #     source_path=TEMP_EXAMPLES,
        #     repo_name=dag_repo,
        #     branch=branch,
        #     target_path=target_exTemple_path,
        #     github_token=github_token,
        #     local_dir=example_local_path
        # )
		# git_examples.clear_target_directory()
		# git_examples._copy_files()

        logging.info(f"Pushed DAG files from temp folder to repo {dag_repo} on branch {branch}.")
    else:
        logging.info("GitSync is disabled. No files pushed.")


if __name__ == "__main__":
    process_files()
