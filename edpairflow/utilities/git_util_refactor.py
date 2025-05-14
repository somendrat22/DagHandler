import os
import logging
import shutil
import git
import time

class GitUtil:
    def __init__(self,
                 source_path: str,
                 repo_name: str,
                 branch: str,
                 target_path: str,
                 github_token: str,
                 local_dir: str,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.source_path = source_path
        self.repo_name = repo_name
        self.branch = branch
        self.target_path = target_path
        self.github_token = github_token
        self.local_dir = local_dir
        self.organization = "somendrat22"
        self.project = "Edp"
	
    def _clear_target_directory(self):
        if os.path.exists(self.target_path):
            for item in os.listdir(self.target_path):
                item_path = os.path.join(self.target_path, item)
                try:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except Exception as e:
                    print(f"Failed to delete {item_path}. Reason: {e}")
        else:
            print(f"Target path {self.target_path} does not exist.")

    def _git_clone(self):
        username = "AzureDevOps"
        repo_url = f"https://{self.github_token}@dev.azure.com/{self.organization}/{self.project}/_git/{self.repo_name}"
        try:
            logging.warn("clone: " + self.local_dir)
            git.Repo.clone_from(repo_url, self.local_dir)
            print("Repository cloned successfully")
        except Exception as e:
            print(f"Error cloning repository: {e}")

    def _change_directory(self):
        os.chdir(self.local_dir)
        logging.warn("change_direct: " + self.local_dir)

    def _git_pull(self):
        repo = git.Repo(self.local_dir)
        repo.remotes.origin.pull()

    def _git_branch(self):
        logging.warn("branch: " + self.local_dir)
        repo = git.Repo(self.local_dir)
        repo.git.checkout(self.branch)

    def _copy_files(self):
        print(f"Source path: {self.source_path}")
        print(f"Target path: {self.target_path}")
        print(f"Source exists? {os.path.exists(self.source_path)}")
        print(f"Full source path: {os.path.abspath(self.source_path)}")
        try:
            if os.path.isfile(self.source_path):
                shutil.copy(self.source_path, self.target_path)
            else:
                shutil.copytree(self.source_path, self.target_path, dirs_exist_ok=True)
        except Exception as e:
            print(f"Error copying files: {e}")

    def _git_add_config(self, key, value):
        repo = git.Repo(self.local_dir)
        repo.config_writer().set_value("user", key, value).release()

    def _git_push(self, retries=8, delay=10):
        for attempt in range(retries):
            try:
                repo = git.Repo(self.local_dir)
                repo.git.add(A=True)
                repo.index.commit('Successfully synced the dags')
                repo.remotes.origin.push()
                print("Pushed successfully")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    print("All attempts to push changes failed.")
                    raise

    def _git_remove_repo(self):
        try:
            shutil.rmtree(self.local_dir)
            print(f"Git repository at {self.local_dir} removed successfully.")
        except Exception as e:
            print(f"Error removing Git repository: {e}")

    def git_push_files(self):
        self._git_clone()
        self._change_directory()
        self._clear_target_directory()
        self._git_branch()
        self._git_pull()
        self._copy_files()
        self._git_add_config("name", "Hello")
        self._git_add_config("email", "nobody@hello.com")
        self._git_push()
        self._git_remove_repo()