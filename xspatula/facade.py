from pathlib import Path

from xspatula import Initiate_process
from xspatula.setup import Initiate_database, Run_process


class Xpatula:
    DEFAULT_SCHEMES = {
        "setup": "./zzz/scheme_ai4sh_local_setup.json",
        "use": "./zzz/scheme_ai4sh_local_use.json",
        "delete": "./zzz/scheme_ai4sh_local_delete.json",
    }

    DEFAULT_JOBS = {
        "setup_db": "job_setup_db.json",
        "setup_processes": "job_setup_processes.json",
        "delete_db": "job_delete_db.json",
    }

    def __init__(self, setup_path="."):
        self.setup_path = Path(setup_path).resolve()
        self.scheme_file = None
        self.job_file = None

    def scheme(self, name_or_path):
        self.scheme_file = self.DEFAULT_SCHEMES.get(name_or_path, name_or_path)
        return self

    def job(self, name_or_path):
        self.job_file = self.DEFAULT_JOBS.get(name_or_path, name_or_path)
        return self

    def run_database(self, interactive=True):
        if not self.scheme_file:
            raise ValueError("No scheme selected. Call xp.scheme(...) first.")

        if not self.job_file:
            raise ValueError("No job selected. Call xp.job(...) first.")

        return Initiate_database(
            str(self.setup_path),
            self.scheme_file,
            self.job_file,
            interactive=interactive
        )

    def run_processes(self):
        if not self.scheme_file:
            raise ValueError("No scheme selected. Call xp.scheme(...) first.")

        if not self.job_file:
            raise ValueError("No job selected. Call xp.job(...) first.")

        structured_process_D, scheme_params_D = Initiate_process(
            str(self.setup_path),
            self.scheme_file,
            self.job_file
        )

        if structured_process_D is not None:
            return Run_process(structured_process_D, scheme_params_D)

        return None

    def create_database(self, interactive=True):
        return (
            self
            .scheme("setup")
            .job("setup_db")
            .run_database(interactive=interactive)
        )

    def setup_processes(self):
        return (
            self
            .scheme("use")
            .job("setup_processes")
            .run_processes()
        )

    def delete_database(self, interactive=True):
        return (
            self
            .scheme("delete")
            .job("delete_db")
            .run_database(interactive=interactive)
        )