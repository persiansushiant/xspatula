from pathlib import Path

from xspatula import Initiate_process
from xspatula.setup import Initiate_database, Run_process


class XspatulaDatabase:
    def __init__(self, project):
        self.project = project

    def create(self, interactive=False):
        return Initiate_database(
            str(self.project.setup_path),
            self.project.scheme_setup,
            self.project.job_setup_db,
            interactive=interactive
        )

    def delete(self, interactive=False):
        return Initiate_database(
            str(self.project.setup_path),
            self.project.scheme_delete,
            self.project.job_delete_db,
            interactive=interactive
        )


class XspatulaProcesses:
    def __init__(self, project):
        self.project = project

    def setup(self):
        structured_process_D, scheme_params_D = Initiate_process(
            str(self.project.setup_path),
            self.project.scheme_use,
            self.project.job_setup_processes
        )

        if structured_process_D is not None:
            return Run_process(structured_process_D, scheme_params_D)

        return None


class Project:
    def __init__(
        self,
        setup_path,
        scheme_setup="./zzz/scheme_ai4sh_local_setup.json",
        scheme_use="./zzz/scheme_ai4sh_local_use.json",
        scheme_delete="./zzz/scheme_ai4sh_local_delete.json",
        job_setup_db="job_setup_db.json",
        job_setup_processes="job_setup_processes.json",
        job_delete_db="job_delete_db.json",
    ):
        self.setup_path = Path(setup_path).resolve()

        self.scheme_setup = scheme_setup
        self.scheme_use = scheme_use
        self.scheme_delete = scheme_delete

        self.job_setup_db = job_setup_db
        self.job_setup_processes = job_setup_processes
        self.job_delete_db = job_delete_db

        self.database = XspatulaDatabase(self)
        self.processes = XspatulaProcesses(self)

    @classmethod
    def open(cls, setup_path, **kwargs):
        return cls(setup_path, **kwargs)