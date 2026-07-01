from pathlib import Path

from xspatula import Initiate_process
from xspatula.setup import Initiate_database, Run_process

from .graph import default_graph_types, graph_capabilities
from .pipeline import XspatulaPipeline
from .schema_graph import XspatulaSchemaGraph


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

        self.pipeline = XspatulaPipeline(self)
        self.schema_graph = XspatulaSchemaGraph(self)

    def scheme(self, name_or_path):
        self.scheme_file = self.DEFAULT_SCHEMES.get(name_or_path, name_or_path)
        return self

    def job(self, name_or_path):
        self.job_file = self.DEFAULT_JOBS.get(name_or_path, name_or_path)
        return self

    def set_scheme(self, scheme_path):
        return self.scheme(scheme_path)

    def set_pilot(self, pilot_path):
        return self.job(pilot_path)

    @property
    def pilot_file(self):
        return self.job_file

    @pilot_file.setter
    def pilot_file(self, value):
        self.job_file = value

    def describe(self):
        return {
            "name": "Xspatula",
            "setup_path": str(self.setup_path),
            "api_version": "2.7.0",
            "available_schemes": self.DEFAULT_SCHEMES,
            "available_jobs": self.DEFAULT_JOBS,
            "selected": {
                "scheme_file": self.scheme_file,
                "job_file": self.job_file,
                "pilot_file": self.job_file,
            },
            "features": graph_capabilities(),
            "graphs": default_graph_types(),
            "methods": [
                "scheme(name_or_path)",
                "job(name_or_path)",
                "set_scheme(path)",
                "set_pilot(path)",
                "plan()",
                "run_database(interactive=True)",
                "run_processes()",
                "create_database(interactive=True)",
                "setup_processes()",
                "delete_database(interactive=True)",
                "build_pipeline()",
                "build_schema_graph(schema_name=None)",
                "describe()",
            ],
        }

    def build_pipeline(self):
        self._require_ready()
        return self.pipeline.build()

    def build_schema_graph(self, schema_name=None):
        if schema_name:
            return self.schema_graph.build_for_schema(schema_name)

        return self.schema_graph.build()

    def _require_ready(self):
        if not self.scheme_file:
            raise ValueError("No scheme selected. Call xp.scheme(...) first.")

        if not self.job_file:
            raise ValueError("No job selected. Call xp.job(...) first.")

    def plan(self):
        self._require_ready()

        scheme_abs = (self.setup_path / self.scheme_file).resolve()
        job_abs = (self.setup_path / self.job_file).resolve()

        return {
            "setup_path": str(self.setup_path),
            "scheme_file": self.scheme_file,
            "scheme_abs_path": str(scheme_abs),
            "job_file": self.job_file,
            "pilot_file": self.job_file,
            "job_abs_path": str(job_abs),
            "execution_steps": [
                "Load scheme file",
                "Resolve project path",
                "Load job file",
                "Resolve process files",
                "Dispatch process definitions",
                "Run selected workflow",
            ],
        }

    def run_database(self, interactive=True):
        self._require_ready()

        return Initiate_database(
            str(self.setup_path),
            self.scheme_file,
            self.job_file,
            interactive=interactive,
        )

    def run_processes(self):
        self._require_ready()

        structured_process_D, scheme_params_D = Initiate_process(
            str(self.setup_path),
            self.scheme_file,
            self.job_file,
        )

        if structured_process_D is not None:
            return Run_process(
                structured_process_D,
                scheme_params_D,
            )

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