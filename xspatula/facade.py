from pathlib import Path

from xspatula import Initiate_process
from xspatula.setup import Initiate_database, Run_process

from .pipeline import XspatulaPipeline


class Xpatula:
    def __init__(self, setup_path="."):
        self.setup_path = Path(setup_path).resolve()
        self.scheme_file = None
        self.pilot_file = None

        # Pipeline subsystem
        self.pipeline = XspatulaPipeline(self)

    def set_scheme(self, scheme_path):
        self.scheme_file = scheme_path
        return self

    def set_pilot(self, pilot_path):
        self.pilot_file = pilot_path
        return self

    def scheme(self, scheme_path):
        return self.set_scheme(scheme_path)

    def pilot(self, pilot_path):
        return self.set_pilot(pilot_path)

    def build_pipeline(self):
        """
        Return a graph representation of the current execution pipeline.
        Launcher is responsible for rendering.
        """
        self._require_ready()
        return self.pipeline.build()

    def _require_ready(self):
        if not self.scheme_file:
            raise ValueError(
                "No scheme selected. Call xp.set_scheme(...) first."
            )

        if not self.pilot_file:
            raise ValueError(
                "No pilot selected. Call xp.set_pilot(...) first."
            )

    def plan(self):
        self._require_ready()

        scheme_abs = (self.setup_path / self.scheme_file).resolve()

        return {
            "setup_path": str(self.setup_path),
            "scheme_file": self.scheme_file,
            "scheme_abs_path": str(scheme_abs),
            "pilot_file": self.pilot_file,
            "execution_steps": [
                "Load scheme file",
                "Resolve project path",
                "Load pilot file",
                "Resolve process files",
                "Run selected workflow",
            ],
        }

    def run_database(self, interactive=True):
        self._require_ready()

        return Initiate_database(
            str(self.setup_path),
            self.scheme_file,
            self.pilot_file,
            interactive=interactive,
        )

    def run_processes(self):
        self._require_ready()

        structured_process_D, scheme_params_D = Initiate_process(
            str(self.setup_path),
            self.scheme_file,
            self.pilot_file,
        )

        if structured_process_D is not None:
            return Run_process(
                structured_process_D,
                scheme_params_D,
            )

        return None