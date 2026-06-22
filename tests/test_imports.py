tests = [
    "import xspatula",
    "from src.lib import Initiate_process",
    "from src_setup.lib_setup import Initiate_database",
    "from src_setup.lib_setup import Run_process",
    "from xspatula.lib import Initiate_process",
    "from xspatula.lib import Structure_processes",
    "from xspatula.lib import Full_path_locate, Get_scheme_project_path_setup",
    "from xspatula.lib import Project_login",
    "from xspatula.lib.login import Get_set_database_session",
    "from xspatula.setup import Initiate_database",
    "from xspatula.setup import Run_process",
]

for statement in tests:
    try:
        exec(statement)
        print(f"OK: {statement}")
    except Exception as e:
        print(f"FAILED: {statement}")
        print(f"  {type(e).__name__}: {e}")