from xspatula import Initiate_process
from xspatula.lib import Structure_processes
from xspatula.setup import Initiate_database
from xspatula.setup import Run_process

def test_public_core_api_imports():
    assert Initiate_process is not None
    assert Structure_processes is not None

def test_public_setup_api_imports():
    assert Initiate_database is not None
    assert Run_process is not None
