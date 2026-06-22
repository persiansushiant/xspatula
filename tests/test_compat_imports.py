from src.lib import Initiate_process
from src_setup.lib_setup import Initiate_database
from src_setup.lib_setup import Run_process

def test_legacy_core_import():
 assert Initiate_process is not None

def test_legacy_setup_imports():
 assert Initiate_database is not None
 assert Run_process is not None
