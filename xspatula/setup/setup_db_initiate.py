"""
 @file setup_db_initiate.py

 @brief Database setup orchestration module.
"""

from os import path, makedirs
from shutil import rmtree
import traceback

from xspatula.setup.setup_db import Setup_prod_roles
from xspatula.lib import Full_path_locate, Get_scheme_project_path_setup
from xspatula.setup.setup_db import Setup_prod_DB, Setup_schemas_tables
from xspatula.postgres.environment_path import get_environment_dir


def Get_user_response_boolean(prompt_text, interactive=True, default_response="y"):
    """
    Prompt user for yes/no confirmation.

    If interactive=False, return default_response without calling input().
    This keeps notebooks interactive while allowing Flask/API/headless runs.
    """

    print("\n⚠️    ✅  %s?: y\n      ❌ To skip/quit: n\n" % prompt_text)

    if not interactive:
        print("⚙️ Headless mode: auto-answering <%s>" % default_response)
        return default_response

    return input("⚠️ %s (y); stop(n): " % prompt_text)


def Delete_db_environments(notebook_FP):
    """
    Delete generated database environment files.
    """

    project_root = path.split(notebook_FP)[0]
    environment_path = get_environment_dir()

    if not environment_path:
        environment_path = path.join(project_root, "src", "postgres", "environment")

    if path.exists(environment_path):
        rmtree(environment_path)


def Create_db_environment_dot(notebook_FP, postgresDB_D):
    """
    Create per pg_user database environment files for database access.
    """

    project_root = path.split(notebook_FP)[0]
    environment_path = get_environment_dir()

    if not environment_path:
        environment_path = path.join(project_root, "src", "postgres", "environment")

    Delete_db_environments(notebook_FP)

    makedirs(environment_path, exist_ok=True)

    for item in postgresDB_D["db_users"]:
        user_id = item["user_id"]
        password = item["password"]

        dot_env_file = path.join(environment_path, f".{user_id}.env")

        with open(dot_env_file, "w", encoding="utf-8") as f:
            f.write(f"DB_NAME={postgresDB_D['db']}\n")
            f.write(f"DB_USER={user_id}\n")
            f.write(f"DB_PASSWORD={password}\n")
            f.write(f"DB_HOST={postgresDB_D['host']}\n")
            f.write(f"DB_PORT={postgresDB_D['port']}\n")

    print(f"\n. Database environment files created in: {environment_path}")


def Initiate_database(notebook_FP, scheme_file, proj_proc_file, interactive=True, default_response="y"):
    """
    Initiate the database setup/delete process.

    interactive=True:
        Keeps legacy notebook behavior and asks y/n.

    interactive=False:
        Does not call input(). Uses default_response.
        Intended for Flask, API, Agent and automated workflows.
    """

    print("Scheme file:", scheme_file)

    scheme_file = Full_path_locate(None, scheme_file, False, notebook_FP)

    success = Get_scheme_project_path_setup(scheme_file, proj_proc_file)

    if not success:
        return None

    scheme_params_D, json_process_file_FPN_L = success

    is_delete = scheme_params_D["process"][0]["delete"]

    if is_delete:
        print("\n.   ⚠️  Confirm that you want to delete parts of or the whole the database.\n")

        confirm = Get_user_response_boolean(
            "Delete database %s?" % scheme_params_D["postgresdb"]["db"],
            interactive=interactive,
            default_response=default_response
        )

        if confirm.lower() != "y":
            return None

    else:
        if scheme_params_D["process"][0]["verbose"] > 0:
            print("\n.   ⚠️  Confirm that you want to set up the database.\n")

            confirm = Get_user_response_boolean(
                "Set up database %s?" % scheme_params_D["postgresdb"]["db"],
                interactive=interactive,
                default_response=default_response
            )

            if confirm.lower() != "y":
                return None

        print("\n. Setting up system production database: %s" % scheme_params_D["postgresdb"]["db"])

        Create_db_environment_dot(notebook_FP, scheme_params_D["postgresdb"])

        success = Setup_prod_DB(scheme_params_D)

        if not success:
            Delete_db_environments(notebook_FP)
            return None

    if json_process_file_FPN_L:
        Setup_schemas_tables(scheme_params_D, json_process_file_FPN_L)

        if not is_delete:
            try:
                Setup_prod_roles(scheme_params_D)
            except Exception:
                traceback.print_exc()

    else:
        print("⚠️ No process to run")