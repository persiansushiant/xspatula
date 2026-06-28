"""
 @file pg_session.py

 @brief PostgreSQL session and credential management module.
"""

from os import getenv
import netrc
from base64 import b64encode, b64decode

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql as pgsql

from xspatula.postgres.pg_common import PG_common
from xspatula.utils import Log
from xspatula.postgres.environment_path import get_environment_dir


class PG_session(PG_common):
    def __init__(self, environment_dot_file, verbose=0, session_id='unknown'):
        env_query_D = Get_env_var(environment_dot_file)

        if env_query_D is None:
            raise RuntimeError(
                '❌ ERROR - Could not load environment <%s>' % environment_dot_file
            )

        self.conn, self.cursor = PG_psycopg2_connect(env_query_D)
        self.session_id = session_id
        self.verbose = verbose

        PG_common.__init__(self)
        self.log = Log

    def _Close(self):
        self.cursor.close()
        self.conn.close()


def User_netrc_credentials(user_netrc_id):
    secrets = netrc.netrc()

    try:
        username, account, password = secrets.authenticators(user_netrc_id)
    except Exception:
        print('❌ ERROR - Could not retrieve credentials for <%s> from .netrc file' % user_netrc_id)
        return None

    password = b64encode(password.encode())
    return {'user_name': username, 'pswd': password}


def User_login_pswd(username, password):
    password = b64encode(password.encode())
    return {'user_name': username, 'pswd': password}


def PG_psycopg2_connect(env_query_D):
    pg_connection_D = {
        'dbname': env_query_D['db'],
        'user': env_query_D['user_name'],
        'password': b64decode(env_query_D['pswd']).decode('ascii'),
        'port': env_query_D['port'],
        'host': env_query_D['host']
    }

    conn = psycopg2.connect(**pg_connection_D)
    conn.autocommit = True
    cursor = conn.cursor()

    return conn, cursor


def PG_user_status(environment_dot_file, user_netrc_id, user_name=None, user_pswd=None):
    try:
        session = PG_session(environment_dot_file)

    except Exception as e:
        print('❌ ERROR - Could not connect to Postgres server, exiting')
        print('    Please check if the Postgres server is running and that you have access rights')
        print('    error:', e)
        return None

    if user_netrc_id:
        user_login_query_D = User_netrc_credentials(user_netrc_id)

    elif user_name and user_pswd:
        user_login_query_D = {'user_name': user_name, 'pswd': user_pswd}

    else:
        print('❌ ERROR - No user credentials given, exiting')
        return None

    if not user_login_query_D or not user_login_query_D['user_name']:
        return None

    plaintext_password = b64decode(user_login_query_D['pswd']).decode('ascii')

    select_cols = 'id, email, first_name, middle_name, last_name, user_name, stratum_code, status_code'

    if '@' in user_login_query_D['user_name']:
        sql = pgsql.SQL(
            'SELECT {} FROM community.user WHERE email = %s AND password = %s;'
        ).format(pgsql.SQL(select_cols))
    else:
        sql = pgsql.SQL(
            'SELECT {} FROM community.user WHERE user_name = %s AND password = %s;'
        ).format(pgsql.SQL(select_cols))

    session.cursor.execute(
        sql,
        (user_login_query_D['user_name'], plaintext_password)
    )

    rec = session.cursor.fetchone()

    session._Close()

    return rec


def Get_env_var(db):
    env_FN = f".{db}.env"

    environment_dir = get_environment_dir()
    env_path = environment_dir / env_FN

    if not env_path.exists():
        print(f"❌ ERROR: the database environment file <{env_FN}> does not exist")
        print(f"    Expected location: {env_path}")
        return None

    load_dotenv(env_path, override=True)

    DB_HOST = getenv("DB_HOST")
    DB_PORT = getenv("DB_PORT")
    DB_NAME = getenv("DB_NAME")
    DB_USER = getenv("DB_USER")
    DB_PASSWORD = getenv("DB_PASSWORD")

    missing = [
        k for k, v in (
            ('DB_HOST', DB_HOST),
            ('DB_PORT', DB_PORT),
            ('DB_NAME', DB_NAME),
            ('DB_USER', DB_USER),
            ('DB_PASSWORD', DB_PASSWORD),
        )
        if v is None
    ]

    if missing:
        print(
            '❌ ERROR: missing environment variable(s) in <%s>: %s'
            % (env_FN, ', '.join(missing))
        )
        return None

    return {
        'host': DB_HOST,
        'port': DB_PORT,
        'db': DB_NAME,
        'user_name': DB_USER,
        'pswd': b64encode(DB_PASSWORD.encode())
    }