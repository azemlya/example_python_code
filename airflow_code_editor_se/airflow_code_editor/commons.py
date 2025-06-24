"""

"""
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Optional, Union

__all__ = [
    'DEBUG',
    'IS_ENABLE_SCAN',
    'PLUGIN_NAME',
    'MENU_CATEGORY',
    'MENU_LABEL',
    'ROUTE',
    'STATIC',
    'CONFIG_SECTION',
    'DEFAULT_GIT_BRANCH',
    'SUPPORTED_GIT_COMMANDS',
    'HTTP_200_OK',
    'HTTP_404_NOT_FOUND',
    'PLUGIN_DEFAULT_CONFIG',
    'ROOT_MOUNTPOUNT',
    'JS_FILES',
    'ICON_HOME',
    'ICON_GIT',
    'ICON_TAGS',
    'FILE_ICON',
    'FOLDER_ICON',
    'ICON_LOCAL_BRANCHES',
    'ICON_REMOTE_BRANCHES',
    'VERSION_FILE',
    'VERSION',
    'Args',
    'GitOutput',
    'TreeOutput',
    'TreeFunc',
    'SET_PERMISSION_ON_LOAD',
    'LEGAL_IMPORTS',
    'FORBIDDEN_FUNCTIONS',
    'FORBIDDEN_FUNCTIONS_PATTERNS',
]

DEBUG: bool = False
IS_ENABLE_SCAN: bool = True

PLUGIN_NAME = 'code_editor'
MENU_CATEGORY = 'Sber Edition'
MENU_LABEL = 'Code Editor SE'
ROUTE = '/' + PLUGIN_NAME
STATIC = '/static/' + PLUGIN_NAME
CONFIG_SECTION = PLUGIN_NAME + '_plugin'
DEFAULT_GIT_BRANCH = 'develop'
HTTP_200_OK = 200
HTTP_404_NOT_FOUND = 404
SUPPORTED_GIT_COMMANDS = [
    'add',
    'branch',
    'checkout',
    'cat-file',
    'commit',
    'diff',
    'log',
    'ls-tree',
    'reset',
    'rm',
    'show',
    'stage',
    'status',
    'tag',
    'unstage',
]
PLUGIN_DEFAULT_CONFIG = {
    'enabled': True,
    'git_enabled': True,
    'git_cmd': 'git',
    'git_default_args': '-c color.ui=true',
    'git_author_name': None,
    'git_author_email': None,
    'git_init_repo': True,
    'root_directory': None,
    'line_length': 88,
    'string_normalization': False,
    'ignored_entries': '.*,__pycache__',
}
ROOT_MOUNTPOUNT = 'root'
JS_FILES = [
    'codemirror.js',
    'mode/python/python.js',
    'addon/fold/foldcode.js',
    'addon/fold/foldgutter.js',
    'addon/fold/indent-fold.js',
    'addon/fold/comment-fold.js',
    'addon/mode/loadmode.js',
    'addon/mode/simple.js',
    'addon/mode/overlay.js',
    'addon/dialog/dialog.js',
    'addon/search/searchcursor.js',
    'addon/search/search.js',
    'addon/search/jump-to-line.js',
    'mode/meta.js',
    'vim.js',
    'emacs.js',
    'sublime.js',
    'airflow_code_editor.js',
]

ICON_HOME = 'home'
ICON_GIT = 'work'
ICON_TAGS = 'style'
FILE_ICON = 'file'
FOLDER_ICON = 'folder'
ICON_LOCAL_BRANCHES = 'fork_right'
ICON_REMOTE_BRANCHES = 'public'

VERSION_FILE = Path(__file__).parent / 'VERSION'
VERSION = VERSION_FILE.read_text().strip()

Args = Dict[str, str]
GitOutput = Union[None, bytes, str]
TreeOutput = List[Dict[str, Any]]
TreeFunc = Callable[[Optional[str], Args], TreeOutput]

SET_PERMISSION_ON_LOAD: Tuple[Tuple[str, str, str], ...] = (
    ('Admin', 'can_list', 'CodeEditorSEView'),
    ('Admin', 'can_create', 'CodeEditorSEView'),
    ('Admin', 'menu_access', 'CodeEditorSEView'),
    ('Admin', 'menu_access', 'Code Editor SE'),
    ('Admin', 'menu_access', 'Sber Edition'),
    ('Op', 'can_list', 'CodeEditorSEView'),
    ('Op', 'can_create', 'CodeEditorSEView'),
    ('Op', 'menu_access', 'CodeEditorSEView'),
    ('Op', 'menu_access', 'Code Editor SE'),
    ('Op', 'menu_access', 'Sber Edition'),
    ('User', 'can_list', 'CodeEditorSEView'),
    ('User', 'can_create', 'CodeEditorSEView'),
    ('User', 'menu_access', 'CodeEditorSEView'),
    ('User', 'menu_access', 'Code Editor SE'),
    ('User', 'menu_access', 'Sber Edition'),
)

LEGAL_IMPORTS: Tuple[str, ...] = (
    'airflow',
    'airflow.models',
    'airflow.decorators',
    'airflow.utils',
    'airflow.utils.dates',
    'airflow.operators.empty',
    'airflow.operators.dummy',
    'airflow.operators.python',
    'airflow.providers.se.greenplum.hooks',
    'airflow.providers.se.greenplum.hooks.greenplum',
    'airflow.providers.se.greenplum.operators',
    'airflow.providers.se.greenplum.operators.greenplum',
    'airflow.providers.se.ctl.operators',
    'airflow.providers.se.ctl.operators.ctl',
    'airflow.providers.se.bitbucket.operators',
    'airflow.providers.se.bitbucket.operators.bitbucket',
    'airflow.providers.se.spark.operators',
    'airflow.providers.se.spark.operators.spark_sql',
    'airflow.providers.se.spark.operators.spark_submit',
    'airflow.providers.se.hive.operators',
    'airflow.providers.se.hive.operators.hive',
    'sqlalchemy',
    'sqlalchemy.orm',
    'contextlib',
    'typing',
    'datetime',
    'time',
    'pendulum',
    'arrow',
    'pandas',
    'pandas.io',
)
FORBIDDEN_FUNCTIONS: Tuple[str, ...] = ('open', 'print', )
FORBIDDEN_FUNCTIONS_PATTERNS: Tuple[str, ...] = ('read', 'write', 'dump', 'save', 'load', 'pickle', 'xcom', )

