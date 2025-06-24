"""

"""
from flask import request
import airflow
from functools import wraps
from airflow_code_editor.code_editor_view import AbstractCodeEditorView
from airflow_code_editor.commons import (
    ROUTE,
    MENU_CATEGORY,
    MENU_LABEL,
    JS_FILES,
    VERSION,
)

__all__ = ["admin_view"]

# ############################################################################
# Flask Admin (Airflow < 2.0 and rbac = False)

try:
    from flask_admin import BaseView, expose

    def login_required(func):
        # when airflow loads plugins, login is still None.
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            if airflow.login:
                return airflow.login.login_required(func)(*args, **kwargs)
            return func(*args, **kwargs)

        return func_wrapper

    class AdminCodeEditorView(BaseView, AbstractCodeEditorView):
        @expose("/")
        @login_required
        def index(self):
            return self._index()

        @expose("/repo", methods=["POST"])
        @login_required
        def repo_base(self, path=None):
            return self._git_repo(path)

        @expose("/repo/<path:path>", methods=["GET", "HEAD", "POST"])
        @login_required
        def repo(self, path=None):
            return self._git_repo(path)

        @expose("/files/<path:path>", methods=["POST"])
        @login_required
        def save(self, path=None):
            return self._save(path)

        @expose("/files/<path:path>", methods=["GET"])
        @login_required
        def load(self, path=None):
            return self._load(path)

        @expose("/format", methods=["POST"])
        @login_required
        def format(self, path=None):
            return self._load(path)

        @expose("/tree", methods=["GET"])
        @login_required
        def tree_base(self, path=None):
            return self._tree(path, args=request.args)

        @expose("/tree/<path:path>", methods=["GET"])
        @login_required
        def tree(self, path=None):
            return self._tree(path, args=request.args)

        @expose("/ping", methods=["GET"])
        @login_required
        def ping(self):
            return self._ping()

        def _render(self, template, *args, **kargs):
            return self.render(
                template + "_admin.html",
                airflow_major_version=self.airflow_major_version,
                js_files=JS_FILES,
                version=VERSION,
                *args,
                **kargs
            )

    admin_view = AdminCodeEditorView(url=ROUTE, category=MENU_CATEGORY, name=MENU_LABEL)

except (ImportError, ModuleNotFoundError):
    admin_view = None
