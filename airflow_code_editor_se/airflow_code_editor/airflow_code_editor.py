"""

"""
import logging
from flask import Blueprint
from typing import Optional
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import cached_app

from airflow_se.obj_imp import AFScrtMngr, Role, Action, Resource, Permission

from airflow_code_editor.commons import DEBUG, STATIC, VERSION, SET_PERMISSION_ON_LOAD
from airflow_code_editor.utils import is_enabled
from airflow_code_editor.flask_admin_view import admin_view
from airflow_code_editor.app_builder_view import appbuilder_view

__author__ = 'Zemlyanskiy Aleksey Albertovich <Zemlyanskiy.A.Al@sberbank.ru>'
__version__ = VERSION

__all__ = ['CodeEditorPlugin']

log = logging.getLogger(__name__)

# Blueprint
code_editor_plugin_blueprint = Blueprint(
    'code_editor_plugin_blueprint',
    __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path=STATIC,
)


# Plugin
class CodeEditorPlugin(AirflowPlugin):
    name = 'code_editor_plugin'
    operators = []
    flask_blueprints = [code_editor_plugin_blueprint, ]
    hooks = []
    executors = []
    admin_views = [admin_view, ] if (is_enabled() and admin_view is not None) else []
    menu_links = []
    appbuilder_views = [appbuilder_view, ] if is_enabled() else []

    @classmethod
    def on_load(cls, *args, **kwargs):
        """При загрузке"""
        try:
            asm: AFScrtMngr = cached_app().appbuilder.sm
            for x in SET_PERMISSION_ON_LOAD:  # даём привилегии по списку
                role_add_perms(asm, *x)
        except Exception as e:
            log.error(f'Missing set permissions on load plugin CodeEditorPlugin: {e}')


def role_add_perms(asm: AFScrtMngr, role_name: str, action_name: str, resource_name: str):
    """Добавление привилегий в роль"""
    role: Optional[Role] = asm.find_role(name=role_name)
    action: Optional[Action] = asm.get_action(action_name) or asm.create_action(action_name)
    resource: Optional[Resource] = asm.get_resource(resource_name) or asm.create_resource(resource_name)
    perm: Optional[Permission] = asm.get_permission(action_name=action_name, resource_name=resource_name) or \
                                 asm.create_permission(action_name=action_name, resource_name=resource_name)
    if asm and action and resource and role and perm:
        asm.add_permission_to_role(role=role, permission=perm)

