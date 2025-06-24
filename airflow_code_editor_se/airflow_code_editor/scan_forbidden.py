"""

"""
import ast
import logging
from typing import Optional, Iterable, Tuple, Set
from flask import session

from airflow_code_editor.commons import (
    DEBUG,
    IS_ENABLE_SCAN,
    LEGAL_IMPORTS,
    FORBIDDEN_FUNCTIONS,
    FORBIDDEN_FUNCTIONS_PATTERNS,
)

__all__ = ['find_forbidden', 'Visitor', ]

log = logging.getLogger(__name__)


class Visitor(ast.NodeVisitor):
    """
    Пользовательский AST Visitor для анализа и обработки python-кода.
    """
    def __init__(
            self,
            ext_user_login: str,
            legal_imports: Iterable[str] = LEGAL_IMPORTS,
            forbidden_functions: Iterable[str] = FORBIDDEN_FUNCTIONS,
            forbidden_functions_patterns: Iterable[str] = FORBIDDEN_FUNCTIONS_PATTERNS
    ) -> None:
        self.ext_user_login = ext_user_login
        self.legal_imports = legal_imports
        self.forbidden_functions = forbidden_functions
        self.forbidden_functions_patterns = forbidden_functions_patterns
        self.imports = set()
        self.functions = set()
        self.owner = False

    def visit_Import(self, node: ast.Import) -> None:
        """
        Посещает узел обычного импорта (ast.Import) и обрабатывает его.
        """
        if DEBUG is True:
            log.info(f'{ast.dump(node)}')
        for alias in node.names:
            if not (alias.name in self.legal_imports or alias.name == 'se' or alias.name.startswith('se.')):
                self.imports.add(alias.name)
        # [fix] Скорее всего, не нужно будет заходить внутри, поэтому не вызываю метод generic_visit [fix]
        self.generic_visit(node)  # Обход дочерних узлов ноды

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """
        Посещает узел импорта из модуля (ast.ImportFrom) и обрабатывает его.
        """
        if DEBUG is True:
            log.info(f'{ast.dump(node)}')
        if isinstance(node.module, str) and \
                not (node.module in self.legal_imports or node.module == 'se' or node.module.startswith('se.')):
            self.imports.add(node.module)
        if node.names in self.forbidden_functions:
            self.functions.add(node.names)
        for x in self.forbidden_functions_patterns:
            for y in node.names:
                if x in y.name:
                    self.functions.add(y.name)
        # [fix] Скорее всего, не нужно будет заходить внутри, поэтому не вызываю метод generic_visit [fix]
        self.generic_visit(node)  # Обход дочерних узлов ноды

    def visit_Call(self, node: ast.Call) -> None:
        """
        Посещает узел вызова функции (ast.Call) и обрабатывает вызов функции и ее аргументы.
        """
        if DEBUG is True:
            log.info(f'{ast.dump(node)}')
        # вызов функции
        if isinstance(node.func, ast.Attribute):  # если функция вызывается как атрибут
            if isinstance(node.func.attr, str):
                if node.func.attr in self.forbidden_functions:
                    self.functions.add(node.func.attr)
                for x in self.forbidden_functions_patterns:
                    if x in node.func.attr:
                        self.functions.add(node.func.attr)
        if hasattr(node.func, 'id') and isinstance(node.func.id, str):
            if node.func.id in self.forbidden_functions:
                self.functions.add(node.func.id)
            for x in self.forbidden_functions_patterns:
                if x in node.func.id:
                    self.functions.add(node.func.id)
        # Поиск owner внутри словаря, созданного с помощью вызова dict(), и проверка его значения
        if not self.owner:
            if isinstance(node.func, ast.Name) and node.func.id == 'dict':
                for keyword in node.keywords:
                    if keyword.arg == 'owner' and hasattr(keyword.value, 'value'):
                        if keyword.value.value == self.ext_user_login:
                            self.owner = True
        self.generic_visit(node)  # Обход дочерних узлов ноды

    def visit_Name(self, node: ast.Name) -> None:
        """
        Посещает узел имени (ast.Name) и обрабатывает его.
        """
        if DEBUG is True:
            log.info(f'{ast.dump(node)}')
        # создание alias на функцию
        if isinstance(node, ast.Attribute):  # если alias смотрит на атрибут
            if isinstance(node.attr, str):
                if node.attr in self.forbidden_functions:
                    self.functions.add(node.attr)
                for x in self.forbidden_functions_patterns:
                    if x in node.attr:
                        self.functions.add(node.attr)
        if hasattr(node, 'id') and isinstance(node.id, str):
            if node.id in self.forbidden_functions:
                self.functions.add(node.id)
            for x in self.forbidden_functions_patterns:
                if x in node.id:
                    self.functions.add(node.id)
        # [fix] Скорее всего, не нужно будет заходить внутри, поэтому не вызываю метод generic_visit [fix]
        self.generic_visit(node)  # Обход дочерних узлов ноды

    def visit_Assign(self, node: ast.Assign) -> None:
        """
        Посещает узел присваивания (ast.Assign) и обрабатывает его.
        """
        if DEBUG is True:
            log.info(f'{ast.dump(node)}')
        # Поиск owner среди переменных и проверка его значения
        if not self.owner:
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == 'owner':
                    if isinstance(node.value, ast.Str) and node.value.s == self.ext_user_login:
                        self.owner = True
        self.generic_visit(node)  # Обход дочерних узлов ноды

    def visit_Dict(self, node: ast.Dict) -> None:
        """
        Посещает узел словаря (ast.Dict) и обрабатывает его ключи и значения.
        """
        if DEBUG is True:
            log.info(f'{ast.dump(node)}')
        # Поиск owner в словаре, созданном при помощи фигурных скобок, и проверка его значения
        if not self.owner:
            for k, v in zip(node.keys, node.values):
                if DEBUG is True:
                    log.info(f'{k=}; {v=}')
                if hasattr(k, 'value') and hasattr(v, 'value') and k.value and v.value and \
                        k.value == 'owner' and v.value == self.ext_user_login:
                    self.owner = True
        self.generic_visit(node)  # Обход дочерних узлов ноды


def find_forbidden(
        source: str,
        legal_imports: Iterable[str] = LEGAL_IMPORTS,
        forbidden_functions: Iterable[str] = FORBIDDEN_FUNCTIONS,
        forbidden_functions_patterns: Iterable[str] = FORBIDDEN_FUNCTIONS_PATTERNS
) -> Tuple[Optional[Set[str]], Optional[Set[str]], Optional[bool], Optional[str]]:
    """
    Сканирует на запрещённое.
    Возвращает Tuple из 4 элементов.
        По позициям:
            0 - список запрещённых импортов или None, если не найдено
            1 - список запрещённых функций или None, если не найдено
            2 - признак владельца, да/нет
            3 - текст ошибки, если таковая возникла
    """
    if IS_ENABLE_SCAN is False:
        return None, None, True, None
    try:
        ext_user = session.get('ext_user')
        if not ext_user:
            raise RuntimeError('Sorry, plugin not work without package "airflow_se"')
        ext_user_login = ext_user.get('login')

        tree: ast.Module = ast.parse(source)
        if DEBUG is True:
            log.info(f'{ast.dump(tree)}')
        visitor = Visitor(ext_user_login, legal_imports, forbidden_functions, forbidden_functions_patterns)
        visitor.visit(tree)

        imports, functions, owner = visitor.imports, visitor.functions, visitor.owner

        if DEBUG is True:
            log.info(f'{session=}')
        return imports if len(imports) > 0 else None, functions if len(functions) > 0 else None, owner, None
    except Exception as e:
        return None, None, None, str(e)

