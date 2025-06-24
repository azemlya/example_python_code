"""

"""
from typing import Optional, List
import logging
import mimetypes
from flask import request, make_response
from flask_wtf.csrf import generate_csrf
from airflow.version import version
from airflow_code_editor.commons import Args, HTTP_404_NOT_FOUND
from airflow_code_editor.tree import get_tree
from airflow_code_editor.utils import (
    get_plugin_boolean_config,
    get_plugin_int_config,
    error_message,
    normalize_path,
    prepare_api_response,
)
from airflow_code_editor.git import (
    execute_git_command,
)
from airflow_code_editor.fs import RootFS
from airflow_code_editor.scan_forbidden import find_forbidden

__all__ = ["AbstractCodeEditorView"]

AIRFLOW_MAJOR_VERSION = int(version.split(".")[0])


class AbstractCodeEditorView(object):
    airflow_major_version = AIRFLOW_MAJOR_VERSION

    def _index(self):
        return self._render("index")

    def _save(self, path=None):
        data: Optional[str] = None
        try:
            # mime_type = request.headers.get("Content-Type", "text/plain")
            # is_text = mime_type.startswith("text/")
            # if is_text:
            # logging.info(f'Upload file: {path}')
            data = request.get_data(as_text=False).decode('utf-8-sig')
            # Newline fix (remove cr)
            data = data.replace("\r", "").rstrip() + "\n"
            # logging.info(f'Data in file {path}: {data}')
            # парсинг запрещённого
            parse_imports, parse_functions, parse_owner, parse_err = find_forbidden(data)
            errs: List[str] = []
            if parse_err:
                errs.append(f"An error occurred while parsing: {parse_err}")
            if parse_imports or parse_functions:
                errs.append(
                    ("FORBIDDEN!!! " +
                     (f"Imports: {list(parse_imports)}; " if parse_imports else "") +
                     (f"Functions: {list(parse_functions)}; " if parse_functions else "")
                     ).strip()
                )
            if parse_owner is False:
                errs.append("The wrong owner of the DAG, in the field \"owner\" substitute your login")
            if len(errs) > 0:
                raise RuntimeError(" ".join(errs))
            # else:  # Binary file
            #     raise RuntimeError("Don't upload binary files")
            #     data = request.get_data()
            root_fs = RootFS()
            # root_fs.path(path).write_file(data=data, is_text=is_text)
            root_fs.path(path).write_file(data=data, is_text=True)
            return prepare_api_response(path=normalize_path(path))
        except Exception as ex:
            # logging.error(ex)
            if len(data) > 4000:
                data = data[:1000] + '...\n<< FIRST 1000 CHARS :: SKIPPED :: LAST 1000 CHARS >>\n....' + data[-1000:]
            logging.error(f"Error saving {path}: {error_message(ex)}\nFile content:\n{data}")
            return prepare_api_response(
                path=normalize_path(path),
                error_message=f"Error saving {path}: {error_message(ex)}",
            )

    def _git_repo(self, path):
        if request.method == "POST":
            return self._git_repo_post(path)
        else:
            return self._git_repo_get(path)

    def _git_repo_get(self, path):
        """Get a file from GIT (invoked by the HTTP GET method)"""
        try:
            # Download git blob - path = '<hash>/<name>'
            path, attachment_filename = path.split('/', 1)
        except Exception:
            # No attachment filename
            attachment_filename = None
        response = execute_git_command(["cat-file", "-p", path]).prepare_git_response()
        if attachment_filename:
            content_disposition = 'attachment; filename="{0}"'.format(
                attachment_filename
            )
            response.headers["Content-Disposition"] = content_disposition
            try:
                content_type = mimetypes.guess_type(attachment_filename)[0]
                if content_type:
                    response.headers["Content-Type"] = content_type
            except Exception:
                pass
        return response

    def _git_repo_post(self, path):
        """Execute a GIT command (invoked by the HTTP POST method)"""
        git_args = request.json.get('args', [])
        return execute_git_command(git_args).prepare_git_response()

    def _load(self, path):
        "Send the contents of a file to the client"
        try:
            path = normalize_path(path)
            if path.startswith("~git/"):
                # Download git blob - path = '~git/<hash>/<name>'
                _, path = path.split("/", 1)
                return self._git_repo_get(path)
            else:
                # Download file
                root_fs = RootFS()
                return root_fs.path(path).send_file(as_attachment=True)
        except Exception as ex:
            logging.error(ex)
            strerror = getattr(ex, 'strerror', str(ex))
            errno = getattr(ex, 'errno', 0)
            message = prepare_api_response(error_message=strerror, errno=errno)
            return make_response(message, HTTP_404_NOT_FOUND)

    def _format(self):
        """Format code"""
        try:
            import black

            data = request.get_data(as_text=True)
            # Newline fix (remove cr)
            data = data.replace("\r", "").rstrip()
            mode = black.Mode(
                string_normalization=get_plugin_boolean_config("string_normalization"),
                line_length=get_plugin_int_config("line_length"),
            )
            data = black.format_str(src_contents=data, mode=mode)
            return prepare_api_response(data=data)
        except ImportError:
            return prepare_api_response(
                error_message="black dependency is not installed: to install black `pip install black`"
            )
        except Exception as ex:
            logging.error(ex)
            return prepare_api_response(error_message=f"Error formatting: {error_message(ex)}")

    def _tree(self, path, args: Args = {}):
        try:
            return prepare_api_response(value=get_tree(path, args))
        except Exception as ex:
            logging.error(ex)
            return prepare_api_response(
                value=[],
                error_message=f"Error: {error_message(ex)}",
            )

    def _ping(self):
        return {'value': generate_csrf()}
