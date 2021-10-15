# import requests

import json
import logging
import os
import socket
import subprocess
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from requests import Session
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
from typing import Union, Any

DEFAULT_API_URL = "rc://"
DEFAULT_PORT = 5572
START_TIMEOUT = 4
QUIT_TIMEOUT = 4

logger = logging.getLogger("fsspec_rclone")


class RcloneSpecFS(AbstractFileSystem):
    """Rclone filesystem."""

    protocol = "rclone"

    def __init__(
        self,
        *args,
        remote: str = None,
        api_url: str = None,
        api_host: str = None,
        api_port: int = None,
        api_user: str = None,
        api_pass: str = None,
        api_spawn: bool = None,
        api_rclone: str = None,
        verbose: Union[int, bool] = None,
        **kwargs,
    ) -> None:
        if not remote and args:
            remote = args[0]
            args = args[1:]
        self._remote = remote or "."

        super().__init__(*args, **kwargs)

        if verbose is True:
            self._verbose = 2
        elif verbose is False or verbose is None:
            self._verbose = 0
        elif isinstance(verbose, int):
            self._verbose = verbose
        else:
            self._verbose = int(verbose)  # type: ignore

        api_url = api_url or DEFAULT_API_URL
        if "://" not in api_url:
            api_url = DEFAULT_API_URL + api_url
        u = urlparse(api_url)
        q = parse_qs(u.query)
        host = u.hostname or api_host
        port = u.port or api_port
        username = api_user or u.username or ""
        password = api_pass or u.password or ""

        if api_spawn is not None:
            spawn = api_spawn
        else:
            spawn = "spawn" in q
        if not host:
            spawn = True
            host = "localhost"
        if not port:
            port = DEFAULT_PORT
            if spawn:
                # detect free local port
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind(("", 0))
                port = s.getsockname()[1]
                s.close()

        self._api = f"http://{host}:{port}/"
        logger.warn("api url = %s", self._api)
        self._sess = Session()
        if username:
            self._sess.auth = (username, password)
            logger.warn("user = %s pass = %s" % self._sess.auth)

        self._rclone = None
        if spawn:
            # send additional settings via environment for better security
            rclone = str(api_rclone or q.get("rclone") or "rclone")
            env = os.environ.copy()
            env["RCLONE_VERBOSE"] = str(self._verbose)
            env["RCLONE_RC_ADDR"] = f"{host}:{port}"
            if username:
                env["RCLONE_RC_USER"] = username
                env["RCLONE_RC_PASS"] = password
            else:
                env["RCLONE_RC_NO_AUTH"] = "true"
                env["RCLONE_RC_USER"] = ""
                env["RCLONE_RC_PASS"] = ""
            self._rclone = subprocess.Popen(
                [rclone, "rcd", self._remote],
                env=env,
                stdin=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )

        # wait for rclone to come up
        deadline = datetime.now() + timedelta(seconds=START_TIMEOUT)
        version = None
        while not version and datetime.now() < deadline:
            try:
                res = self._do("core/version")
                version = res.get("version")
            except Exception:
                time.sleep(0.1)

        if self._rclone and self._rclone.poll() is not None:
            version = None
        if not version:
            if self._rclone:
                self._rclone.kill()
                self._rclone = None  # type: ignore
            raise Exception("Timeout connecting to rclone")
        logger.warn("rclone %s on port %d", version, port)

    def __del__(self) -> None:
        self.quit()

    def quit(self) -> None:
        if self._rclone is None:
            return
        try:
            logger.warn("stopping rclone...")
            self._do("core/quit")
            self._rclone.wait(QUIT_TIMEOUT / 2)
        except subprocess.TimeoutExpired:
            logger.warn("terminating rclone...")
            self._rclone.terminate()
            try:
                self._rclone.wait(QUIT_TIMEOUT / 2)
            except subprocess.TimeoutExpired:
                self._rclone.kill()
        finally:
            self._rclone = None  # type: ignore

    def _do(self, cmd: str, **kwargs: Any) -> Any:
        data = kwargs
        for key, val in data.items():
            if isinstance(val, dict):
                data[key] = json.dumps(val)
        res = self._sess.post(self._api + cmd, data=kwargs)
        code = res.status_code
        logger.warn("Command %s, args %s, status %d", cmd, kwargs, code)
        try:
            dres = res.json()
        except json.JSONDecodeError:
            logger.warn("Text API response: %s", res.text)
            raise
        if code != 200:
            logger.warn("Error API response: %s", dres)
            msg = dres.get("error")
            raise Exception(f"Rclone API returned status code {code}: '{msg}'")
        return dres

    def ls(self, path: str, detail: bool = True, recurse: bool = True):
        opt = {"recurse": recurse}
        res = self._do("operations/list", fs=self._remote, remote=path, opt=opt)
        res = res["list"]
        if not detail:
            res = sorted(obj["Path"] for obj in res)
        print(res)
        return res

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Any = None,
        autocommit: bool = True,
        cache_options: Any = None,
        **kwargs,
    ):
        """Return a file-like"""
        return RcloneSpecFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )

    def info(self, path, **kwargs):
        return {"name": path, "size": 0, "type": "-"}


class RcloneSpecFile(AbstractBufferedFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__content = None

    def _fetch_range(self, start, end):
        if self.__content is None:
            self.__content = self.fs.cat_file(self.path)
        content = self.__content[start:end]
        if "b" not in self.mode:
            return content.decode("utf-8")
        else:
            return content
