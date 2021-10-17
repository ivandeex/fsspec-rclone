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
from tempfile import NamedTemporaryFile
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
from typing import Union, Dict, Tuple, Optional, Any

DEFAULT_API_URL = "rc://"
DEFAULT_PORT = 5572
START_TIMEOUT = 4
QUIT_TIMEOUT = 4
CACHE_FILE_PREFIX = ".fsspec-rclone."

logger = logging.getLogger("fsspec_rclone")


class RcloneSpecFS(AbstractFileSystem):
    """Rclone filesystem."""

    protocol = "rclone"

    def __init__(  # noqa C901
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
        self.verbose = verbose
        self._fcache: Dict[str, Any] = {}
        self._hash1 = ""  # will be selected lazily

        if not remote and args:
            remote = args[0]
            args = args[1:]
        if not remote and "fstype" in kwargs:
            remote = self._remote_from_dict(kwargs)
            kwargs = {}
        self._fs = remote or "."

        super().__init__(*args, **kwargs)

        api_url = api_url or DEFAULT_API_URL
        if "://" not in api_url:
            api_url = DEFAULT_API_URL + api_url
        u = urlparse(api_url)
        q = parse_qs(u.query)
        host = u.hostname or api_host
        port = u.port or api_port or 0
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
        logger.debug("api url = %s", self._api)
        self._sess = Session()
        if username:
            self._sess.auth = (username, password)
            logger.debug("user = %s pass = %s" % self._sess.auth)

        self._rclone: Any = None
        if spawn:
            rclone = str(api_rclone or q.get("rclone") or "rclone")
            self._spawn_rclone(rclone, host, port, username, password)
        version = self._wait_rclone()
        if not version:
            self._stop_rclone(True)
            raise Exception("Timeout connecting to rclone")
        logger.debug("rclone %s on port %d", version, port)

    @property
    def verbose(self):
        return self._verbose

    @verbose.setter
    def verbose(self, verbose: Union[bool, int, None]):
        if verbose is True:
            verbose = 2
        elif verbose is False or verbose is None:
            verbose = 0
        else:
            verbose = int(verbose)
        if verbose >= 2:
            logger.setLevel(logging.DEBUG)
        elif verbose == 1:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.ERROR)
        self._verbose = verbose

    @staticmethod
    def _remote_from_dict(kwargs: Dict[str, Any]) -> str:
        fstype = kwargs.get("fstype")
        if not fstype:
            raise ValueError("please provide fstype")
        remote = ":%s" % kwargs.pop("fstype")
        for key, val in sorted(kwargs.items()):
            val = str(val)
            if "," in val:
                if "'" in val:
                    if '"' in val:
                        val = val.replace('"', '""')
                    val = '"' + val + '"'
                else:
                    val = "'" + val + "'"
            remote += f",{key}={val}"
        remote += ":"
        return remote

    def _spawn_rclone(
        self, rclone: str, host: str, port: int, username: str, password: str
    ) -> None:
        # send additional settings via environment for better security
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
            [rclone, "rcd", self._fs],
            env=env,
            stdin=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

    def _wait_rclone(self) -> Optional[str]:
        deadline = datetime.now() + timedelta(seconds=START_TIMEOUT)
        version = None
        while not version and datetime.now() < deadline:
            try:
                res = self._call("core/version")
                version = res.get("version")
            except Exception:
                time.sleep(0.1)
        if self._rclone and self._rclone.poll() is not None:
            version = None
        return version

    def __del__(self) -> None:
        self._stop_rclone(True)

    def _stop_rclone(self, graceful: bool) -> None:
        if not self._rclone:
            return
        if graceful:
            logger.info("stopping rclone...")
            self._call("core/quit")
            try:
                self._rclone.wait(QUIT_TIMEOUT)
                self._rclone = None
                return
            except subprocess.TimeoutExpired:
                pass
        if graceful:
            logger.info("terminating rclone...")
            self._rclone.terminate()
            try:
                self._rclone.wait(QUIT_TIMEOUT)
                self._rclone = None
                return
            except subprocess.TimeoutExpired:
                pass
        self._rclone.kill()
        self._rclone = None

    def _call(self, cmd: str, **kwargs: Any) -> Any:
        data = kwargs
        for key, val in data.items():
            if isinstance(val, dict):
                data[key] = json.dumps(val)
        res = self._sess.post(self._api + cmd, data=kwargs)
        code = res.status_code
        logger.debug("Command %s, args %s, status %d", cmd, kwargs, code)
        try:
            dres = res.json()
        except json.JSONDecodeError:
            logger.debug("Text API response: %s", res.text)
            raise
        if code != 200:
            logger.debug("Error API response: %s", dres)
            msg = dres.get("error")
            raise Exception(f"Rclone API returned status code {code}: '{msg}'")
        return dres

    def mkdir(self, path: str, create_parents: bool = True):
        return self.makedirs(path)

    def makedirs(self, path: str, exist_ok: bool = False):
        self._call("operations/mkdir", fs=self._fs, remote=path)

    def rmdir(self, path):
        self._call("operations/mkdir", fs=self._fs, remote=path)

    def _to_direntry(self, obj: dict, detail: bool = False) -> Union[str, dict]:
        if not detail:
            return obj["Path"]

        # select a hash if found many
        hashes = obj.get("Hashes", {})
        if self._hash1:
            hash1 = hashes.get(self._hash1, "")
        elif not hashes:
            hash1 = ""
        elif hashes.get("sha1"):  # prefer sha1
            hash1 = hashes["sha1"]
            self._hash1 = "sha1"
        elif hashes.get("md5"):  # then md5
            hash1 = hashes["md5"]
            self._hash1 = "md5"
        else:
            self._hash1 = sorted(hashes)[0]  # choose one
            hash1 = hashes.get(self._hash1, "")

        return {
            "name": obj["Name"],
            "path": obj["Path"],
            "size": obj["Size"],
            "time": obj["ModTime"],
            "hash": hash1,
            "type": "directory" if obj["IsDir"] else "file",
        }

    def info(self, path: str, show_hash: bool = False):
        logger.debug("getting details for %s", path)
        nil = {"name": "", "size": 0, "type": "-"}
        if path == "":
            return nil
        dir = os.path.dirname(path)
        lst = self.ls(dir, detail=True, recurse=False, show_hash=show_hash)
        logger.debug("lst of '%s' for '%s': %r", dir, path, lst)
        for obj in lst:
            if obj["path"] == path:
                return obj
        raise FileNotFoundError(path)

    def checksum(self, path: str) -> str:
        fi = self.info(path, show_hash=True)
        if fi["hash"]:
            return fi["hash"]
        else:
            return "%d/%s" % (fi["size"], fi["time"])

    def modified(self, path: str) -> str:
        fi = self.info(path, show_hash=True)
        return fi["time"]

    def ls(
        self,
        path: str,
        detail: bool = False,
        recurse: bool = False,
        show_hash: bool = False,
        sort: bool = False,
    ):
        opt = {
            "recurse": recurse,
            "showHash": show_hash,
        }
        res = self._call("operations/list", fs=self._fs, remote=path, opt=opt)
        lst = res["list"]
        if sort:
            lst.sort(key=lambda obj: obj["Path"])
        return [self._to_direntry(obj, detail) for obj in lst]

    def walk(  # noqa C901
        self,
        path: str,
        maxdepth: int = None,
        detail: bool = False,
        show_hash: bool = False,
    ):
        if maxdepth is None:
            maxdepth = 0
        opt = {
            "recurse": (maxdepth != 1),
            "showHash": show_hash,
        }
        res = self._call("operations/list", fs=self._fs, remote=path, opt=opt)
        lst = res["list"]

        def get_depth(obj: dict) -> Tuple[int, str]:
            path = obj["Path"]
            return path.count("/"), path

        lst.sort(key=get_depth)

        stack = [(0, "")]
        if path != "":
            stack = [get_depth({"Path": path + "/"})]
        while stack:
            rdepth, root = stack.pop(0)
            xdirs, xfiles = {}, {}  # detailed
            ldirs, lfiles = [], []  # no detail
            for obj in lst:
                depth, path = get_depth(obj)
                if depth != rdepth:
                    continue
                if root and not path.startswith(root):
                    continue
                if not obj["IsDir"]:
                    if detail:
                        obj = self._to_direntry(obj, True)
                        xfiles[obj["name"]] = obj
                    else:
                        lfiles.append(obj["Name"])
                    continue
                if maxdepth == 0 or rdepth + 1 < maxdepth:
                    stack.append((rdepth + 1, path + "/"))
                if detail:
                    obj = self._to_direntry(obj, True)
                    xdirs[obj["name"]] = obj
                else:
                    ldirs.append(obj["Name"])
            root = root.rstrip("/")
            if detail:
                yield root, xdirs, xfiles
            else:
                yield root, ldirs, lfiles

    def get_file(
        self,
        rpath: str,
        lpath: str,
        callback: Any = None,  # TODO ignored
        **kwargs,
    ):
        """Copy single remote file to local"""
        if self.isdir(rpath):
            os.makedirs(lpath, exist_ok=True)
            return None
        lpath_abs = os.path.abspath(lpath)
        res = self._call(
            "operations/copyfile",
            srcFs=self._fs,
            srcRemote=rpath,
            dstFs="/",
            dstRemote=lpath_abs,
        )
        # callback.relative_update(total_size)
        logger.debug("get_file('%s' -> '%s'): %r", rpath, lpath, res)

    def _rm(self, path: str):
        self._call("operations/deletefile", fs=self._fs, remote=path)

    def rm(self, path: str, recursive: bool = False, maxdepth: int = None) -> None:
        if recursive and maxdepth is None:
            self._call("operations/purge", fs=self._fs, remote=path)
        else:
            super().rm(path, recursive, maxdepth)

    def cp_file(self, path1: str, path2: str):
        self._call(
            "operations/copyfile",
            srcFs=self._fs,
            srcRemote=path1,
            dstFs=self._fs,
            dstRemote=path2,
        )

    def mv(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: int = None,
        **kwargs,
    ):
        if recursive or maxdepth:
            raise NotImplementedError
        self._call(
            "operations/movefile",
            srcFs=self._fs,
            srcRemote=path1,
            dstFs=self._fs,
            dstRemote=path2,
        )

    def invalidate_cache(self, path=None):
        if path is None:
            fcache = self._fcache
            self._fcache = {}
            for _, buf in fcache.items():
                buf["file"].close()
            return
        buf = self._fcache.get(path)
        if buf:
            del self._fcache[path]
            buf["file"].close()

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


class RcloneSpecFile(AbstractBufferedFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "b" not in self.mode:
            raise NotImplementedError("Only binary mode is supported")
        logger.debug("opened file %s in mode %s", self.path, self.mode)

    def read(self, length=-1):
        """
        Return data from cache, or fetch pieces as necessary
        Parameters:
        length: int (-1)
            Number of bytes to read; if <0, all remaining bytes.
        """
        logger.debug("read %s from %d by %d", self.path, self.loc, length)
        if self.mode != "rb":
            raise ValueError("File not in read mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        length = -1 if length is None else int(length)
        if length < 0:
            length = self.size - self.loc
            logger.debug("real length: %d", length)
        if length == 0 or self.size == 0:
            return b""

        buf = self.fs._fcache.get(self.path)
        if not buf:
            f = NamedTemporaryFile(prefix=CACHE_FILE_PREFIX)
            buf = {"file": f, "mode": self.mode}
            self.fs.get_file(self.path, f.name)
            self.fs._fcache[self.path] = buf
        elif buf["mode"] != "rb":
            raise ValueError("File is being written to")
        else:
            f = buf["file"]

        f.seek(self.loc)
        data = f.read(length)
        self.loc += len(data)
        return data

    def write(self, data: bytes, flush: bool = False):
        if self.mode not in {"wb", "ab"}:
            raise ValueError("File not in write mode")
        if self.closed:  # type: ignore
            raise ValueError("I/O operation on closed file.")
        if self.forced:  # type: ignore
            raise ValueError("This file has been force-flushed, can only close")

        buf = self.fs._fcache.get(self.path)
        if not buf:
            f = NamedTemporaryFile(prefix=CACHE_FILE_PREFIX)
            buf = {"file": f, "mode": self.mode}
            if self.mode == "ab":
                self.fs.get_file(self.path, f.name)
            self.fs._fcache[self.path] = buf
        elif buf["mode"] == "rb":
            buf["mode"] = self.mode
            f = buf["file"]
            if self.mode == "wb":
                f.truncate(0)
                f.seek(0)
            elif self.mode == "ab":
                f.seek(0, 2)

        num = f.write(data)
        self.loc += num
        logger.debug("%s: wrote %d bytes of %s, loc %d", self.path, num, data, self.loc)
        if flush:
            self.flush()
        return num

    def flush(self, force: bool = False) -> None:
        if self.closed:  # type: ignore
            return
        if self.mode not in {"wb", "ab"}:
            return
        buf = self.fs._fcache.get(self.path)
        if not buf or buf["mode"] == "rb":
            logger.debug("%s: nothing to flush", self.path)
            return
        f = buf["file"]
        f.flush()
        self.size = os.path.getsize(f.name)
        res = self.fs._call(
            "operations/copyfile",
            srcFs="/",
            srcRemote=f.name,
            dstFs=self.fs._fs,
            dstRemote=self.path,
        )
        buf["mode"] = "r"  # mark as written
        logger.debug(
            "flushed %s from %s size %d result %r", self.path, f.name, self.size, res
        )
