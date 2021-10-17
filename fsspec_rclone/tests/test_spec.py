import pytest
import os
import shutil
import tempfile
from datetime import datetime
from fsspec_rclone import RcloneSpecFS

test_files = {
    "hello1.txt": b"hello\n",
    "subdir/hello2.txt": b"hello, world!",
    "csv1.csv": b"""name,amount,id
Alice,100,1
Bob,200,2
Charlie,300,3
""",
}


@pytest.fixture(scope="session")
def fs():
    subdir = datetime.now().isoformat()
    temp_dir = os.path.join(tempfile.gettempdir(), "test-fsspec-rclone", subdir)
    os.makedirs(temp_dir)
    for name, data in test_files.items():
        path = os.path.join(temp_dir, name)
        dir = os.path.dirname(path)
        os.makedirs(dir, exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
    RcloneSpecFS.clear_instance_cache()
    fs = RcloneSpecFS(remote=temp_dir)
    fs.invalidate_cache()
    yield fs
    shutil.rmtree(temp_dir)


def test_read(fs):
    with fs.open("hello1.txt", "rb") as f:
        data = f.read()
        assert data == b"hello\n"


def test_write(fs):
    path = "hello2.tmp"
    try:
        with fs.open(path, "wb") as f:
            f.write(b"hello", flush=False)

        with fs.open(path, "rb") as f:
            assert f.read() == b"hello"

        with fs.open(path, "ab") as f:
            f.write(b",world", flush=True)

        with fs.open(path, "rb") as f:
            assert f.read() == b"hello,world"
    finally:
        fs.rm_file(path)


def test_ls(fs):
    ls = fs.ls("", detail=False, recurse=False)
    assert len(ls) == 3
    assert "hello1.txt" in ls
    assert "csv1.csv" in ls
    assert "subdir" in ls


def test_ls_recursive(fs):
    ls = fs.ls("", detail=False, recurse=True)
    assert len(ls) == 4
    assert "hello1.txt" in ls
    assert "csv1.csv" in ls
    assert "subdir" in ls
    assert "subdir/hello2.txt" in ls


def test_walk(fs):
    tree = list(fs.walk(""))
    assert len(tree) == 2
    assert tree[0] == ("", ["subdir"], ["csv1.csv", "hello1.txt"])
    assert tree[1] == ("subdir", [], ["hello2.txt"])

    tree = list(fs.walk("", maxdepth=1))
    assert len(tree) == 1
    assert tree[0] == ("", ["subdir"], ["csv1.csv", "hello1.txt"])

    tree = list(fs.walk("subdir"))
    assert len(tree) == 1
    assert tree[0] == ("subdir", [], ["hello2.txt"])


def test_find(fs):
    ff = fs.find("")
    assert ff == ["csv1.csv", "hello1.txt", "hello2.txt"]

    ff = fs.find("", maxdepth=1)
    assert ff == ["csv1.csv", "hello1.txt"]

    ff = fs.find("subdir")
    assert ff == ["hello2.txt"]


def test_exists(fs):
    assert fs.exists("subdir/hello2.txt")
    assert fs.exists("subdir")
    assert not fs.exists("no-such-file")


def test_info(fs):
    fi = fs.info("subdir/hello2.txt", show_hash=True)
    assert isinstance(fi, dict)
    assert fi["name"] == "hello2.txt"
    assert fi["path"] == "subdir/hello2.txt"
    assert fi["size"] == 13
    assert fi["type"] == "file"
    assert isinstance(fi["hash"], str)
    assert isinstance(fi["time"], str)
    assert "T" in fi["time"]

    fi = fs.info("subdir")
    assert isinstance(fi, dict)
    assert fi["name"] == "subdir"
    assert fi["path"] == "subdir"
    assert fi["size"] == -1
    assert fi["type"] == "directory"
    assert fi["hash"] == ""
    assert isinstance(fi["time"], str)
    assert "T" in fi["time"]


def test_checksum(fs):
    cs = fs.checksum("hello1.txt")
    assert isinstance(cs, str)
    assert cs != ""

    cs = fs.checksum("subdir")
    assert isinstance(cs, str)
    assert cs != ""


def test_cat_ranges(fs):
    path = "subdir/hello2.txt"
    assert fs.cat_file(path) == b"hello, world!"
    assert fs.cat_file(path, 7) == b"world!"
    assert fs.cat_file(path, 8, 10) == b"or"
    assert fs.cat_file(path, 2, 2) == b""
