from .spec import RcloneSpecFS, RcloneSpecFile
from fsspec import register_implementation

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

register_implementation(RcloneSpecFS.protocol, RcloneSpecFS)

__all__ = ["__version__", "RcloneSpecFS", "RcloneSpecFile"]
