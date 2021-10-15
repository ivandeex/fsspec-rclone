from .spec import RcloneSpecFS, RcloneSpecFile
from fsspec import register_implementation

__version__ = "0.0.1"

register_implementation(RcloneSpecFS.protocol, RcloneSpecFS)

__all__ = ["__version__", "RcloneSpecFS", "RcloneSpecFile"]
