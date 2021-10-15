# fsspec-rclone

Implementation of fsspec for rclone.

A simple example:

```python
import fsspec

with fsspec.open("rclone://sftp:host=localhost,user=myuser,pass=mypass,config=/etc/rclone/rclone.conf:path/to/file", "r") as f:
    print(f.read())
```

TBC
