from urllib.parse import urlparse, parse_qsl
import os
from typing import List

class LayerUrlHelper:
    def __init__(self, url: str):
        self.url = urlparse(url)
        self.scheme = self.url.scheme
        self._init()

    def _init(self):
        if self.url.scheme == "s3":
            self._init_s3()
        elif self.url.scheme == "file":
            self._init_fs()

    def _init_fs(self):
        self.prefix = (self.url.netloc + self.url.path).split("/")

    def _init_s3(self):
        parsed_url = self.url
        # Extract username and password
        if '@' in parsed_url.netloc:
            auth, host_port = parsed_url.netloc.split('@', 1)
            username, password = auth.split(':', 1)
        else:
            host_port = parsed_url.netloc
            username = password = None
        self.username = username
        self.password = password

        # Extract hostname and port
        if ':' in host_port:
            hostname, port = host_port.split(':', 1)
            port = int(port)
        else:
            hostname = host_port
            port = 443  # Default HTTPS port

        self.hostname = hostname
        self.port = port

        # Extract bucket name and prefix
        path_parts = parsed_url.path.strip('/').split('/')
        self.bucket_name = path_parts[0]
        self.prefix = path_parts[1:] if len(path_parts) > 1 else []
        # Parse query parameters
        query_params = dict(parse_qsl(parsed_url.query))
        self.use_ssl = query_params.get('secure', 'true').lower() == 'true'

    def set_prefix(self, prefix: List[str]):
        if self.scheme == "s3":
            self.url = self.url._replace(path=self.bucket_name + "/" + "/".join(prefix))
            self.prefix = prefix
        elif self.scheme == "file":
            self.url = urlparse("file://" + os.path.sep.join(prefix))
            self.prefix = prefix

    def string(self):
        return self.url.geturl()

