class GigapiException(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.code = code