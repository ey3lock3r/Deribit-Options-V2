class CBotResponseError(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.code = code

class CBotError(Exception):
    def __init__(self, message):
        super().__init__(message)