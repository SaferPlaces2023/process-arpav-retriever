class StatusException(Exception):

    OK = 'OK'
    PARTIAL = 'PARTIAL'
    SKIPPED = 'SKIPPED'
    DENIED = 'DENIED'
    INVALID = 'INVALID'
    ERROR = 'ERROR'

    def __init__(self, status, message):
        self.status = status
        self.message = message
        super().__init__(self.message)