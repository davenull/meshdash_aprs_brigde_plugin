class ProtocolError(Exception):
    pass


class KissFramingError(ProtocolError):
    pass


class Ax25FrameError(ProtocolError):
    pass


class AprsMessageError(ProtocolError):
    pass
