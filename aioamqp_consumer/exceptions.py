import pickle
import traceback

from . import settings
from .log import logger


class DeliveryError(Exception):
    pass


class Ack(DeliveryError):
    pass


class Reject(DeliveryError):
    pass


class DeadLetter(DeliveryError):
    pass


class RpcError(Exception):

    content_type = 'application/python-pickle;exception'

    def __init__(self, err):
        self.err = err

    def _dumps(self, err):
        return pickle.dumps(err, protocol=pickle.HIGHEST_PROTOCOL)

    def dumps(self):
        try:
            return self._dumps(self.err)
        except settings.PICKLE_ERRORS as exc:
            logger.warning(exc, exc_info=exc)

            return self._dumps(exc)
        except BaseException as exc:
            logger.critical(exc, exc_info=exc)

            try:
                return self._dumps(exc)
            except BaseException as exc:
                logger.critical(exc, exc_info=exc)

                exc_tb = traceback.format_exception(
                    etype=type(exc),
                    value=exc,
                    tb=exc.__traceback__,
                )

                return self._dumps(pickle.PickleError(''.join(exc_tb)))

    @classmethod
    def loads(cls, pkl):
        try:
            return cls(pickle.loads(pkl))
        except settings.PICKLE_ERRORS as exc:
            logger.warning(exc, exc_info=exc)

            return cls(exc)
        except BaseException as exc:
            logger.critical(exc, exc_info=exc)

            return cls(exc)
