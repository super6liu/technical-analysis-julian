from datetime import datetime
from logging import (DEBUG, ERROR, INFO, WARNING, FileHandler, Formatter,
                     StreamHandler, getLogger)

logger = getLogger()
logger.setLevel(DEBUG)

formatter = Formatter(
    '%(asctime)s %(levelname)-8s - %(name)s - %(funcName)s: %(message)s')

fh = FileHandler('logs/{:%Y-%m-%d_%H}-debug.log'.format(datetime.now()))
fh.setFormatter(formatter)
fh.setLevel(DEBUG)

fhError = FileHandler('logs/{:%Y-%m-%d_%H}-error.log'.format(datetime.now()))
fhError.setFormatter(formatter)
fhError.setLevel(ERROR)

sh = StreamHandler()
sh.setFormatter(formatter)
sh.setLevel(INFO)

logger.addHandler(fh)
logger.addHandler(fhError)
logger.addHandler(sh)


logger.debug("App starts.")
logger.setLevel(WARNING)

class WithLogger():
    def __init__(self) -> None:
        self.logger = getLogger(self.__class__.__name__)
        self.logger.setLevel(DEBUG)
