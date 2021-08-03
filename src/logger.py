from datetime import datetime
from logging import DEBUG, WARNING, FileHandler, Formatter, StreamHandler, getLogger

logger = getLogger()
logger.setLevel(WARNING)

formatter = Formatter(
    '%(asctime)s - %(levelname)s - %(thread)d - %(name)s: %(message)s')

fh = FileHandler('logs/{:%Y-%m-%d}.log'.format(datetime.now()))
fh.setFormatter(formatter)
fh.setLevel(DEBUG)

sh = StreamHandler()
sh.setFormatter(formatter)
sh.setLevel(WARNING)

logger.addHandler(fh)
logger.addHandler(sh)


class WithLogger():
    def __init__(self) -> None:
        self.logger = getLogger(self.__class__.__name__)
        self.logger.setLevel(DEBUG)
