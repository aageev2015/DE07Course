import random
import math
import sys


class ReqIdGeneratorInterface:
    def __init__(self):
        pass

    def gen(self) -> str:
        pass


class ReqIdGeneratorRandom(ReqIdGeneratorInterface):
    REQ_ID_VALUE_MAX = sys.maxsize-1
    REQ_ID_VALUE_MIN = int(math.pow(10, math.floor(math.log10(REQ_ID_VALUE_MAX))))

    def __init__(self):
        super(ReqIdGeneratorRandom, self).__init__()

    def gen(self) -> str:
        # Critical section possible. Random is thread-safe. Locking no needed
        return str(random.randint(self.REQ_ID_VALUE_MIN, self.REQ_ID_VALUE_MAX))
