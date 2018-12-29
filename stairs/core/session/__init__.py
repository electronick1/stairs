import threading
from stairs.core.utils import AttrDict

local = threading.local()
global_stairs = AttrDict()
