import os
import sys

FILE_PATH = os.path.realpath(os.path.dirname(__file__))

CORE_PATH = '/home/yangrq/projects/pycore'
if CORE_PATH not in sys.path:
    sys.path.append(CORE_PATH)

