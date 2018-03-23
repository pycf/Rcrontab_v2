import os
import sys

print(sys.path)

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)
print(sys.path)

print("sep", os.sep)
print("linesep:", os.linesep)
print("pathsep:", os.pathsep)
print(path)
print("environ:", os.environ)
print(os.name)

