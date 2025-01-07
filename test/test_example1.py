"""
test_calc_func.py contains pytest tests for math functions.
pytest discovers tests named "test_*".
Each function in this module is a test case.
"""
import sys
import os
 
# Add folder1 to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
 
# Now you can import file1
# import example1
from example1 import *

NUMBER_1 = 3.0
NUMBER_2 = 2.0


def test_add():
    value = add(NUMBER_1, NUMBER_2)
    assert value == 5.0


def test_subtract():
    value = subtract(NUMBER_1, NUMBER_2)
    assert value == 1.0


def test_subtract_negative():
    value = subtract(NUMBER_2, NUMBER_1)
    assert value == -1.0


def test_multiply():
    value = multiply(NUMBER_1, NUMBER_2)
    assert value == 6.0


def test_divide():
    value = divide(NUMBER_1, NUMBER_2)
    assert value == 1.5