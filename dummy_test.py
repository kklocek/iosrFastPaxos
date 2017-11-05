import pytest

def foo():
  return 1

def test_answer():
  assert 1 == foo()
