import runpy
import pathlib


def test_reference():
    path = pathlib.Path(__file__).parents[1] / 'reference/build.py'
    runpy.run_path(str(path), run_name='__main__')
