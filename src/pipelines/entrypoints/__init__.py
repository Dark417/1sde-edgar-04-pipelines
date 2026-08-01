"""L6 -- job task entrypoints. Thin by rule: no logic lives here.

These are the only modules allowed to touch ``dbutils``. Anything that touches
``dbutils`` cannot run in the local test suite, and the local suite is where the two
tests that decide this project run.
"""
