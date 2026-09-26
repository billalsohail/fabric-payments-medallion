"""Fabric notebooks, in percent-cell ``.py`` form.

These are importable as modules so that ``orchestration/run.py`` can call them the way a Data
Factory **Notebook activity** does — by name, with parameters. On Fabric the kernel executes the
file itself; here the file is both a script and a module, which is why every notebook's parameter
cell resolves CLI flags only when it is the program being run (see ``src.runtime.params``).
"""
