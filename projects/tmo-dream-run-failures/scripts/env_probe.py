#!/usr/bin/env python3
"""
Environment probe: prints where dream, psana, numpy, mpi4py, and MPI come from.
Safe to call from shell setup scripts or MPI jobs.
"""

import os

def try_import(name):
    try:
        mod = __import__(name)
        return mod
    except Exception as e:
        print(f"{name}: FAILED TO IMPORT ({e})")
        return None

print("==== ENVIRONMENT PROBE ====")

# Python executable
print("Python executable:", os.path.realpath(os.sys.executable))

# dream
dream = try_import("dream")
if dream:
    print("dream module path:", os.path.realpath(dream.__file__))

# psana
psana = try_import("psana")
if psana:
    print("psana module path:", os.path.realpath(psana.__file__))

# numpy
numpy = try_import("numpy")
if numpy:
    print("numpy module path:", os.path.realpath(numpy.__file__))
    try:
        print("numpy version:", numpy.__version__)
    except Exception:
        pass

# mpi4py + MPI backend
mpi4py = try_import("mpi4py")
if mpi4py:
    from mpi4py import MPI
    print("mpi4py module path:", os.path.realpath(mpi4py.__file__))
    print("MPI vendor:", MPI.get_vendor())
    try:
        backend_path = MPI._backend.__file__
    except Exception:
        backend_path = "<unknown>"
    print("MPI backend .so:", backend_path)

print("============================")
