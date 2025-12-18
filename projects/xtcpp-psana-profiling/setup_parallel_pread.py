from pathlib import Path

from Cython.Build import cythonize
from setuptools import Extension, setup

here = Path(__file__).parent

extensions = [
    Extension(
        name="parallel_pread",
        sources=["parallel_pread.pyx"],
        extra_compile_args=["-O3"],
        extra_link_args=[],
    )
]

setup(
    name="parallel_pread",
    ext_modules=cythonize(
        extensions,
        compiler_directives={"language_level": "3"},
    ),
)
