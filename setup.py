from setuptools import setup, Extension
import pybind11
import sys
import os

# Download and include nlohmann/json
NLOHMANN_JSON_PATH = "include/nlohmann"
os.makedirs(NLOHMANN_JSON_PATH, exist_ok=True)

if not os.path.exists(f"{NLOHMANN_JSON_PATH}/json.hpp"):
    import urllib.request
    url = "https://github.com/nlohmann/json/releases/download/v3.11.2/json.hpp"
    urllib.request.urlretrieve(url, f"{NLOHMANN_JSON_PATH}/json.hpp")

# Determine platform-specific flags
extra_compile_args = ['-std=c++17', '-O3', '-DPYBIND11_DETAILED_ERROR_MESSAGES']
if sys.platform == 'win32':
    extra_compile_args.extend(['/EHsc', '/arch:AVX2'])
else:
    extra_compile_args.extend([
        '-march=native',  # Use best available CPU instructions
        '-ffast-math',    # Aggressive floating point optimizations
        '-flto',          # Link-time optimization
    ])

ext_modules = [
    Extension(
        "ryanair.utils.path_finder",
        ["ryanair/utils/path_finder.cpp"],
        include_dirs=[
            pybind11.get_include(),
            pybind11.get_include(user=True),
            "include"  # Include directory for nlohmann/json
        ],
        language='c++',
        extra_compile_args=extra_compile_args,
    ),
]

setup(
    name="ryanair.utils.path_finder",
    ext_modules=ext_modules,
    install_requires=['pybind11>=2.6.0'],
) 