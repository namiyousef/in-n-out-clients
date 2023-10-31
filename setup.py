import codecs
import json
import os.path

from setuptools import find_packages, setup


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), "r") as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


def parse_requirements(path_to_file):
    with open(path_to_file) as f:
        requirements = f.readlines()

    return requirements


test_packages = ["pytest", "coverage", "pytest-dependency"]

core_packages = parse_requirements("requirements/core.txt")

with open("requirements/extra.json", "r") as f:
    extras_require = json.load(f)

setup(
    name="in-n-out-clients",
    version=get_version("in_n_out_clients/__init__.py"),
    description="Clients for In-N-Out",
    long_description="dummy",
    author="Yousef Nami",
    author_email="namiyousef@hotmail.com",
    url="https://github.com/namiyousef/in-n-out-clients",
    install_requires=core_packages,
    test_require=test_packages,
    packages=find_packages(exclude=("tests*", "experiments*")),
    extras_require=extras_require,
    # package_data={'': ['api/specs/api.yaml']},
    include_package_data=True,
    license="MIT",
    # entry_points={
    #    'console_scripts': ['in-n-out-api=in_n_out.run_api:'],
    # }
)
