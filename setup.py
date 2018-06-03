import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name = "pydatras",
    version = "0.1",
    author = "Jorge Tornero, Spanish Institute of Oceanography",
    author_email = "jorge.tornero@ieo.es",
    description = "A simple package for downloading ICES/CIEM DATRAS dataset",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/jtornero/pydatras",
    packages = setuptools.find_packages(),
    install_requires = [
        'zeep>=2.5.0',
        'pandas>=0.21.0',
        'six>=1.11.0'
    ],
    license = 'GPLv3', 
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved ::  GNU General Public License v3",
        "Operating System :: OS Independent",
    
    ),
)