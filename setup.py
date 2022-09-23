from setuptools import find_packages, setup

setup(
    name="sqlmesh",
    description="",
    #long_description=open("README.md").read(),
    #long_description_content_type="text/markdown",
    #url="https://github.com/TobikoData/sqlmesh",
    author="Tobiko Data, Inc.",
    #author_email="toby.mao@gmail.com",
    license="Apache-2.0",
    packages=find_packages(include=["sqlmesh", "sqlmesh.*"]),
    install_requires=[],
    extras_require={},
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: SQL",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
