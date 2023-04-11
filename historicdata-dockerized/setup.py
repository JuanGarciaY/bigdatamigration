from setuptools import setup, find_packages

setup(
    name="test",
    version="1.0.0",
    description="Mi proyecto de ejemplo",
    author="Mi Nombre",
    author_email="miemail@example.com",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]",
        "pyodbc"
    ]
)   