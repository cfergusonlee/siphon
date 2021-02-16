from setuptools import setup


def readme():
    with open("README.md") as f:
        return f.read()


setup(
    name="siphon",
    version="0.1.0",
    description="Pandas Transfer Utility",
    long_description=readme(),
    author="Courtney Ferguson Lee",
    author_email="cfergusonlee@gmail.com",
    license="MIT",
    url="https://github.com/cfergusonlee/siphon",
    packages=["siphon"],
    keywords="pandas postgres sql",
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
    ],
    install_requires=["pandas>=1.2.0", "numpy", "sqlalchemy", "psycopg2"],
)
