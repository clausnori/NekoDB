from setuptools import setup, find_packages

setup(
    name="NekoDB",
    version="1.0.1",
    description="Simple NonSql DB",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="ClausNori",
    author_email="tihiy.black@gmail.com",
    url="https://github.com/clausnori/NekoDB",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "xxhash","lz4","numpy"
    ],
)