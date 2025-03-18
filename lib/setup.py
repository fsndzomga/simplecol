from setuptools import setup, find_packages

setup(
    name="simplecol",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
    ],
    author="FS Ndzomga",
    description="Simple columnar storage system with query capabilities",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/fsndzomga/simplecol",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires='>=3.7',
)
