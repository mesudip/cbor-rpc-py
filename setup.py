from setuptools import setup, find_packages

setup(
    name="cbor-rpc",
    version="0.1.0",
    description="An async-compatible CBOR-based RPC system",
    author="Sudip Bhattarai",
    author_email="sudip@bhattarai.me",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/mesudip/cbor-rpc-py ",
    packages=find_packages(include=["cbor_rpc", "cbor_rpc.*"]),
    install_requires=["asyncssh>=2.14.0", "bcrypt", "cbor2", "ijson[yajl]"],
    extras_require={
        "test": ["pytest>=8.3.2", "pytest-asyncio>=0.24.0", "pytest-cov>=5.0.0", "docker"],
    },
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
