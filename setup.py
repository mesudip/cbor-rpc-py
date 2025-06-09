from setuptools import setup, find_packages

setup(
    name="cbor-rpc",
    version="0.1.0",
    description="An async-compatible CBOR-based RPC system",
    author="Sudip Bhattarai",
    author_email="sudip@bhattarai.me",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    # url="https://github.com/your_username/cbor-rpc", # Replace with your project's URL
    packages=find_packages(exclude=["cbor_rpc"]),
    install_requires=[
        "pytest>=8.3.2",
        "pytest-asyncio>=0.24.0"
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
