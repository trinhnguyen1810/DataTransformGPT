from setuptools import setup, find_packages

setup(
    name="datatransformgpt",
    packages=find_packages(),
    version="0.1",
    install_requires=[
        "streamlit",
        "pandas",
        "redis",
        "snowflake-connector-python",
        "python-dotenv"
    ]
)