from setuptools import setup, find_packages

setup(
    name="snowpark metadata",
    version="0.1.0",  # Update with your project's version
    description="Metadata ETL framework",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/jagadeeschaws/snowmeta.git",
    author="Jagadeesh",
    author_email="jagadeesch@gmail.com",
    license="MIT",  # Or your choice of license
    packages=find_packages(),
    install_requires=[
        "colorlog==6.7.0",
        "mparticle==0.15.0",
        "setuptools==65.5.1",
        "snowflake_connector_python[pandas]==3.0.0",
        "urllib3==1.26.14",
        "snowflake-snowpark-python==1.1.0",
        "Jinja2==3.1.2",
        "boto3==1.26.75",
        "botocore==1.29.77",
        "aiohttp==3.8.4",
        "aiohttp-retry==2.8.3",
        "aiosignal==1.3.1",
        "async-timeout==4.0.2",
        "attrs==22.2.0",
        "certifi==2022.12.7",
        "charset-normalizer==2.0.0",
        "Faker==18.4.0",
        "frozenlist==1.3.3",
        "idna==3.4",
        "multidict==6.0.4",
        "python-dateutil==2.8.2",
        "python-dotenv==1.0.0",
        "requests==2.28.2",
        "six==1.16.0",
        "yarl==1.8.2",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
