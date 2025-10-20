from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Read requirements from requirements.txt
def read_requirements():
    requirements = []
    with open('requirements.txt', 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if line and not line.startswith('#') and not line.startswith('-'):
                # Extract package name and version
                if '==' in line:
                    requirements.append(line)
                elif line.startswith(' '):
                    # Skip indented dependency comments
                    continue
                else:
                    requirements.append(line)
    return requirements

setup(
    name='gigapi',
    version='0.1.0',
    author='gigapi',
    author_email='N/A',
    description='High-performance, schema-on-write database for time-series data management',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/gigapi/gigapi',
    packages=find_packages(),
    py_modules=[],  # Remove this line since modules are now in packages
    entry_points={
        'console_scripts': [
            'gigapi=gigapi.__main__:main',  # Update the entry point
        ],
    },
    package_data={
        'gigapi.views': ['ui.zip'],  # Update package data path
        '': ['*.yaml', '*.yml', '*.json', '*.toml'],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    python_requires='>=3.8',
    install_requires=[
        'duckdb==1.3.2',
        'fastapi==0.116.1',
        'uvicorn==0.35.0',
        'pydantic==2.11.7',
        'pydantic-settings==2.10.1',
        'icecream==2.1.5',
        'SQLAlchemy==2.0.41',
        'fsspec==2025.7.0',
        'python-dotenv==1.1.1',
        'aiohttp==3.12.14',
        'asyncpg==0.30.0',
        'fastparquet==2024.11.0',
        'thriftpy2==0.5.3',
        'line-protocol-parser==1.1.1',
        'assertpy==1.1',
        'objgraph==3.6.2',
        'attrs==25.3.0',
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-asyncio',
            'black',
            'flake8',
            'mypy',
        ],
        'flight': [
            'pyarrow==20.0.0',
        ],
        'aws': [
            'boto3==1.39.4',
            'botocore==1.39.4',
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords='database, time-series, parquet, duckdb, fastapi, analytics',
    project_urls={
        'Bug Reports': 'https://github.com/gigapi/gigapi/issues',
        'Source': 'https://github.com/gigapi/gigapi',
        'Documentation': 'https://github.com/gigapi/gigapi/blob/main/README.md',
    },
)