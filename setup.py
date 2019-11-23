import os
from setuptools import setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


README = """
Mutual Exclusion using AWS DynamoDB
"""

setup(
    name='dyndbmutex',
    version='0.4.1',
    license='ASL v2.0',
    packages=['dyndbmutex'],
    description='Mutual Exclusion using AWS DynamoDB',
    long_description=README,
    url='https://github.com/chiradeep/dyndb-mutex',
    download_url='https://github.com/chiradeep/dyndb-mutex/releases/download/v0.41/dyndbmutex-0.4.1.tar.gz',
    author='chiradeep',
    author_email='chiradeep@apache.org',
    install_requires=['boto3>=1.9.0'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing'
    ]
)
