from setuptools import find_packages, setup

setup(
    name='minelink',
    version='0.0.1',
    description='minelink',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        'numpy'
    ],
)