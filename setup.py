from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()


def get_requirements():
    with open('requirements.txt') as f:
        return f.read().split()


setup(
    name='stitches',
    version='0.1.0',
    packages=find_packages(),
    url='https://github.com/JGCRI/stitches',
    license='BSD 2-Clause',
    author='',
    author_email='',
    description='Amalgamate existing climate data to create monthly climate variable fields',
    long_description=readme(),
    python_requires='>=3.9.0',
    include_package_data=True,
    install_requires=get_requirements()
)
