import pathlib
from setuptools import setup, find_packages

README = (pathlib.Path(__file__).parent/'README.md').read_text()

setup(
    name='vohlc',
    version='0.1.0',
    description='Create Candlestick data based on volume instead of time',
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/paulschick/python-vohlc',
    author='Paul Schick',
    author_email='paul@paulschick.dev',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'ccxt',
        'pandas'
    ],
    include_package_data=True,
    zip_safe=True
)
