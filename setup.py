from setuptools import setup

setup(
    name='vohlc',
    version='0.0.1',
    description='Create Candlestick data based on volume instead of time',
    url='https://github.com/paulschick/python-vohlc',
    author='Paul Schick',
    author_email='paul@paulschick.dev',
    license='MIT',
    packages=['vohlc'],
    install_requires=[
        'ccxt',
        'pandas'
    ],
    zip_safe=True
)
