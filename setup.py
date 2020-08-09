import io
import os
import re

from setuptools import setup


def get_version():
    regex = r"__version__\s=\s\'(?P<version>[\d\.]+?)\'"

    path = ('aioamqp_consumer', '__init__.py')

    return re.search(regex, read(*path)).group('version')


def read(*parts):
    filename = os.path.join(os.path.abspath(os.path.dirname(__file__)), *parts)

    with io.open(filename, encoding='utf-8', mode='rt') as fp:
        return fp.read()


setup(
    name='aioamqp_consumer',
    version=get_version(),
    author='OCEAN S.A.',
    author_email='hellysmile@gmail.com',
    url='https://github.com/aio-libs/aioamqp_consumer',
    description='consumer/producer/rpc library built over aioamqp',
    long_description=read('README.rst'),
    python_requires='>=3.6.0',
    install_requires=[
        'aioamqp>=0.14.0',
        'aiorun>=2020.6.1',
        'async_timeout>=3.0.0',
    ],
    extras_require={
        'debug': ['aioreloader'],
    },
    packages=['aioamqp_consumer'],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords=['amqp', 'asyncio', 'aioamqp', 'consumer', 'producer', 'rpc'],
)
