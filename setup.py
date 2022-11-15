from setuptools import setup, find_packages  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
import re
import ast

_version_re = re.compile(r'__version__\s+=\s+(.*)')
version = str(ast.literal_eval(
    _version_re.search(
        open('commondao/__init__.py').read()
    ).group(1)
))


setup(
        name='lessweb-commondao',
        version=version,
        description='mysql service and toolkit for lessweb',
        long_description='\nREADME: https://github.com/lessweb/lessweb-commondao\n\n'
                         'Cookbook: http://www.lessweb.cn',
        url='https://github.com/lessweb/lessweb-commondao',
        author='qorzj',
        author_email='goodhorsezxj@gmail.com',
        license='Apache 2',
        platforms=['any'],

        classifiers=[
            ],
        keywords='lessweb mysql',
        packages=['commondao', 'commondao.utils'],
        install_requires=['aiohttp', 'lesscli', 'lessweb', 'mysql-connector', 'aiomysql'],
        entry_points={
            'console_scripts': [
                'commondao = commondao.index:main',
                ],
            },
    )
