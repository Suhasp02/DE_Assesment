from setuptools import setup, find_packages

setup(
    name='DE_Assessment',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'SQLAlchemy==1.4.31',
        'pandas==1.3.3',
        'PyYAML==6.0',
        'psycopg2-binary==2.9.9',
    ],
    tests_require=[
        'unittest',
        'unittest-mock',
    ],
    entry_points={
        'console_scripts': [
            'DE_Assessment=fund_analysis:main'
        ]
    },
    author='Suhas P',
    author_email='suhas.p.murthy@gmail.com',
    description='A script for processing data files and loading into a database',
    keywords='data processing, database, SQLAlchemy, pandas, YAML',
)
