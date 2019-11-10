from setuptools import setup, find_packages


setup(
    name="stairs-project",
    version="0.1.6",
    author="Aleh Shydlouski",
    author_email="oleg.ivye@gmail.com",
    description="Framework for data processing using data pipelines",
    keywords=['data-pipeline',
              'data-science',
              'data-engineering',
              'distributed-computing'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'tqdm',
        'redis >= 3.0.0',
        'blinker',
        'click',
        'ujson >= 1.0',
        'ipython >= 6.0.0',
        'stepist >= 0.1.6'
    ],
    url='https://github.com/electronick1/stairs',
    download_url='https://github.com/electronick1/stairs/archive/0.1.6.tar.gz',
    classifiers=[],
    entry_points={'console_scripts': [
        'stairs-admin = stairs.services.management.commands:init_cli',
    ]},
)
