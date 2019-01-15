from setuptools import setup, find_packages


setup(
    name="stairs-project",
    version="0.0.0.3",
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
        'tqdm==4.25.0',
        'redis==3.0.1',
        'blinker==1.4',
        'click==7.0',
        'ujson==1.35',
        'flask',
        'stepist'
    ],
    url='https://github.com/electronick1/stairs',
    download_url='https://github.com/electronick1/stairs/archive/0.0.3.tar.gz',
    classifiers=[],
    entry_points={'console_scripts': [
        'stairs-admin = stairs.services.management.commands:init_cli',
    ]},
)
