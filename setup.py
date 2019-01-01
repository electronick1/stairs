from setuptools import setup, find_packages


setup(
    name="stairs-project",
    version="0.0.0.1",
    author="Aleh Shydlouski",
    author_email="oleg.ivye@gmail.com",
    description="Framework for data processing",
    keywords=['data', 'data-processing'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'tqdm',
        'ujson',
        'redis',
        'stepist',
    ],
    classifiers=[],
    entry_points={'console_scripts': [
        'stairs-admin = stairs.services.management.commands:init_cli',
    ]},
)
