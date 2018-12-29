from setuptools import setup, find_packages


setup(
    name="stairs",
    version="0.0.0.1",
    author="Aleh Shydlouski",
    author_email="oleg.ivye@gmail.com",
    description="Framework for data processing",
    keywords=['data', 'ai', 'distribute'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'tqdm',
        'ujson',
        'redis',
    ],
    classifiers=[],
    entry_points={'console_scripts': [
        'stairs-admin = stairs.services.management.commands:init_cli',
    ]},
)
