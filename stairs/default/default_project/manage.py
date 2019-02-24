#!/usr/bin/env python
from stairs.services.management import init_cli
from stairs.core.project import StairsProject

if __name__ == "__main__":
    stairs_project = StairsProject(config_file="config.py")
    init_cli()
