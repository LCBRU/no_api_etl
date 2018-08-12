#!/usr/bin/env python
from migrate.versioning.shell import main
from api.environment import CONNECTION_STRING

if __name__ == '__main__':
    main(url=CONNECTION_STRING, repository='repository', debug='False')
