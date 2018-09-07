#!/usr/bin/env python
from migrate.versioning.shell import main
from api.environment import ETL_CENTRAL_CONNECTION_STRING

if __name__ == '__main__':
    main(url=ETL_CENTRAL_CONNECTION_STRING, repository='repository', debug='False')
