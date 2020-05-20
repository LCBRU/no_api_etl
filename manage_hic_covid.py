#!/usr/bin/env python
from migrate.versioning.shell import main
from api.environment import HIC_COVID_CONNECTION_STRING

if __name__ == '__main__':
    main(url=HIC_COVID_CONNECTION_STRING, repository='hic_covid_repository', debug='False')
