import logging
import sys
import j1nuclei.cli

if __name__ == '__main__':
    logging.getLogger('j1nuclei').setLevel(logging.DEBUG)
    sys.exit(j1nuclei.cli.main())
