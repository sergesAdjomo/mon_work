import logging
import sys
from expose.expose import expose_data


if __name__ == "__main__":

    logging.info("Start Schema builder")
    # Zone brute -> Zone fine
    flux = sys.argv[4][len("--flux=") :]
    expose_data(flux=flux)
    print("End of processing")