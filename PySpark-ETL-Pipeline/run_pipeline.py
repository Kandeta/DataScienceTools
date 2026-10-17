"""spark-submit entry point (spark-submit needs a file, not `-m`)."""

import sys

from spark_etl.pipeline import main

if __name__ == "__main__":
    sys.exit(main())
