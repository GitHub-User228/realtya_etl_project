#!/usr/local/bin/python3

import argparse
import warnings

warnings.filterwarnings("ignore")

from etl.utils import boolean
from etl.pipeline import run_etl_pipeline


parser = argparse.ArgumentParser()
parser.add_argument(
    "-p",
    "--parse",
    help="Whether to parse new data. Default: True",
    default=True,
    type=boolean,
)
parser.add_argument(
    "-t",
    "--transform",
    help="Whether to transform data. If --parse is True - newly parsed "
    + "data is transformed, else - the parsed data at the curren day. "
    + "Default: True",
    default=True,
    type=boolean,
)
parser.add_argument(
    "-ows",
    "--overwrite-source",
    help="Whether to overwrite recently obtained source data, if it was "
    + "obtained at the same day. Default: False",
    default=False,
    type=boolean,
)
parser.add_argument(
    "-owd",
    "--overwrite-destination",
    help="Whether to overwrite recently obtained destination data, "
    + "if it was obtained at the same day. Default: False",
    default=False,
    type=boolean,
)

args = parser.parse_args()

run_etl_pipeline(
    parse=args.parse,
    transform=args.transform,
    overwrite_source=args.overwrite_source,
    overwrite_destination=args.overwrite_destination,
)
