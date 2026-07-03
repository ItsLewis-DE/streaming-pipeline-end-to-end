import sys,os

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json 
import logging 

import argparse

logger = logging.getLogger(__name__)
def get_rundate() ->str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rundate",required=False,help="Execution date (YYYY-MM-DD)")
    args, unknown = parser.parse_known_args()

    if args.rundate:
        logger.info(f"SPARK_APP:Using rundate from Airflow")
        return args.rundate
    
    try:
        with open("run_config.json","r") as f:
            data = json.load(f)
            logger.info("Using rundate from file")
            return data['rundate']
    except Exception as e:
        logger.info("Cannot read rundate from config")
        return "1900-01-01"