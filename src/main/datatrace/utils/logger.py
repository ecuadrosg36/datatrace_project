import logging

default_log_level = logging.INFO
logging.basicConfig(format="%(asctime)s - %(module)s - %(levelname)s: %(message)s", level=default_log_level)

log = logging.getLogger(name="COE_data_trace")