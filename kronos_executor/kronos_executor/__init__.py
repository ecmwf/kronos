
# -------- setup a logger for the kronos_executor --------
import logging

log_msg_format = '%(asctime)s; %(name)s; %(levelname)s; %(message)s'

# ============ ROOT LOGGER =============
logger = logging.getLogger()

# NOTE: set root to logging.DEBUG if needed
logger.setLevel(logging.INFO)
# ======================================

# root stdout handler
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(logging.Formatter(log_msg_format))
logger.addHandler(sh)

