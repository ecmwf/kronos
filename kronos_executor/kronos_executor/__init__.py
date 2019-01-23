
# -------- setup a logger for the kronos_executor --------
import logging

log_msg_format = '%(asctime)s; %(name)s; %(levelname)s; %(message)s'

# ============ ROOT LOGGER =============
logger = logging.getLogger()

# NOTE: set root to logging.DEBUG if needed
logger.setLevel(logging.INFO)
# ======================================

# add stdout handler if not there already
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(logging.Formatter(log_msg_format))
if all(type(s) != logging.StreamHandler for s in logger.handlers):
    logger.addHandler(sh)

