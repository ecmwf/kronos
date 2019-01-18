import logging

logger = logging.getLogger(__name__)

msg_format = '%(asctime)s; %(name)s; %(levelname)s; %(message)s'

# to stdout
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter(msg_format))
logger.addHandler(ch)
