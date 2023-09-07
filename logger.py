import logging


def create_logger():
    """ Создает базовый логгер """

    logger = logging.getLogger('basic')
    logger.setLevel('DEBUG')
    # file_handler = logging.FileHandler('./../project/logs/gui.log', mode='w',  encoding='utf-8')
    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    stream_handler.setFormatter(formatter)
