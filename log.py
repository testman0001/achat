#!/usr/bin/env python3
# -*- coding:UTF-8 -*-
import logging
import sys

class Logger(object):
    """
    日志模块
    """
    _level_dict= {
    'CRITICAL' : logging.CRITICAL,
    'ERROR' : logging.ERROR,
    'WARN' : logging.WARNING,
    'WARNING' : logging.WARNING,
    'INFO' : logging.INFO,
    'DEBUG' : logging.DEBUG,
    }

    def __init__(self, log_path="./achat_cli.log", log_level="DEBUG"):
        logger = logging.getLogger("achat")

        if log_level not in self._level_dict.keys():
            return None
        level = self._level_dict[log_level]
        logger.setLevel(level)

        log_format = logging.Formatter(fmt="%(asctime)s %(levelname)s %(message)s",
            datefmt='%Y-%m-%d  %H:%M:%S' )    

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_format)
        logger.addHandler(console_handler)

        self._logger = logger

    def log_debug(self, message):
        self._logger.debug('{}:{} {}'.format(
            sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno, message))

    def log_info(self, message):
        self._logger.info('{}:{} {}'.format(
            sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno, message))

    def log_warning(self, message):
        self._logger.warning('{}:{} {}'.format(
            sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno, message))

    def log_error(self, message):
        self._logger.error('{}:{} {}'.format(
            sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno, message))

    def log_critical(self, message):
        self._logger.critical('{}:{} {}'.format(
            sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno, message))

    def log_test(self):
        self.log_debug("debug")
        self.log_info("info")
        self.log_warning("warning")
        self.log_error("error")
        self.log_critical("critical")

if __name__=='__main__':
    logger = Logger()
    logger.log_test()