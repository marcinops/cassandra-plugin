#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging

class OS_Command_Response(object):
    def __init__(self, stdout, stderr, exit_code):
        logger = logging.getLogger(__name__)
        self.__stdout = stdout.encode('utf-8').strip()
        self.__stderr = stderr.encode('utf-8').strip()
        self.__exit_code = exit_code
        logger.debug("Command stdout is {}".format(self.__stdout))
        logger.debug("Command stderr is {}".format(self.__stderr))
        logger.debug("Command exit code is {}".format(exit_code))


    @property
    def stdout(self):
        return self.__stdout

    @property
    def stderr(self):
        return self.__stderr

    @property
    def exit_code(self):
        return self.__exit_code
