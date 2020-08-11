#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import traceback
from dlpx.virtualization.platform.exceptions import UserError
import logging

logger = logging.getLogger(__name__)

# Decorator to add exception handling for the functions defined in modules of operations package




def exception_decorator(function):
    def wrapper_function(*args, **kwargs):
        try:
            result = function(*args, **kwargs)
            return result
        except UserError as e:
            logger.debug(traceback.format_exc())
            logger.debug(str(e))
            raise e   
        except Exception as w:
            logger.debug("Exception {}".format(type(w)))
            logger.debug("Args {}".format(w.args))
            logger.debug("Message {}".format(w.message))
            logger.debug(traceback.format_exc())
            raise UserError(message="Unknown exception in plugin. Please contact plugin author", output=w)
    return wrapper_function
