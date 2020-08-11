#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

from cassandra.medusa import Medusa

class Backup_factory(object):

    def __new__(cls, node_obj, backup_type="medusa"):
        if backup_type == "medusa":
            return Medusa(node_obj)
        else:
            raise "Backup class {} not implemeneted".format(backup_type)



