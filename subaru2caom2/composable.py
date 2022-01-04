# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

"""
Implements the default entry point functions for the workflow 
application.

'run' executes based on either provided lists of work, or files on disk.
'run_by_state' executes incrementally, usually based on time-boxed 
intervals.

CFHT precedent:
- run_state - uses time-boxed directory listing to store the data

SCLA:
- need to store the data, and for that case, need to be able to retrieve
  the data from a remote location
- that storing will operate in a time-boxed fashion, like CFHT
- general run combinations will be STORE+INGEST, and then after that just
  INGEST with a todo.txt file to fix mapping errors.
- want to be able to scale horizontally, so there will be end timestamps as
  well
"""

import logging
import sys
import traceback

from datetime import datetime

from caom2pipe import client_composable as clc
from caom2pipe import data_source_composable as dsc
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc
from caom2pipe import reader_composable as rdc
from caom2pipe import run_composable as rc
from subaru2caom2 import APPLICATION, SubaruName, transfer
from subaru2caom2 import fits2caom2_augmentation, preview_augmentation
from subaru2caom2 import cleanup_augmentation
from vos import Client


META_VISITORS = [
    fits2caom2_augmentation, preview_augmentation, cleanup_augmentation
]
DATA_VISITORS = []
SCLA_BOOKMARK = 'scla_timestamp'


def _run():
    """
    Uses a todo file to identify the work to be done.

    :return 0 if successful, -1 if there's any sort of failure. Return status
        is used by airflow for task instance management and reporting.
    """
    config = mc.Config()
    config.get_executors()
    clients = None
    source_transfer = None
    if mc.TaskType.STORE in config.task_types:
        vo_client = Client(vospace_certfile=config.proxy_fqn)
        clients = clc.ClientCollection(config)
        source_transfer = transfer.VoTransferCheck(
            vo_client, clients.data_client
        )
    name_builder = nbc.GuessingBuilder(SubaruName)
    return rc.run_by_todo(
        name_builder=name_builder,
        meta_visitors=META_VISITORS,
        data_visitors=DATA_VISITORS,
        store_transfer=source_transfer,
        clients=clients,
    )


def run():
    """Wraps _run in exception handling, with sys.exit calls."""
    try:
        result = _run()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)


def _run_remote():
    """
    Uses a VOSpace directory listing to identify the work to be done.

    :return 0 if successful, -1 if there's any sort of failure. Return status
        is used by airflow for task instance management and reporting.
    """
    config = mc.Config()
    config.get_executors()
    vo_client = Client(vospace_certfile=config.proxy_fqn)
    clients = clc.ClientCollection(config)
    source_transfer = transfer.VoTransferCheck(
        vo_client, clients.data_client
    )
    data_source = dsc.VaultDataSource(vo_client, config)
    name_builder = nbc.GuessingBuilder(SubaruName)
    reader = rdc.VaultReader(vo_client)
    return rc.run_by_todo(
        name_builder=name_builder,
        meta_visitors=META_VISITORS,
        data_visitors=DATA_VISITORS,
        source=data_source,
        store_transfer=source_transfer,
        clients=clients,
        metadata_reader=reader,
    )


def run_remote():
    """Wraps _run_remote in exception handling, with sys.exit calls."""
    try:
        result = _run_remote()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)


def _run_state():
    """Uses a state file with a timestamp to control which entries will be
    processed.
    """
    config = mc.Config()
    config.get_executors()
    builder = nbc.GuessingBuilder(SubaruName)
    source_client = Client(vospace_certfile=config.proxy_fqn)
    clients = clc.ClientCollection(config)
    data_source = dsc.VaultDataSource(source_client, config)
    source_transfer = transfer.VoTransferCheck(
        source_client, clients.data_client
    )
    state = mc.State(config.state_fqn)
    end_timestamp_s = state.bookmarks.get(SCLA_BOOKMARK).get(
        'end_timestamp', datetime.now()
    )
    end_timestamp_dt = mc.make_time_tz(end_timestamp_s)
    return rc.run_by_state(
        name_builder=builder,
        command_name=APPLICATION,
        bookmark_name=SCLA_BOOKMARK,
        meta_visitors=META_VISITORS,
        data_visitors=DATA_VISITORS,
        end_time=end_timestamp_dt,
        source=data_source,
        source_transfer=source_transfer,
        clients=clients,
    )


def run_state():
    """Wraps _run_state in exception handling."""
    try:
        _run_state()
        sys.exit(0)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)
