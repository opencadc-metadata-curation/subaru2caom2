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
This module implements the ObsBlueprint mapping, as well as the workflow 
entry point that executes the workflow.
"""

import importlib
import logging
import os
import sys
import traceback

from caom2 import Observation, DataProductType, CalibrationLevel
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
from caom2pipe import manage_composable as mc


__all__ = [
    'APPLICATION',
    'ARCHIVE',
    'COLLECTION',
    'subaru_main_app',
    'SubaruName',
    'to_caom2',
    'update',
]


APPLICATION = 'subaru2caom2'
COLLECTION = 'SUBARU'
ARCHIVE = 'SUBARUPROC'


class SubaruName(mc.StorageName):
    """Naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support uncompressed files in storage
    """

    SUBARU_NAME_PATTERN = '*'

    def __init__(self, file_name=None, artifact_uri=None, entry=None):
        if file_name is not None:
            self._file_name = file_name
            self.obs_id = self._get_obs_id()
            super(SubaruName, self).__init__(
                self.obs_id,
                collection=COLLECTION,
                collection_pattern=SubaruName.SUBARU_NAME_PATTERN,
                scheme='cadc',
                archive=ARCHIVE,
                fname_on_disk=file_name,
                entry=entry,
                compression='',
            )
        if artifact_uri is not None:
            scheme, path, file_name = mc.decompose_uri(artifact_uri)
            self._file_name = file_name
            self.obs_id = self._get_obs_id()
            super(SubaruName, self).__init__(
                self.obs_id,
                collection=COLLECTION,
                collection_pattern=SubaruName.SUBARU_NAME_PATTERN,
                scheme=scheme,
                archive=path,
                fname_on_disk=file_name,
                entry=entry,
                compression='',
            )
        self._product_id = self._get_product_id()
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.error(self)

    def __str__(self):
        return (
            f'\n'
            f'      obs_id: {self.obs_id}\n'
            f'source_names: {self.source_names}\n'
            f'   file_name: {self.file_name}\n'
            f'     lineage: {self.lineage}\n'
        )

    def _get_obs_id(self):
        bits = self._file_name.split('.')
        return '.'.join(ii for ii in bits[:3])

    def _get_product_id(self):
        return SubaruName.remove_extensions(self._file_name)

    @property
    def file_name(self):
        return self._file_name

    @property
    def product_id(self):
        return self._product_id

    def is_valid(self):
        return True

    @staticmethod
    def remove_extensions(entry):
        return mc.StorageName.remove_extensions(
            entry
        ).replace('.fz', '').replace('.weight', '')


def accumulate_bp(bp, uri):
    """Configure the telescope-specific ObsBlueprint at the CAOM model 
    Observation level."""
    logging.debug('Begin accumulate_bp.')
    bp.configure_position_axes((1, 2))
    bp.configure_time_axis(3)
    bp.configure_energy_axis(4)
    bp.configure_polarization_axis(5)
    bp.configure_observable_axis(6)

    meta_producer = mc.get_version(APPLICATION)
    bp.set('Observation.metaProducer', meta_producer)
    bp.set('Plane.metaProducer', meta_producer)
    bp.set('Artifact.metaProducer', meta_producer)
    bp.set('Chunk.metaProducer', meta_producer)

    bp.set('Plane.calibrationLevel', CalibrationLevel.PRODUCT)
    bp.set('Plane.dataProductType', DataProductType.IMAGE)
    logging.debug('Done accumulate_bp.')


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes (an n:n
    relationship between TDM attributes and CAOM attributes). Must have this
    signature for import_module loading and execution.

    :param observation A CAOM Observation model instance.
    :param **kwargs Everything else."""
    logging.debug('Begin update.')
    mc.check_param(observation, Observation)

    headers = kwargs.get('headers')
    fqn = kwargs.get('fqn')
    uri = kwargs.get('uri')
    subaru_name = None
    if uri is not None:
        subaru_name = SubaruName(artifact_uri=uri)
    if fqn is not None:
        subaru_name = SubaruName(file_name=os.path.basename(fqn))
    if subaru_name is None:
        raise mc.CadcException(
            f'Need one of fqn or uri defined for {observation.observation_id}'
        )

    logging.debug('Done update.')
    return observation


def _build_blueprints(uris):
    """This application relies on the caom2utils fits2caom2 ObsBlueprint
    definition for mapping FITS file values to CAOM model element
    attributes. This method builds the DRAO-ST blueprint for a single
    artifact.

    The blueprint handles the mapping of values with cardinality of 1:1
    between the blueprint entries and the model attributes.

    :param uris The artifact URIs for the files to be processed."""
    module = importlib.import_module(__name__)
    blueprints = {}
    for uri in uris:
        blueprint = ObsBlueprint(module=module)
        if not mc.StorageName.is_preview(uri):
            accumulate_bp(blueprint, uri)
        blueprints[uri] = blueprint
    return blueprints


def _get_uris(args):
    result = []
    if args.local:
        for ii in args.local:
            file_id = mc.StorageName.remove_extensions(os.path.basename(ii))
            file_name = f'{file_id}.fits'
            result.append(SubaruName(file_name=file_name).file_uri)
    elif args.lineage:
        for ii in args.lineage:
            result.append(ii.split('/', 1)[1])
    else:
        raise mc.CadcException(
            f'Could not define uri from these args {args}')
    return result


def to_caom2():
    """This function is called by pipeline execution. It must have this name.
    """
    args = get_gen_proc_arg_parser().parse_args()
    uris = _get_uris(args)
    blueprints = _build_blueprints(uris)
    result = gen_proc(args, blueprints)
    logging.debug(f'Done {APPLICATION} processing.')
    return result
           

def subaru_main_app():
    args = get_gen_proc_arg_parser().parse_args()
    try:
        result = to_caom2()
        sys.exit(result)
    except Exception as e:
        logging.error(f'Failed {APPLICATION} execution for {args}.')
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)
