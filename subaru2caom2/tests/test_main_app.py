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

from mock import patch

from cadcdata import FileInfo
from subaru2caom2 import main_app, APPLICATION, COLLECTION, SubaruName
from subaru2caom2 import PRODUCER
from caom2pipe import astro_composable as ac
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc

import logging
import os
import sys
import traceback

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
PLUGIN = os.path.join(os.path.dirname(THIS_DIR), 'main_app.py')

LOOKUP = {
    'SCLA_189.232+62.201': [
        'SCLA_189.232+62.201.W-C-IC.cat',
        'SCLA_189.232+62.201.W-S-I.fits',
        'SCLA_189.232+62.201.W-C-IC.fits',
        'SCLA_189.232+62.201.W-S-I.weight.fits.fz',
        'SCLA_189.232+62.201.W-C-IC.weight.fits.fz',
        'SCLA_189.232+62.201.W-S-Z.cat',
        'SCLA_189.232+62.201.W-J-V.cat',
        'SCLA_189.232+62.201.W-S-Z.fits',
        'SCLA_189.232+62.201.W-J-V.fits',
        'SCLA_189.232+62.201.W-S-Z.weight.fits.fz',
        'SCLA_189.232+62.201.W-J-V.weight.fits.fz',
        'SCLA_189.232+62.201.W-S-I.cat',
    ],
    'SCLA.652.157': [
        'SCLA.652.157.W-J-VR.weight.fits.fz',
        'SCLA.652.157.W-J-VR.fits',
        'SCLA.652.157.W-C-RC.weight.fits.fz',
        'SCLA.652.157.W-C-RC.fits',
    ],
    'SCLA.396.170': [
        'SCLA.396.170.W-C-RC.weight.fits.fz',
        'SCLA.396.170.W-C-RC.fits',
    ],
    'SCLA.285.288': [
        'SCLA.285.288.W-S-Z+.weight.fits.fz',
        'SCLA.285.288.W-S-Z+.fits',
        'SCLA.285.288.W-S-R+.weight.fits.fz',
        'SCLA.285.288.W-S-R+.fits',
        'SCLA.285.288.W-S-I+.weight.fits.fz',
        'SCLA.285.288.W-S-I+.fits',
        'SCLA.285.288.W-S-G+.weight.fits.fz',
        'SCLA.285.288.W-S-G+.fits',
        'SCLA.285.288.W-J-V.weight.fits.fz',
        'SCLA.285.288.W-J-V.fits',
        'SCLA.285.288.N-B-L921.weight.fits.fz',
        'SCLA.285.288.N-B-L921.fits',
    ],
    'SCLA.134.129': [
        'SCLA.134.129.W-J-VR.weight.fits.fz',
        'SCLA.134.129.W-J-VR.fits',
    ],
    'SUPA0037434': [
        'SUPA0037434p.fits.fz',
    ],
    'SUPA0102090': [
        'SUPA0102090p.fits.fz',
    ],
    'SUPA0122144': [
        'SUPA0122144p.fits.fz',
    ],
    # 'SUPA0142581': [
    #     'SUPA0142581p.fits.fz',
    # ],
}


def pytest_generate_tests(metafunc):
    obs_id_list = [
        f'{TEST_DATA_DIR}/{ii}.expected.xml' for ii in LOOKUP.keys()
    ]
    metafunc.parametrize('test_name', obs_id_list)


@patch('caom2utils.data_util.get_local_headers_from_fits')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2utils.data_util.StorageClientWrapper')
def test_main_app(
    data_client_mock, access_mock, local_headers_mock, test_name
):
    access_mock.return_value = 'https://localhost'
    local_headers_mock.side_effect = ac.make_headers_from_file
    output_file = test_name.replace('expected', 'actual')

    if os.path.exists(output_file):
        os.unlink(output_file)

    tn = os.path.basename(test_name).replace('.expected.xml', '')
    local = _get_local(tn)
    lineage = _get_lineage(tn)

    data_client_mock.return_value.info.side_effect = _info

    sys.argv = (
        f'{APPLICATION} --no_validate --local {local} --observation '
        f'{COLLECTION} {tn} -o {output_file} --plugin {PLUGIN} '
        f'--module {PLUGIN} --lineage {lineage}'
    ).split()
    print(sys.argv)
    try:
        main_app.to_caom2()
    except Exception as e:
        logging.error(traceback.format_exc())

    compare_result = mc.compare_observations(output_file, test_name)
    if compare_result is not None:
        raise AssertionError(compare_result)
    # assert False  # cause I want to see logging messages


def _info(uri):
    return FileInfo(id=uri, file_type='application/fits')


def _get_local(entry):
    return ' '.join(f'{TEST_DATA_DIR}/{ii}.header' for ii in LOOKUP.get(entry))


def _get_lineage(entry):
    result = ''
    for ii in LOOKUP.get(entry):
        storage_name = SubaruName(file_name=ii)
        result = (
            f'{result} {storage_name.product_id}/{PRODUCER}:{COLLECTION}/'
            f'{storage_name.file_name}'
        )
    return result
