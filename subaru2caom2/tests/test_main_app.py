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

from os.path import dirname, exists, join, realpath
from mock import patch

from cadcdata import FileInfo
from caom2.diff import get_differences
from caom2utils.caomvalidator import validate
from subaru2caom2 import COLLECTION, SubaruName
from subaru2caom2 import PRODUCER
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc
from caom2pipe import reader_composable as rdc
from subaru2caom2 import fits2caom2_augmentation

import logging

THIS_DIR = dirname(realpath(__file__))
TEST_DATA_DIR = join(THIS_DIR, 'data')
PLUGIN = join(dirname(THIS_DIR), 'main_app.py')

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
    'SUPA0017978': ['SUPA0017978p.weight.fits.fz', 'SUPA0017978p.fits.fz'],
    'SCLA.258.182': ['SCLA.258.182.W-J-B.fits'],
}


def pytest_generate_tests(metafunc):
    metafunc.parametrize('test_name', LOOKUP.keys())


@patch('caom2utils.data_util.get_local_headers_from_fits')
def test_visitor(local_headers_mock, test_name):
    local_headers_mock.side_effect = ac.make_headers_from_file

    observation = None
    for f_name in LOOKUP[test_name]:
        fqn = f'{TEST_DATA_DIR}/{f_name}.header'
        storage_name = SubaruName(file_name=f_name, entry=fqn)
        file_info = FileInfo(
            id=storage_name.file_uri, file_type='application/fits'
        )
        headers = ac.make_headers_from_file(fqn)
        metadata_reader = rdc.FileMetadataReader()
        metadata_reader._headers = {storage_name.file_uri: headers}
        metadata_reader._file_info = {storage_name.file_uri: file_info}
        kwargs = {
            'storage_name': storage_name,
            'metadata_reader': metadata_reader,
        }
        observation = fits2caom2_augmentation.visit(observation, **kwargs)

    validate(observation)
    expected_fqn = (
        f'{TEST_DATA_DIR}/{test_name}.expected.xml'
    )
    actual_fqn = expected_fqn.replace('expected', 'actual')
    if not exists(expected_fqn):
        mc.write_obs_to_file(observation, actual_fqn)
    expected = mc.read_obs_from_file(expected_fqn)
    compare_result = get_differences(expected, observation)
    if compare_result is not None:
        mc.write_obs_to_file(observation, actual_fqn)
        compare_text = '\n'.join([r for r in compare_result])
        msg = (
            f'Differences found in observation {expected.observation_id}\n'
            f'{compare_text}'
        )
        raise AssertionError(msg)
