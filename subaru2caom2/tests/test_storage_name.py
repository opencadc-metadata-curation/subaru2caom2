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

from caom2pipe import name_builder_composable as nbc
from subaru2caom2 import SubaruName, COLLECTION


def test_is_valid():
    assert SubaruName('anything').is_valid()


def test_storage_name():
    test_obs_id = 'SCLA_189.232+62.201'
    test_f_id = f'{test_obs_id}.W-C-IC'
    test_f_name = f'{test_f_id}.fits'
    test_subject = SubaruName(file_name=test_f_name)
    assert test_subject.obs_id == test_obs_id, 'wrong obs id'
    assert test_subject.product_id == test_f_id, 'wrong product id'

    test_subject = SubaruName(
        uri=f'cadc:{COLLECTION}/SCLA_189.232+62.201.W-J-V.cat'
    )
    assert test_subject.obs_id == 'SCLA_189.232+62.201'
    assert test_subject.product_id == 'SCLA_189.232+62.201.W-J-V'
    assert test_subject.is_legacy

    test_subject = SubaruName(
        file_name='SUPA0037434p.fits.fz', entry='SUPA0037434p.fits.fz'
    )
    assert test_subject.obs_id == 'SUPA0037434'
    assert test_subject.product_id == 'SUPA0037434p'
    assert not test_subject.is_legacy
    assert test_subject.file_uri == f'cadc:{COLLECTION}/SUPA0037434p.fits.fz'
    assert test_subject.prev == 'SUPA0037434.gif'
    assert test_subject.prev_uri == f'cadc:{COLLECTION}/SUPA0037434.gif'
    assert test_subject.thumb_uri == f'cadc:{COLLECTION}/SUPA0037434_th.gif'
    assert (
        test_subject.source_names[0] == 'SUPA0037434p.fits.fz'
    ), 'wrong source name'
    assert (
        test_subject.destination_uris[0] ==
        f'cadc:{COLLECTION}/SUPA0037434p.fits.fz'
    ), 'wrong destination uri'


def test_builder():
    test_uri = f'cadc:{COLLECTION}/SUPA0037434p.fits.fz'
    name_builder = nbc.GuessingBuilder(SubaruName)
    for entry in [
        'vos:goliaths/subaru_test/SUPA0037434p.fits.fz',  # vos uri
        '/tmp/SUPA0037434p.fits.fz',  # use_local_files: True
        'SUPA0037434p.fits.fz',       # todo.txt
        'cadc:SUBARUCADC/SUPA0037434p.fits.fz',  # storage uri
    ]:
        test_subject = name_builder.build(entry)
        assert test_subject.destination_uris[0] == test_uri, 'destination'
        assert test_subject.source_names[0] == entry, 'source'


def test_case():
    for uri in [
        'cadc:SUBARUCADC/SUPA0017978p.fits.fz',
        'cadc:SUBARUCADC/SUPA0017978p.weight.fits.fz',
    ]:
        test_subject = SubaruName(uri=uri)
        assert test_subject.obs_id == 'SUPA0017978', f'wrong obs id {uri}'
        assert (
            test_subject.product_id == 'SUPA0017978p'
        ), f'wrong product id {uri}'
