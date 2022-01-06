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

import logging
import os
import test_main_app

from collections import deque
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
from mock import ANY, Mock, patch

from caom2pipe import data_source_composable as dsc
from caom2pipe import manage_composable as mc
from subaru2caom2 import composable, SubaruName, SCLA_BOOKMARK


@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run(run_mock, clients_mock):
    test_obs_id = 'SUPA0014258'
    test_f_name = 'SUPA0014258p.weight.fits.fz'
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        # execution
        composable._run()
        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, SubaruName), type(test_storage)
        assert test_storage.obs_id == test_obs_id, 'wrong obs id'
        assert test_storage.file_name == test_f_name, 'wrong file name'
    finally:
        os.getcwd = getcwd_orig
        _cleanup()


@patch('subaru2caom2.composable.Client', autospec=True)
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one', autospec=True)
def test_run_remote(run_mock, clients_mock, vo_client_mock):
    test_obs_id = 'SUPA0014258'
    test_f_name = 'SUPA0014258p.weight.fits.fz'
    vo_client_mock.return_value.listdir.return_value = [test_f_name]

    node1 = type('', (), {})()
    node1.props = {
        'date': '2020-09-15 19:55:03.067000+00:00',
        'size': 14,
    }
    node1.uri = f'vos://cadc.nrc.ca!vault/goliaths/moc/{test_f_name}'
    node1.type = 'vos:DataNode'
    node1.node_list = [node1]
    vo_client_mock.return_value.get_node.return_value = node1

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        # execution
        composable._run_remote()
        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[1]
        assert isinstance(test_storage, SubaruName), type(test_storage)
        assert test_storage.obs_id == test_obs_id, 'wrong obs id'
        assert test_storage.file_name == test_f_name, 'wrong file name'
    finally:
        os.getcwd = getcwd_orig
        _cleanup()


@patch('subaru2caom2.composable.Client')
@patch('subaru2caom2.preview_augmentation.visit')
@patch('subaru2caom2.transfer.VoTransferCheck')
@patch('caom2pipe.data_source_composable.VaultDataSource.get_time_box_work')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run_store_ingest(
        data_client_mock,
        repo_client_mock,
        data_source_mock,
        transferrer_mock,
        visit_mock,
        vo_mock,
):
    an_hour_ago = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    m_time = an_hour_ago + timedelta(minutes=10)
    temp_deque = deque()
    dir_entry = dsc.StateRunnerMeta(
        'vos:goliaths/test/SUPA0017978p.fits.fz', m_time.timestamp()
    )
    temp_deque.append(dir_entry)
    data_source_mock.side_effect = [temp_deque, deque()]
    repo_client_mock.return_value.read.return_value = None

    def _visit_mock(
        obs,
        working_directory=None,
        cadc_client=None,
        caom_repo_client=None,
        stream=None,
        storage_name=None,
        metadata_reader=None,
        observable=None,
    ):
        return obs
    visit_mock.side_effect = _visit_mock

    cwd = os.getcwd()
    with TemporaryDirectory() as tmp_dir_name:
        os.chdir(tmp_dir_name)
        test_config = mc.Config()
        test_config.working_directory = tmp_dir_name
        test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
        test_config.data_sources = ['vos:goliaths/test']
        test_config.data_source_extensions = ['.fits.fz']
        test_config.logging_level = 'INFO'
        test_config.proxy_file_name = 'cadcproxy.pem'
        test_config.proxy_fqn = f'{tmp_dir_name}/cadcproxy.pem'
        test_config.state_file_name = 'state.yml'
        test_config.state_fqn = f'{tmp_dir_name}/state.yml'
        test_config.interval = 100
        test_config.features.supports_latest_client = True
        mc.Config.write_to_file(test_config)
        with open(test_config.proxy_fqn, 'w') as f:
            f.write('test content')
        with open(test_config.state_fqn, 'w') as f:
            f.write(
                f'bookmarks:\n  {SCLA_BOOKMARK}:\n'
                f'    last_record: {an_hour_ago}\n'
            )
        getcwd_orig = os.getcwd
        os.getcwd = Mock(return_value=tmp_dir_name)
        try:
            test_result = composable._run_state()
            assert test_result is not None, 'expect result'
            assert test_result == 0, 'expect success'
            assert transferrer_mock.return_value.get.called, 'get not called'
            transferrer_mock.return_value.get.assert_called_with(
                'vos:goliaths/test/SUPA0017978p.fits.fz',
                f'{tmp_dir_name}/SUPA0017978/SUPA0017978p.fits.fz',
            ), 'wrong args'
            assert repo_client_mock.return_value.read.called, 'read called'
            # make sure data is not being written to CADC storage :)
            assert (
                data_client_mock.return_value.put.called
            ), 'put should be called'
            assert (
                    data_client_mock.return_value.put.call_count == 1
            ), 'wrong number of puts'
            data_client_mock.return_value.put.assert_called_with(
                f'{tmp_dir_name}/SUPA0017978',
                'cadc:SUBARUCADC/SUPA0017978p.fits.fz',
                None,
            )
            assert vo_mock.return_value.copy.called, 'copy'
            vo_mock.return_value.copy.assert_called_with(
                'vos:goliaths/test/SUPA0017978p.fits.fz', ANY, head=True
            ), 'wrong copy parameters'
            assert vo_mock.return_value.get_node.called, 'get_node'
            vo_mock.return_value.get_node.assert_called_with(
                'vos:goliaths/test/SUPA0017978p.fits.fz',
                limit=None,
                force=False,
            ), 'wrong get_node parameters'
        finally:
            os.getcwd = getcwd_orig
            os.chdir(cwd)


def _cleanup():
    # clean up the files created as a by-product of a run
    for f_name in [
        'data_report.txt',
        'failure_log.txt',
        'rejected.yml',
        'retries.txt',
        'success_log.txt',
    ]:
        fqn = os.path.join(test_main_app.TEST_DATA_DIR, f_name)
        if os.path.exists(fqn):
            os.unlink(fqn)
