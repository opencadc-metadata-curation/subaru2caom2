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
#  : 4 $
#
# ***********************************************************************
#

from PIL import Image

from os.path import exists, join

from caom2 import ReleaseType, ProductType
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc
from vos import Client


class SubaruPreviewVisitor(mc.PreviewVisitor):

    def __init__(self, **kwargs):
        super().__init__(
            archive='SUBARUCADC',
            release_type=ReleaseType.META,
            mime_type='image/gif',
            **kwargs,
        )
        self._preview_fqn = join(self._working_dir, self._storage_name.prev)
        self._thumb_fqn = join(self._working_dir, self._storage_name.thumb)
        self._vo_client = None
        if self._cadc_client is not None:
            self._vo_client = Client(
                vospace_certfile='/usr/src/app/cadcproxy.pem'
            )

    def _gen_thumbnail(self):
        self._logger.debug(
            f'Generating thumbnail for file {self._science_fqn}.'
        )
        image = Image.open(self._preview_fqn)
        image.thumbnail((256, 256))
        image.save(self._thumb_fqn)
        count = 1
        return count

    def generate_plots(self, obs_id):
        count = 0
        if not self._storage_name.file_uri.endswith('p.fits.fz'):
            self._logger.info(
                f'Skip preview generation for {self._storage_name.file_uri}.'
            )
            return 0
        preview_vo_fqn = f'vos:sgwyn/suprime/preview/{self._storage_name.prev}'
        if self._vo_client is not None:
            vos_meta = clc.vault_info(self._vo_client, preview_vo_fqn)
            if vos_meta is not None and vos_meta.size > 0:
                self._vo_client.copy(
                    preview_vo_fqn, self._preview_fqn, send_md5=True
                )
            else:
                self._logger.warning(
                    f'Not retrieving preview {preview_vo_fqn} because '
                    f'metadata is {vos_meta}.'
                )
        if exists(self._preview_fqn):
            self.add_preview(
                self._storage_name.prev_uri,
                self._storage_name.prev,
                ProductType.PREVIEW,
                ReleaseType.DATA,
            )
            self.add_to_delete(self._preview_fqn)
            count = 1
            count += self._gen_thumbnail()
            if count == 2:
                self.add_preview(
                    self._storage_name.thumb_uri,
                    self._storage_name.thumb,
                    ProductType.THUMBNAIL,
                    ReleaseType.META,
                )
                self.add_to_delete(self._thumb_fqn)
        return count


def visit(observation, **kwargs):
    return SubaruPreviewVisitor(**kwargs).visit(observation)
