/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.vaadin.ui.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.vaadin.server.DownloadStream;
import com.vaadin.server.StreamResource;

public class ExportFileDownloader extends StreamResource {
    private static final long serialVersionUID = 1L;
    protected String contentType;
    protected String fileName;

    public ExportFileDownloader(final String fileName, final String contentType, final File file) throws FileNotFoundException {
        super(new FileStreamSource(file), fileName);
        this.contentType = contentType;
        this.fileName = fileName;
    }

    @Override
    public DownloadStream getStream() {
        DownloadStream download = new DownloadStream(super.getStreamSource().getStream(), contentType, fileName);
        download.setCacheTime(2000);
        return download;
    }

    public static class FileStreamSource implements StreamResource.StreamSource {
        private static final long serialVersionUID = 1L;
        private FileInputStream stream;

        public FileStreamSource(File downloadFile) throws FileNotFoundException {
            stream = new FileInputStream(downloadFile);
        }

        @Override
        public InputStream getStream() {
            return stream;
        }
    }
}