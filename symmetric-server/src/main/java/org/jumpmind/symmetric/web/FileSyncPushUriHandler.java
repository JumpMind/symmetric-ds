/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;

public class FileSyncPushUriHandler extends AbstractUriHandler {

    private ISymmetricEngine engine;

    public FileSyncPushUriHandler(ISymmetricEngine engine, IInterceptor... interceptors) {
        super("/filesync/push/*", engine.getParameterService(), interceptors);
        this.engine = engine;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException, FileUploadException {
        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);

        if (StringUtils.isBlank(nodeId)) {
            ServletUtils.sendError(res, HttpServletResponse.SC_BAD_REQUEST,
                    "Node must be specified");
            return;
        } else if (!ServletFileUpload.isMultipartContent(req)) {
            ServletUtils.sendError(res, HttpServletResponse.SC_BAD_REQUEST,
                    "We only handle multipart requests");
            return;
        } else {
            log.debug("File sync push request received from {}", nodeId);
        }

        ServletFileUpload upload = new ServletFileUpload();

        // Parse the request
        FileItemIterator iter = upload.getItemIterator(req);
        while (iter.hasNext()) {
            FileItemStream item = iter.next();
            String name = item.getFieldName();
            if (!item.isFormField()) {
                log.info("File field " + name + " with file name " + item.getName()
                        + " detected.");                
                engine.getFileSyncService().loadFilesFromPush(nodeId, item.openStream(),
                        res.getOutputStream());

            }
        }

    }

}
