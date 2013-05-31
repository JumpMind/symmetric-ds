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
                log.debug("Processing upload file field " + name + " with file name " + item.getName()
                        + " detected.");                
                engine.getFileSyncService().loadFilesFromPush(nodeId, item.openStream(),
                        res.getOutputStream());

            }
        }

    }

}
