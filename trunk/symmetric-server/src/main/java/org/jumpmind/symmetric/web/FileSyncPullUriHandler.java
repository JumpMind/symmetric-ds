package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.Status;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class FileSyncPullUriHandler extends AbstractUriHandler {

    private ISymmetricEngine engine;

    public FileSyncPullUriHandler(ISymmetricEngine engine, IInterceptor... interceptors) {
        super("/filesync/pull/*", engine.getParameterService(), interceptors);
        this.engine = engine;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);

        if (StringUtils.isBlank(nodeId)) {
            ServletUtils.sendError(res, HttpServletResponse.SC_BAD_REQUEST,
                    "Node must be specified");
            return;
        } else {
            log.debug("File sync pull request received from {}", nodeId);
        }

        IOutgoingTransport outgoingTransport = createOutgoingTransport(res.getOutputStream(), 
                req.getHeader(WebConstants.HEADER_ACCEPT_CHARSET),
                engine.getConfigurationService().getSuspendIgnoreChannelLists(nodeId));
        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(engine.getNodeService().findIdentityNodeId(), nodeId,
                        ProcessType.FILE_SYNC_PULL_HANDLER));
        try {
            
            res.setContentType("application/zip");
            res.addHeader("Content-Disposition", "attachment; filename=\"file-sync.zip\"");
            
            engine.getFileSyncService().sendFiles(processInfo,
                    engine.getNodeService().findNode(nodeId), outgoingTransport);
            processInfo.setStatus(Status.DONE);
        } catch (RuntimeException ex) {
            processInfo.setStatus(Status.ERROR);
            throw ex;
        } finally {
            if (outgoingTransport != null) {
                outgoingTransport.close();
            }
        }

    }

}
