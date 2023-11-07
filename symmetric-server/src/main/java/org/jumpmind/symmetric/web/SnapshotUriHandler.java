package org.jumpmind.symmetric.web;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.util.SnapshotUtil;

import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class SnapshotUriHandler extends AbstractUriHandler {
    private ISymmetricEngine engine;

    public SnapshotUriHandler(ISymmetricEngine engine, IInterceptor[] interceptors) {
        super("/snapshot/*", engine.getParameterService(), interceptors);
        this.engine = engine;
    }

    @Override
    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        if (req.getMethod().equals("POST")) {
            String tmpDir = System.getProperty("java.io.tmpdir");
            String secret = IOUtils.toString(req.getInputStream(), StandardCharsets.UTF_8);
            if (new File(tmpDir, secret).exists()) {
                try (ServletOutputStream resOutputStream = res.getOutputStream()) {
                    resOutputStream.print("Taking snapshot");
                    File snapshot = SnapshotUtil.createSnapshot(engine, (engineName, stepNumber, totalSteps) -> {
                        try {
                            resOutputStream.print(".");
                        } catch (IOException e) {
                        }
                    });
                    resOutputStream.println();
                    resOutputStream.println("Created snapshot file in " + snapshot.getCanonicalPath());
                }
            } else {
                ServletUtils.sendError(res, WebConstants.SC_FORBIDDEN);
            }
        } else {
            ServletUtils.sendError(res, WebConstants.SC_FORBIDDEN);
        }
    }
}
