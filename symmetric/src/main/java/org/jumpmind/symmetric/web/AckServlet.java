package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class AckServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        Map parameters = req.getParameterMap();
        List<BatchInfo> batches = new ArrayList<BatchInfo>();

        for (Object batch : parameters.keySet()) {
            String batchId = batch.toString();
            if (batchId.startsWith(WebConstants.ACK_BATCH_NAME)) {
                Object value = parameters.get(batch);
                String status = "";
                if (value instanceof String) {
                    status = (String) value;
                } else if (value instanceof String[]) {
                    String[] array = (String[]) value;
                    if (array.length > 0) {
                        status = array[0];
                    }
                }

                if (status.trim().equalsIgnoreCase(WebConstants.ACK_BATCH_OK)) {
                    batches.add(new BatchInfo(getBatchIdFrom(batchId)));
                } else {
                    try {
                        int lineNumber = Integer.parseInt(status);
                        batches.add(new BatchInfo(getBatchIdFrom(batchId),
                                lineNumber));
                    } catch (NumberFormatException ex) {
                        batches.add(new BatchInfo(getBatchIdFrom(batchId),
                                BatchInfo.UNDEFINED_ERROR_LINE_NUMBER));
                    }
                }
            }
        }

        ApplicationContext ctx = WebApplicationContextUtils
                .getWebApplicationContext(getServletContext());
        IAcknowledgeService service = (IAcknowledgeService) ctx
                .getBean(Constants.ACKNOWLEDGE_SERVICE);
        service.ack(batches);
    }

    private String getBatchIdFrom(String webParameter) {
        int index = WebConstants.ACK_BATCH_NAME.length();
        if (index >= 0) {
            return webParameter.substring(index);
        } else {
            throw new IllegalStateException("Invalid batch parameter "
                    + webParameter);
        }
    }

}
