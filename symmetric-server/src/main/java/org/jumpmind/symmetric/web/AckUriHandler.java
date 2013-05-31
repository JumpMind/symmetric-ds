package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.AbstractTransportManager;

public class AckUriHandler extends AbstractUriHandler {

    private static final Comparator<BatchAck> BATCH_ID_COMPARATOR = new Comparator<BatchAck>() {
        public int compare(BatchAck batchInfo1, BatchAck batchInfo2) {
            Long batchId1 = batchInfo1.getBatchId();
            Long batchId2 = batchInfo1.getBatchId();
            return batchId1.compareTo(batchId2);
        }
    };

    private IAcknowledgeService acknowledgeService;
    
    public AckUriHandler(
            IParameterService parameterService, IAcknowledgeService acknowledgeService, IInterceptor...interceptors) {
        super("/ack/*", parameterService, interceptors);
        this.acknowledgeService = acknowledgeService;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        if (log.isDebugEnabled()) {
            log.debug("Reading ack: {}", req.getParameterMap());
        }
        @SuppressWarnings("unchecked")
        List<BatchAck> batches = AbstractTransportManager.readAcknowledgement(req
                .getParameterMap());
        Collections.sort(batches, BATCH_ID_COMPARATOR);
        ack(batches);
    }

    protected void ack(List<BatchAck> batches) throws IOException {
        for (BatchAck batchInfo : batches) {
            acknowledgeService.ack(batchInfo);
        }
    }

}
