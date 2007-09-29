package org.jumpmind.symmetric.transport.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.model.IncomingBatchHistory.Status;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.AbstractTransportManager;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.springframework.beans.factory.BeanFactory;

/**
 * Coordinates interaction between two symmetric engines in the same JVM.
 */
public class InternalTransportManager extends AbstractTransportManager
        implements ITransportManager {

    static final Log logger = LogFactory.getLog(InternalTransportManager.class);

    @SuppressWarnings("unused")
    private IRuntimeConfig runtimeConfiguration;

    public InternalTransportManager(IRuntimeConfig config) {
        this.runtimeConfiguration = config;
    }

    public IIncomingTransport getPullTransport(final Node remote, final Node local)
            throws IOException {
        final PipedOutputStream respOs = new PipedOutputStream();
        final PipedInputStream respIs = new PipedInputStream(respOs);

        runAtClient(remote.getSyncURL(), null, respOs, new IClientRunnable() {
            public void run(BeanFactory factory, InputStream is, OutputStream os)
                    throws Exception {
                IDataExtractorService extractor = (IDataExtractorService) factory
                        .getBean(Constants.DATAEXTRACTOR_SERVICE);
                IOutgoingTransport transport = new InternalOutgoingTransport(
                        respOs);
                extractor.extract(local, transport);
                transport.close();
            }
        });
        return new InternalIncomingTransport(respIs);
    }

    public IOutgoingWithResponseTransport getPushTransport(final Node remote, final Node local)
            throws IOException {

        final PipedOutputStream pushOs = new PipedOutputStream();
        final PipedInputStream pushIs = new PipedInputStream(pushOs);

        final PipedOutputStream respOs = new PipedOutputStream();
        final PipedInputStream respIs = new PipedInputStream(respOs);

        runAtClient(remote.getSyncURL(), pushIs, respOs, new IClientRunnable() {
            public void run(BeanFactory factory, InputStream is, OutputStream os)
                    throws Exception {
                // This should be basically what the push servlet does ...
                IDataLoaderService service = (IDataLoaderService) factory
                        .getBean(Constants.DATALOADER_SERVICE);
                service.loadData(pushIs, respOs);
            }
        });
        return new InternalOutgoingWithResponseTransport(pushOs, respIs);
    }

    public IIncomingTransport getRegisterTransport(final Node client)
            throws IOException {

        final PipedOutputStream respOs = new PipedOutputStream();
        final PipedInputStream respIs = new PipedInputStream(respOs);

        runAtClient(runtimeConfiguration.getRegistrationUrl(), null, respOs,
                new IClientRunnable() {
                    public void run(BeanFactory factory, InputStream is,
                            OutputStream os) throws Exception {
                        // This should be basically what the registration servlet does ...
                        IRegistrationService service = (IRegistrationService) factory
                                .getBean(Constants.REGISTRATION_SERVICE);
                        service.registerNode(client, os);
                    }
                });
        return new InternalIncomingTransport(respIs);
    }

    public boolean sendAcknowledgement(Node remote,
            List<IncomingBatchHistory> list, Node local) throws IOException {
        try {
            if (list != null && list.size() > 0) {
                SymmetricEngine remoteEngine = getTargetEngine(remote
                        .getSyncURL());
                List<BatchInfo> batches = new ArrayList<BatchInfo>();
                for (IncomingBatchHistory loadStatus : list) {
                    if (loadStatus.getStatus() == Status.OK
                            || loadStatus.getStatus() == Status.SK) {
                        batches.add(new BatchInfo(loadStatus.getBatchId()));
                    } else {
                        batches.add(new BatchInfo(loadStatus.getBatchId(),
                                loadStatus.getFailedRowNumber()));
                    }
                }
                IAcknowledgeService service = (IAcknowledgeService) remoteEngine
                        .getApplicationContext().getBean(
                                Constants.ACKNOWLEDGE_SERVICE);
                service.ack(batches);

            }
            return true;
        } catch (Exception ex) {
            logger.error(ex, ex);
            return false;
        }
    }

    public void writeAcknowledgement(OutputStream out,
            List<IncomingBatchHistory> list) throws IOException {
        String data = getAcknowledgementData(list);
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, ENCODING), true);
        pw.println(data);
        pw.close();
    }

    private void runAtClient(final String url, final InputStream is,
            final OutputStream os, final IClientRunnable runnable) {
        new Thread() {
            public void run() {
                try {
                    SymmetricEngine engine = getTargetEngine(url);
                    runnable.run(engine.getApplicationContext(), is, os);
                } catch (Exception e) {
                    logger.error(e, e);
                } finally {
                    IOUtils.closeQuietly(is);
                    IOUtils.closeQuietly(os);
                }
            }
        }.start();
    }

    private SymmetricEngine getTargetEngine(String url) {
        SymmetricEngine engine = SymmetricEngine.findEngineByUrl(url);
        if (engine == null) {
            throw new NullPointerException(
                    "Could not find the engine reference for the following url: "
                            + url);
        } else {
            return engine;
        }
    }

    interface IClientRunnable {
        public void run(BeanFactory factory, InputStream is, OutputStream os)
                throws Exception;
    }

}
