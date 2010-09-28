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
 * under the License.  */


package org.jumpmind.symmetric.transport.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.StandaloneSymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IConfigurationService;
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
 *
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
public class InternalTransportManager extends AbstractTransportManager implements ITransportManager {

    static final ILog log = LogFactory.getLog(InternalTransportManager.class);

    IConfigurationService configurationService;
    public InternalTransportManager(IConfigurationService configurationService) {
        super();
        this.configurationService = configurationService;
    }

    public IIncomingTransport getPullTransport(Node remote, final Node local,
            String securityToken, Map<String, String> requestProperties, String registrationUrl)
            throws IOException {
        final PipedOutputStream respOs = new PipedOutputStream();
        final PipedInputStream respIs = new PipedInputStream(respOs);

        final ChannelMap suspendIgnoreChannels = configurationService.getSuspendIgnoreChannelLists(remote.getNodeId());

        runAtClient(remote.getSyncUrl(), null, respOs, new IClientRunnable() {
            public void run(BeanFactory factory, InputStream is, OutputStream os) throws Exception {
                IDataExtractorService extractor = (IDataExtractorService) factory
                        .getBean(Constants.DATAEXTRACTOR_SERVICE);
                IOutgoingTransport transport = new InternalOutgoingTransport(respOs, suspendIgnoreChannels);
                extractor.extract(local, transport);
                transport.close();
            }
        });
        return new InternalIncomingTransport(respIs);
    }

    public IOutgoingWithResponseTransport getPushTransport(Node remote,
        Node local, String securityToken, String registrationUrl) throws IOException {
        final PipedOutputStream pushOs = new PipedOutputStream();
        final PipedInputStream pushIs = new PipedInputStream(pushOs);

        final PipedOutputStream respOs = new PipedOutputStream();
        final PipedInputStream respIs = new PipedInputStream(respOs);

        runAtClient(remote.getSyncUrl(), pushIs, respOs, new IClientRunnable() {
            public void run(BeanFactory factory, InputStream is, OutputStream os) throws Exception {
                // This should be basically what the push servlet does ...
                IDataLoaderService service = (IDataLoaderService) factory.getBean(Constants.DATALOADER_SERVICE);
                service.loadData(pushIs, respOs);
            }
        });
        return new InternalOutgoingWithResponseTransport(pushOs, respIs);
    }

    public IIncomingTransport getRegisterTransport(final Node client, String registrationUrl) throws IOException {

        final PipedOutputStream respOs = new PipedOutputStream();
        final PipedInputStream respIs = new PipedInputStream(respOs);

        runAtClient(registrationUrl, null, respOs, new IClientRunnable() {
            public void run(BeanFactory factory, InputStream is, OutputStream os) throws Exception {
                // This should be basically what the registration servlet does
                // ...
                IRegistrationService service = (IRegistrationService) factory.getBean(Constants.REGISTRATION_SERVICE);
                service.registerNode(client, os, false);
            }
        });
        return new InternalIncomingTransport(respIs);
    }

    public int sendAcknowledgement(Node remote, List<IncomingBatch> list,
            Node local, String securityToken, String registrationUrl) throws IOException {
        try {
            if (list != null && list.size() > 0) {
                ISymmetricEngine remoteEngine = getTargetEngine(remote.getSyncUrl());

                String ackData = getAcknowledgementData(local.getNodeId(), list);
                List<BatchInfo> batches = readAcknowledgement(ackData);
                IAcknowledgeService service = (IAcknowledgeService) remoteEngine.getApplicationContext().getBean(
                        Constants.ACKNOWLEDGE_SERVICE);
                for (BatchInfo batchInfo : batches) {
                    service.ack(batchInfo);
                }

            }
            return HttpURLConnection.HTTP_OK;
        } catch (Exception ex) {
            log.error(ex);
            return -1;
        }
    }

    public void writeAcknowledgement(OutputStream out,
        List<IncomingBatch> list, Node local, String securityToken)
        throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, Constants.ENCODING), true);
        pw.println(getAcknowledgementData(local.getNodeId(), list));
        pw.close();
    }

    private void runAtClient(final String url, final InputStream is, final OutputStream os,
            final IClientRunnable runnable) {
        new Thread() {
            public void run() {
                try {
                    ISymmetricEngine engine = getTargetEngine(url);
                    runnable.run(engine.getApplicationContext(), is, os);
                } catch (Exception e) {
                    log.error(e);
                } finally {
                    IOUtils.closeQuietly(is);
                    IOUtils.closeQuietly(os);
                }
            }
        }.start();
    }

    private ISymmetricEngine getTargetEngine(String url) {
        ISymmetricEngine engine = StandaloneSymmetricEngine.findEngineByUrl(url);
        if (engine == null) {
            throw new NullPointerException("Could not find the engine reference for the following url: " + url);
        } else {
            return engine;
        }
    }

    interface IClientRunnable {
        public void run(BeanFactory factory, InputStream is, OutputStream os) throws Exception;
    }

}