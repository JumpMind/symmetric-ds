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


package org.jumpmind.symmetric.transport.handler;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;

import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndContentImpl;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.feed.synd.SyndFeedImpl;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedOutput;

/**
 * ,
 */
public class AlertResourceHandler extends AbstractTransportResourceHandler {
    private static final int MAX_ERRORS = 1000;

    private static final FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    private IIncomingBatchService incomingBatchService;

    private IOutgoingBatchService outgoingBatchService;

    private IParameterService parameterService;

    private INodeService nodeService;

    public void write(CharSequence feedURL, Writer outputWriter) throws IOException {
        try {
            SyndFeed feed = new SyndFeedImpl();
            feed.setFeedType("rss_2.0");
            feed.setTitle("SymmetricDS Alerts for " + parameterService.getSyncUrl());
            feed.setDescription("Problems synchronizing data");
            feed.setLink(feedURL.toString());

            List<SyndEntry> entries = new ArrayList<SyndEntry>();

            for (IncomingBatch batch : findIncomingBatchErrors()) {
                String title = "Incoming Batch " + batch.getNodeBatchId();
                StringBuilder value = new StringBuilder("Node ");
                value.append(batch.getNodeId());
                value.append(" incoming batch ");
                value.append(batch.getBatchId());
                value.append(" is in error at ");
                value.append(formatDate(batch.getCreateTime()));
                value.append(".  ");
                int sqlCode = batch.getSqlCode();
                String msg = batch.getSqlMessage();
                if (sqlCode > 0 || !StringUtils.isBlank(msg)) {
                    value.append("The sql error code is ");
                    value.append(sqlCode);
                    value.append(" and the error message is: ");
                    value.append(msg);
                }
                entries.add(createEntry(title, value.toString(), batch.getCreateTime(), nodeService.findNode(
                        batch.getNodeId()).getSyncUrl()
                        + "/batch/" + batch.getBatchId()));
            }

            for (OutgoingBatch batch : findOutgoingBatchErrors().getBatches()) {
                String title = "Outgoing Batch " + batch.getNodeBatchId();
                StringBuilder value = new StringBuilder("Node ");
                value.append(batch.getNodeId());
                value.append(" outgoing batch ");
                value.append(batch.getBatchId());
                value.append(" is in error at ");
                value.append(formatDate(batch.getCreateTime()));
                value.append(".  ");
                value.append("The batch has been attempted ");
                value.append(batch.getSentCount());
                value.append(" times.  ");
                int sqlCode = batch.getSqlCode();
                String msg = batch.getSqlMessage();
                if (sqlCode > 0 || !StringUtils.isBlank(msg)) {
                    value.append("The sql error code is ");
                    value.append(sqlCode);
                    value.append(" and the error message is: ");
                    value.append(msg);
                }

                entries.add(createEntry(title, value.toString(), batch.getCreateTime(), "batch/" + batch.getBatchId()));
            }

            Collections.sort(entries, new SyndEntryOrderer());
            feed.setEntries(entries);

            SyndFeedOutput out = new SyndFeedOutput();
            out.output(feed, outputWriter);
        } catch (FeedException e) {
            log.warn(e);
            throw new IOException(e.getMessage());
        }
    }

    class SyndEntryOrderer implements Comparator<SyndEntry> {
        public int compare(SyndEntry o1, SyndEntry o2) {
            return o1.getPublishedDate().compareTo(o2.getPublishedDate());
        }
    }

    private SyndEntry createEntry(String title, String value, Date publishedDate, String sourceLink) {
        SyndEntry entry = new SyndEntryImpl();
        entry.setTitle(title);
        if (sourceLink != null) {
            entry.setLink(sourceLink);
        }
        entry.setPublishedDate(publishedDate);
        SyndContent content = new SyndContentImpl();
        content.setType("text/html");
        content.setValue(value);
        entry.setDescription(content);
        return entry;
    }

    private String formatDate(Date date) {
        return formatter.format(date);
    }

    private List<IncomingBatch> findIncomingBatchErrors() {
        return getIncomingBatchService().findIncomingBatchErrors(MAX_ERRORS);
    }

    private OutgoingBatches findOutgoingBatchErrors() {

        return getOutgoingBatchService().getOutgoingBatchErrors(MAX_ERRORS);
    }

    private IIncomingBatchService getIncomingBatchService() {
        return incomingBatchService;
    }

    public void setIncomingBatchService(IIncomingBatchService incomingBatchService) {
        this.incomingBatchService = incomingBatchService;
    }

    private IOutgoingBatchService getOutgoingBatchService() {
        return outgoingBatchService;
    }

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

}