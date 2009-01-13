/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
 *               
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.transport.handler;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.OutgoingBatchHistory.Status;
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

public class AlertResourceHandler extends AbstractTransportResourceHandler {
    private static final int MAX_ERRORS = 1000;

    private static final FastDateFormat formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    private IIncomingBatchService incomingBatchService;

    private IOutgoingBatchService outgoingBatchService;

    private IParameterService parameterService;
    
    private INodeService nodeService;

    public void write(CharSequence feedURL, Writer outputWriter) throws IOException, FeedException {
        SyndFeed feed = new SyndFeedImpl();
        feed.setFeedType("rss_2.0");
        feed.setTitle("SymmetricDS Alerts for " + parameterService.getMyUrl());
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
            List<IncomingBatchHistory> list = filterOutIncomingHistoryErrors(incomingBatchService
                    .findIncomingBatchHistory(batch.getBatchId(), batch.getNodeId()));
            if (list.size() > 0) {
                value.append("The batch has been attempted ");
                value.append(list.size());
                value.append(" times.  ");
                IncomingBatchHistory history = list.get(list.size() - 1);
                int sqlCode = history.getSqlCode();
                String msg = history.getSqlMessage();
                if (sqlCode > 0 || !StringUtils.isBlank(msg)) {
                    value.append("The sql error code is ");
                    value.append(sqlCode);
                    value.append(" and the error message is: ");
                    value.append(msg);
                }
            }
            entries.add(createEntry(title, value.toString(), batch.getCreateTime(), nodeService.findNode(batch.getNodeId()).getSyncURL() + "/batch/" + batch.getBatchId()));
        }

        for (OutgoingBatch batch : findOutgoingBatchErrors()) {
            String title = "Outgoing Batch " + batch.getNodeBatchId();
            StringBuilder value = new StringBuilder("Node ");
            value.append(batch.getNodeId());
            value.append(" outgoing batch ");
            value.append(batch.getBatchId());
            value.append(" is in error at ");
            value.append(formatDate(batch.getCreateTime()));
            value.append(".  ");
            List<OutgoingBatchHistory> histories = filterOutOutgoingHistoryErrors(outgoingBatchService
                    .findOutgoingBatchHistory(batch.getBatchId(), batch.getNodeId()));
            if (histories.size() > 0) {
                value.append("The batch has been attempted ");
                value.append(histories.size());
                value.append(" times.  ");
                OutgoingBatchHistory history = histories.get(histories.size() - 1);
                int sqlCode = history.getSqlCode();
                String msg = history.getSqlMessage();
                if (sqlCode > 0 || !StringUtils.isBlank(msg)) {
                    value.append("The sql error code is ");
                    value.append(sqlCode);
                    value.append(" and the error message is: ");
                    value.append(msg);
                }
            }

            entries.add(createEntry(title, value.toString(), batch.getCreateTime(), "batch/" + batch.getBatchId()));
        }

        Collections.sort(entries, new SyndEntryOrderer());
        feed.setEntries(entries);

        SyndFeedOutput out = new SyndFeedOutput();
        out.output(feed, outputWriter);
    }

    private List<IncomingBatchHistory> filterOutIncomingHistoryErrors(List<IncomingBatchHistory> list) {
        for (Iterator<IncomingBatchHistory> iterator = list.iterator(); iterator.hasNext();) {
            IncomingBatchHistory outgoingBatchHistory = iterator.next();
            if (outgoingBatchHistory.getStatus() != org.jumpmind.symmetric.model.IncomingBatchHistory.Status.ER) {
                iterator.remove();
            }
        }
        return list;
    }

    private List<OutgoingBatchHistory> filterOutOutgoingHistoryErrors(List<OutgoingBatchHistory> list) {
        for (Iterator<OutgoingBatchHistory> iterator = list.iterator(); iterator.hasNext();) {
            OutgoingBatchHistory outgoingBatchHistory = iterator.next();
            if (outgoingBatchHistory.getStatus() != Status.ER) {
                iterator.remove();
            }
        }
        return list;
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

    private List<OutgoingBatch> findOutgoingBatchErrors() {

        return getOutgoingBatchService().getOutgoingBatcheErrors(MAX_ERRORS);
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
