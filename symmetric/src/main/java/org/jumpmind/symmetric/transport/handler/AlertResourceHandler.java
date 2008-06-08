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
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;

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

    private static final FastDateFormat formatter = FastDateFormat
            .getInstance("yyyy-MM-dd HH:mm:ss");

    private IIncomingBatchService incomingBatchService;

    private IOutgoingBatchService outgoingBatchService;

    public void write(CharSequence feedURL, Writer outputWriter)
            throws IOException, FeedException {
        SyndFeed feed = new SyndFeedImpl();
        feed.setFeedType("rss_2.0");
        feed.setTitle("SymmetricDS Alerts");
        feed.setDescription("Problems synchronizing data");
        feed.setLink(feedURL.toString());

        List<SyndEntry> entries = new ArrayList<SyndEntry>();

        for (IncomingBatch batch : findIncomingBatchErrors()) {
            String title = "Incoming Batch " + batch.getNodeBatchId();
            String value = "Node " + batch.getNodeId() + " incoming batch "
                    + batch.getBatchId() + " is in error at "
                    + formatDate(batch.getCreateTime());
            entries.add(createEntry(title, value));
        }

        for (OutgoingBatch batch : findOutgoingBatchErrors()) {
            String title = "Outgoing Batch " + batch.getNodeBatchId();
            String value = "Node " + batch.getNodeId() + " outgoing batch "
                    + batch.getBatchId() + " is in error at "
                    + formatDate(batch.getCreateTime());
            entries.add(createEntry(title, value));
        }

        feed.setEntries(entries);

        SyndFeedOutput out = new SyndFeedOutput();
        out.output(feed, outputWriter);
    }

    private SyndEntry createEntry(String title, String value) {
        SyndEntry entry = new SyndEntryImpl();
        entry.setTitle(title);
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

    public void setIncomingBatchService(
            IIncomingBatchService incomingBatchService) {
        this.incomingBatchService = incomingBatchService;
    }

    private IOutgoingBatchService getOutgoingBatchService() {
        return outgoingBatchService;
    }

    public void setOutgoingBatchService(
            IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

}
