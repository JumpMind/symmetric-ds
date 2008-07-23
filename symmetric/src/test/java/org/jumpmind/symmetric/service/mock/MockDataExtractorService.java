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
package org.jumpmind.symmetric.service.mock;

import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IExtractListener;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class MockDataExtractorService implements IDataExtractorService {

    public boolean extract(Node node, IOutgoingTransport transport) throws Exception {
        return false;
    }

    public boolean extract(Node node, IExtractListener handler) throws Exception {
        return false;
    }

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId, String endBatchId)
            throws Exception {
        return false;
    }

    public boolean extractBatchRange(IExtractListener handler, String startBatchId, String endBatchId) throws Exception {
        return false;
    }

    public OutgoingBatch extractInitialLoadFor(Node node, Trigger config, IOutgoingTransport transport) {
        return null;
    }

    public void extractInitialLoadWithinBatchFor(Node node, Trigger trigger, IOutgoingTransport transport,
            DataExtractorContext ctx) {
    }

    public OutgoingBatch extractNodeIdentityFor(Node node, IOutgoingTransport transport) {
        return null;
    }

    public void addExtractorFilter(IExtractorFilter extractorFilter) {
    }

}
