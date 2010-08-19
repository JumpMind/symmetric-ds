/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *               Eric Long <erilong@users.sourceforge.net>
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

package org.jumpmind.symmetric.service;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public interface IDataExtractorService {

    public void extractConfiguration(Node node, Writer writer, DataExtractorContext ctx) throws IOException;
    
    public void extractConfigurationStandalone(Node node, OutputStream out) throws IOException;

    public void extractConfigurationStandalone(Node node, Writer out) throws IOException;
    
    public void extractInitialLoadWithinBatchFor(Node node, TriggerRouter trigger, Writer writer,
            DataExtractorContext ctx);

    /**
     * @return true if work was done or false if there was no work to do.
     */
    public boolean extract(Node node, IOutgoingTransport transport) throws IOException;

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId, String endBatchId)
            throws IOException;

    public boolean extractBatchRange(IExtractListener handler, String startBatchId, String endBatchId) throws IOException;

    public void addExtractorFilter(IExtractorFilter extractorFilter);

}
