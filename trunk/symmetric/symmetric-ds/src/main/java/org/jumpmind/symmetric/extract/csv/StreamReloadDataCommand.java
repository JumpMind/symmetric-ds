/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * Copyright (C) Andrew Wilcox <andrewbwilcox@users.sourceforge.net>
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

package org.jumpmind.symmetric.extract.csv;

import java.io.Writer;
import java.io.IOException;

import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.ITriggerRouterService;

class StreamReloadDataCommand extends AbstractStreamDataCommand {

    private IDataExtractorService dataExtractorService;

    private ITriggerRouterService triggerRouterService;

    private INodeService nodeService;

    public void execute(Writer out, Data data, String routerId, DataExtractorContext context) throws IOException {
        String triggerId = data.getTriggerHistory().getTriggerId();
        TriggerRouter triggerRouter = triggerRouterService.findTriggerRouterById(triggerId, routerId);
        if (triggerRouter != null) {
            // The initial_load_select can be overridden
            if (data.getRowData() != null) {      
                triggerRouter.setInitialLoadSelect(data.getRowData());
            }
            Node node = nodeService.findNode(context.getBatch().getNodeId());
            dataExtractorService.extractInitialLoadWithinBatchFor(node, triggerRouter, out, context, data.getTriggerHistory());
            out.flush();
        } else {
            log.error("TriggerRouterUnavailable", triggerId, routerId);
        }
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
    
    public boolean isTriggerHistoryRequired() {
        return true;
    }

}