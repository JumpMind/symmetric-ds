/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;

public class PullJob extends AbstractJob {

    private static final Log logger = LogFactory.getLog(PushJob.class);

    private IPullService pullService;
    
    private INodeService nodeService;

    @Override
    public void doJob() throws Exception {
        pullService.pullData();

        // Reschedule immediately if we are in the middle of an initial load
        // so that the initial load completes as quickly as possible.
        if (nodeService.isDataLoadStarted()) {
            rescheduleImmediately = true;
        }
    }

    public void setPullService(IPullService service) {
        this.pullService = service;
    }

    @Override
    Log getLogger() {
        return logger;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
}