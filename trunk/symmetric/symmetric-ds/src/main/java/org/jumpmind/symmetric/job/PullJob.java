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

import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;

public class PullJob extends AbstractJob {

    private IPullService pullService;
    
    private INodeService nodeService;

    @Override
    public void doJob() throws Exception {
        boolean dataPulled = pullService.pullData();

        // Re-pull immediately if we are in the middle of an initial load
        // so that the initial load completes as quickly as possible.
        while (nodeService.isDataLoadStarted() && dataPulled) {
            dataPulled = pullService.pullData();
        }
    }

    public void setPullService(IPullService service) {
        this.pullService = service;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
}