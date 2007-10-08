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

package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public interface IDataExtractorService {

    public void extractClientIdentityFor(Node client,
            IOutgoingTransport transport);

    public OutgoingBatch extractInitialLoadFor(Node client, Trigger config,
            IOutgoingTransport transport);

    /**
     * @return true if work was done or false if there was no work to do.
     */
    public boolean extract(Node client, IOutgoingTransport transport)
            throws Exception;

    public boolean extract(Node client, final IExtractListener handler)
            throws Exception;

}
