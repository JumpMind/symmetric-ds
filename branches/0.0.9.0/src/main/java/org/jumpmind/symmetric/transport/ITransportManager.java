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

package org.jumpmind.symmetric.transport;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.IncomingBatchHistory;

public interface ITransportManager {

    public boolean sendAcknowledgement(Node remote, List<IncomingBatchHistory> list, Node local) throws IOException;

    public void writeAcknowledgement(OutputStream out, List<IncomingBatchHistory> list) throws IOException;

    public IIncomingTransport getPullTransport(Node remote, Node local) throws IOException;
    
    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local) throws IOException;

    public IIncomingTransport getRegisterTransport(Node client) throws IOException;
    
}
