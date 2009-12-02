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

package org.jumpmind.symmetric.transport.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ISyncUrlExtension;
import org.jumpmind.symmetric.transport.ITransportManager;

public class MockTransportManager implements ITransportManager {

    protected IIncomingTransport incomingTransport;

    protected IOutgoingWithResponseTransport outgoingTransport;

    public String resolveURL(String url) {
        return null;
    }

    public void addExtensionSyncUrlHandler(String name, ISyncUrlExtension handler) {
    }

    public IIncomingTransport getPullTransport(Node remote, Node local,
            String securityToken, Map<String, String> requestProperties)
            throws IOException {
        return incomingTransport;
    }

    public IOutgoingWithResponseTransport getPushTransport(Node remote,
        Node local, String securityToken) throws IOException {
        return outgoingTransport;
    }

    public boolean sendAcknowledgement(Node remote, List<IncomingBatch> list,
                Node local, String securityToken) throws IOException {
        return true;
    }

    public void writeAcknowledgement(OutputStream out,
            List<IncomingBatch> list, Node local, String securityToken)
            throws IOException {
    }

    public IIncomingTransport getIncomingTransport() {
        return incomingTransport;
    }

    public void setIncomingTransport(IIncomingTransport is) {
        this.incomingTransport = is;
    }

    public IOutgoingWithResponseTransport getOutgoingTransport() {
        return outgoingTransport;
    }

    public void setOutgoingTransport(IOutgoingWithResponseTransport outgoingTransport) {
        this.outgoingTransport = outgoingTransport;
    }

    public IIncomingTransport getRegisterTransport(Node node) throws IOException {
        return incomingTransport;
    }

    public List<BatchInfo> readAcknowledgement(String parameterString) throws IOException {
        return null;
    }

    public List<BatchInfo> readAcknowledgement(Map<String, Object> parameters) {
        return null;
    }

    public List<BatchInfo> readAcknowledgement(String parameterString1, String parameterString2) throws IOException {
        return null;
    }

}
