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
import java.util.Map;

import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;

public interface ITransportManager {

    public boolean sendAcknowledgement(Node remote, List<IncomingBatch> list, Node local, String securityToken, String registrationUrl) throws IOException;

    public void writeAcknowledgement(OutputStream out, List<IncomingBatch> list, Node local, String securityToken) throws IOException;

    public List<BatchInfo> readAcknowledgement(String parameterString1, String parameterString2) throws IOException;

    public IIncomingTransport getPullTransport(Node remote, Node local, String securityToken, Map<String,String> requestProperties, String registrationUrl) throws IOException;

    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local, String securityToken, String registrationUrl) throws IOException;

    public IIncomingTransport getRegisterTransport(Node node, String registrationUrl) throws IOException;
    
    public void addExtensionSyncUrlHandler(String name, ISyncUrlExtension handler);
    
    /**
     * This is the proper way to determine the URL for a node.  It delegates to configured 
     * extension points when necessary to take in to account custom load balancing and
     * url selection schemes.
     * @param url This is the url configured in sync_url of the node table
     */
    public String resolveURL(String url, String registrationUrl);

}
