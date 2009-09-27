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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.web.WebConstants;

public class MockOutgoingTransport implements IOutgoingTransport {

    private StringWriter writer = new StringWriter();
    private BufferedWriter bWriter;

    public MockOutgoingTransport() {
    }

    public void close() throws IOException {
        bWriter.flush();
    }

    public BufferedWriter open() throws IOException {
        bWriter = new BufferedWriter(writer);
        return bWriter;
    }

    public boolean isOpen() {
        return true;
    }

    public String toString() {
        try {
            bWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer.getBuffer().toString();
    }

    public Map<String, Set<String>> getSuspendIgnoreChannelLists(IConfigurationService configurationService)
            throws IOException {
        Map<String, Set<String>> suspendIgnoreChannelsList = new HashMap<String, Set<String>>();

        Set<String> suspendChannels = new TreeSet<String>();
        suspendIgnoreChannelsList.put(WebConstants.SUSPENDED_CHANNELS, suspendChannels);

        Set<String> ignoreChannels = new TreeSet<String>();

        suspendIgnoreChannelsList.put(WebConstants.IGNORED_CHANNELS, ignoreChannels);
        return suspendIgnoreChannelsList;
    }

}
