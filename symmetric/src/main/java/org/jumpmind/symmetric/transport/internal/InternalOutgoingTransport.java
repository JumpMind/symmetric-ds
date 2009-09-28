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

package org.jumpmind.symmetric.transport.internal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class InternalOutgoingTransport implements IOutgoingTransport {

    BufferedWriter writer = null;

    ChannelMap map = null;

    boolean open = true;

    public InternalOutgoingTransport(OutputStream pushOs) throws UnsupportedEncodingException {
        this(pushOs,new ChannelMap());
    }
    
    public InternalOutgoingTransport(OutputStream pushOs, ChannelMap map) throws UnsupportedEncodingException {
        writer = new BufferedWriter(new OutputStreamWriter(pushOs, Constants.ENCODING));
            this.map = map;
    }

    public InternalOutgoingTransport(BufferedWriter writer) {
        this.writer = writer;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(writer);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public BufferedWriter open() throws IOException {
        return writer;
    }

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) {
        return map;
    }

}
