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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;

public class InternalOutgoingWithResponseTransport implements IOutgoingWithResponseTransport {

    BufferedWriter writer = null;

    BufferedReader reader = null;

    boolean open = true;

    InternalOutgoingWithResponseTransport(OutputStream pushOs, InputStream respIs) {
        writer = new BufferedWriter(new OutputStreamWriter(pushOs));
        reader = new BufferedReader(new InputStreamReader(respIs));
    }

    public BufferedReader readResponse() throws IOException {
        IOUtils.closeQuietly(writer);
        return reader;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(writer);
        IOUtils.closeQuietly(reader);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public BufferedWriter open() throws IOException {
        return writer;
    }

}