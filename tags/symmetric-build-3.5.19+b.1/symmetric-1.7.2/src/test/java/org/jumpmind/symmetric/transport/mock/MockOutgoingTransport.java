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

import org.jumpmind.symmetric.transport.IOutgoingTransport;

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

}
