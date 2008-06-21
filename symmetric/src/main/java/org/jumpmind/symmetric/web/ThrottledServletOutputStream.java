/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Henglin Wang <henglinwang@users.sourceforge.net>
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
package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletOutputStream;

import org.jumpmind.symmetric.util.MeteredOutputStream;

public class ThrottledServletOutputStream extends ServletOutputStream {
    private MeteredOutputStream stream;

    public ThrottledServletOutputStream(OutputStream output, long maxBps) {
        stream = new MeteredOutputStream(output, maxBps);
    }

    public ThrottledServletOutputStream(OutputStream output, long maxBps, long threshold) {
        stream = new MeteredOutputStream(output, maxBps, threshold);
    }

    public ThrottledServletOutputStream(OutputStream output, long maxBps, long threshold, long checkPoint) {
        stream = new MeteredOutputStream(output, maxBps, threshold, checkPoint);
    }

    @Override
    public void write(int b) throws IOException {
        stream.write(b);

    }

    public void write(byte[] b) throws IOException {
        stream.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        stream.write(b, off, len);
    }

    @Override
    public void close() throws IOException {

        super.close();
        stream.close();
    }

    @Override
    public void flush() throws IOException {

        super.flush();
        stream.flush();
    }

}
