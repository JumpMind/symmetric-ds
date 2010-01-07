/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
 *               
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

package org.jumpmind.symmetric.transport.handler;

import java.io.IOException;
import java.io.OutputStream;

import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.ITransportResourceHandler;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;

/**
 * In order to better support other transports, the logic associated with
 * transport resources, e.g., pull, push, ack, and registration is isolated away
 * from the HttpServletRequest and HttpServletResponse.
 * 
 * Filters should probably eventually be done this way as well.
 * 
 * This should also probably be springified so that they can be injected into
 * all the right places.
 * 
 * @author Keith Naas <knaas@users.sourceforge.net>
 * 
 */
public abstract class AbstractTransportResourceHandler implements ITransportResourceHandler {

    protected IOutgoingTransport createOutgoingTransport(OutputStream outputStream) throws IOException {
        return new InternalOutgoingTransport(outputStream);
    }

}