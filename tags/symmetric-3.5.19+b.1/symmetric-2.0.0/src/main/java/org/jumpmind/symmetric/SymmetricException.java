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
package org.jumpmind.symmetric;

import org.jumpmind.symmetric.common.Message;

/**
 * This is a {@link RuntimeException} that supports using the SymmetricDS
 * {@link Message} infrastructure
 */
public class SymmetricException extends RuntimeException {

    private static final long serialVersionUID = -3111453874504638368L;

    public SymmetricException() {
        super();
    }

    public SymmetricException(Throwable cause) {
        super(cause);
    }

    public SymmetricException(String messageKey) {
        super(Message.get(messageKey));
    }

    public SymmetricException(String messageKey, Object... args) {
        super(Message.get(messageKey, args));
    }

    public SymmetricException(String messageKey, Throwable cause) {
        super(Message.get(messageKey), cause);
    }

    public SymmetricException(String messageKey, Throwable cause,
            Object... args) {
        super(Message.get(messageKey, args), cause);
    }

}
