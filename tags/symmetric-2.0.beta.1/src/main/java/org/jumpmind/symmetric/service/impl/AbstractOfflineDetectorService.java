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
package org.jumpmind.symmetric.service.impl;

import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.symmetric.io.IOfflineListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IOfflineDetectorService;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;

public abstract class AbstractOfflineDetectorService extends AbstractService implements IOfflineDetectorService {

    private List<IOfflineListener> offlineListeners;

    public void setOfflineListeners(List<IOfflineListener> listeners) {
        this.offlineListeners = listeners;
    }

    public void addOfflineListener(IOfflineListener listener) {
        if (offlineListeners == null) {
            offlineListeners = new ArrayList<IOfflineListener>();
        }
        offlineListeners.add(listener);
    }

    public boolean removeOfflineListener(IOfflineListener listener) {
        if (offlineListeners != null) {
            return offlineListeners.remove(listener);
        } else {
            return false;
        }
    }

    protected void fireOffline(Exception error, Node remoteNode) {
        if (offlineListeners != null) {
            for (IOfflineListener listener : offlineListeners) {
                if (isOffline(error)) {
                    listener.offline(remoteNode);
                } else if (isBusy(error)) {
                    listener.busy(remoteNode);
                } else if (isNotAuthenticated(error)) {
                    listener.notAuthenticated(remoteNode);
                }
            }
        }
    }

    /**
     * Check to see if the {@link Exception} was caused by an offline scenario.
     * 
     * @param ex
     *            The exception to check. Nested exception will also be checked.
     * @return true if this exception was caused by the {@link Node} being offline.
     */
    protected boolean isOffline(Exception ex) {
        boolean offline = false;
        if (ex != null) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            offline = cause instanceof SocketException || cause instanceof ConnectException;
        }
        return offline;
    }

    protected boolean isNotAuthenticated(Exception ex) {
        boolean offline = false;
        if (ex != null) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            offline = cause instanceof AuthenticationException;
        }
        return offline;
    }

    protected boolean isBusy(Exception ex) {
        boolean offline = false;
        if (ex != null) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            offline = cause instanceof ConnectionRejectedException;
        }
        return offline;
    }
}
