/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.service.impl;

import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IOfflineDetectorService;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.SyncDisabledException;

/**
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
public abstract class AbstractOfflineDetectorService extends AbstractService implements IOfflineDetectorService {

    private List<IOfflineClientListener> offlineListeners;

    public void setOfflineListeners(List<IOfflineClientListener> listeners) {
        this.offlineListeners = listeners;
    }

    public void addOfflineListener(IOfflineClientListener listener) {
        if (offlineListeners == null) {
            offlineListeners = new ArrayList<IOfflineClientListener>();
        }
        offlineListeners.add(listener);
    }

    public boolean removeOfflineListener(IOfflineClientListener listener) {
        if (offlineListeners != null) {
            return offlineListeners.remove(listener);
        } else {
            return false;
        }
    }

    protected void fireOffline(Exception error, Node remoteNode) {
        if (offlineListeners != null) {
            for (IOfflineClientListener listener : offlineListeners) {
                if (isOffline(error)) {
                    listener.offline(remoteNode);
                } else if (isBusy(error)) {
                    listener.busy(remoteNode);
                } else if (isNotAuthenticated(error)) {
                    listener.notAuthenticated(remoteNode);
                } else if (isSyncDisabled(error)) {
                    listener.syncDisabled(remoteNode);
                } else if (isRegistrationRequired(error)) {
                    listener.registrationRequired(remoteNode);
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
    
    protected boolean isSyncDisabled(Exception ex) {
        boolean syncDisabled = false;
        if (ex != null) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            syncDisabled = cause instanceof SyncDisabledException;
            if (syncDisabled == false && ex instanceof SyncDisabledException) {
                syncDisabled = true;
            }
        }
        return syncDisabled;
    }
    
    protected boolean isRegistrationRequired(Exception ex) {
        boolean registrationRequired = false;
        if (ex != null) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            registrationRequired = cause instanceof RegistrationRequiredException;
            if (registrationRequired == false && ex instanceof RegistrationRequiredException) {
                registrationRequired = true;
            }
        }
        return registrationRequired;
    }
}