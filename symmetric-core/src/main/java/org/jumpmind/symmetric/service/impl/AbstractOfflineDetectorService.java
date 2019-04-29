/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.service.impl;

import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.exception.HttpException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatus.Status;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IOfflineDetectorService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.ServiceUnavailableException;
import org.jumpmind.symmetric.transport.SyncDisabledException;

/**
 * Abstract service that provides help methods for detecting offline status.
 */
public abstract class AbstractOfflineDetectorService extends AbstractService implements IOfflineDetectorService {

    protected IExtensionService extensionService;

    private Map<String, Long> transportErrorTimeByNode = new HashMap<String, Long>();

    public AbstractOfflineDetectorService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IExtensionService extensionService) {
        super(parameterService, symmetricDialect);
        this.extensionService = extensionService;
    }

    protected void fireOnline(Node remoteNode, RemoteNodeStatus status) {
        transportErrorTimeByNode.remove(remoteNode.getNodeId());
        List<IOfflineClientListener> offlineListeners = extensionService.getExtensionPointList(IOfflineClientListener.class);
        if (offlineListeners != null) {
            for (IOfflineClientListener listener : offlineListeners) {
                listener.online(remoteNode);
            }
        }
    }
    
    protected void fireOffline(Exception exception, Node remoteNode, RemoteNodeStatus status) {
        String syncUrl = remoteNode.getSyncUrl() == null ? parameterService.getRegistrationUrl() : remoteNode
                        .getSyncUrl();
        Throwable cause = getRootCause(exception);
        if (cause == null) {
            cause = exception;
        }
        if (isOffline(exception)) {
            if (shouldLogTransportError(remoteNode.getNodeId())) {
                log.warn(String.format("Could not communicate with %s at %s because: %s", remoteNode, syncUrl, exception), exception);
            } else {
                log.info(String.format("Could not communicate with %s at %s because: %s", remoteNode, syncUrl, exception));
            }
            status.setStatus(Status.OFFLINE);
        } else if (isServiceUnavailable(exception)) {
            if (shouldLogTransportError(remoteNode.getNodeId())) {
                log.warn("Remote node {} at {} was unavailable.", new Object[] {remoteNode, syncUrl});
            } else {
                log.info("Remote node {} at {} was unavailable.  It may be starting up.", new Object[] {remoteNode, syncUrl});
            }
            status.setStatus(Status.OFFLINE);            
        } else if (isBusy(exception)) {
            if (shouldLogTransportError(remoteNode.getNodeId())) {
                log.warn("Remote node {} at {} was busy", new Object[] {remoteNode, syncUrl});
            } else {
                log.info("Remote node {} at {} was busy", new Object[] {remoteNode, syncUrl});
            }
            status.setStatus(Status.BUSY);
        } else if (isNotAuthenticated(exception)) {
            log.warn("Authorization denied from {} at {}", new Object[] {remoteNode, syncUrl});
            status.setStatus(Status.NOT_AUTHORIZED);
        } else if (isSyncDisabled(exception)) {
            log.warn("Sync was not enabled for {} at {}", new Object[] {remoteNode, syncUrl});
            status.setStatus(Status.SYNC_DISABLED);
        } else if (isRegistrationRequired(exception)) {
            log.warn("Registration was not open at {}", new Object[] {remoteNode, syncUrl});
            status.setStatus(Status.REGISTRATION_REQUIRED);
        } else if (getHttpException(exception) != null) {
            HttpException http = getHttpException(exception);
            if (shouldLogTransportError(remoteNode.getNodeId())) {
                log.warn(String.format("Could not communicate with %s at %s because it returned HTTP code %s", remoteNode, syncUrl, http.getCode()), exception);
            } else {
                log.info(String.format("Could not communicate with %s at %s because it returned HTTP code %s", remoteNode, syncUrl, http.getCode()));
            }
        } else {
            log.warn(String.format("Could not communicate with node '%s' at %s because of unexpected error", remoteNode, syncUrl), exception);
            status.setStatus(Status.UNKNOWN_ERROR);
        }

        List<IOfflineClientListener> offlineListeners = extensionService.getExtensionPointList(IOfflineClientListener.class);
        if (offlineListeners != null) {
            for (IOfflineClientListener listener : offlineListeners) {
                if (isOffline(exception)) {
                    listener.offline(remoteNode);
                } else if (isBusy(exception)) {
                    listener.busy(remoteNode);
                } else if (isNotAuthenticated(exception)) {
                    listener.notAuthenticated(remoteNode);
                } else if (isSyncDisabled(exception)) {
                    listener.syncDisabled(remoteNode);
                } else if (isRegistrationRequired(exception)) {
                    listener.registrationRequired(remoteNode);
                } else {
                    listener.unknownError(remoteNode, exception);
                }
            }
        }
    }

    protected boolean shouldLogTransportError(String nodeId) {
        long maxErrorMillis = parameterService.getLong(ParameterConstants.TRANSPORT_MAX_ERROR_MILLIS, 300000);
        Long errorTime = transportErrorTimeByNode.get(nodeId);
        if (errorTime == null) {
            errorTime = System.currentTimeMillis();
            transportErrorTimeByNode.put(nodeId, errorTime);
        }
        return System.currentTimeMillis() - errorTime >= maxErrorMillis;
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
            Throwable cause = getRootCause(ex);
            if (cause == null) {
                cause = ex;
            }
            offline = cause instanceof SocketException || 
              cause instanceof ConnectException ||
              cause instanceof SocketTimeoutException ||
              cause instanceof UnknownHostException;
        }
        return offline;
    }

    protected boolean isNotAuthenticated(Exception ex) {
        boolean offline = false;
        if (ex != null) {
            Throwable cause = getRootCause(ex);
            offline = ex instanceof AuthenticationException || 
                    cause instanceof AuthenticationException;
        }
        return offline;
    }

    protected boolean isBusy(Exception ex) {
        boolean offline = false;
        if (ex != null) {
            Throwable cause = getRootCause(ex);
            offline = ex instanceof ConnectionRejectedException || 
                    cause instanceof ConnectionRejectedException;
        }
        return offline;
    }
    
    protected boolean isServiceUnavailable(Exception ex){
        boolean offline = false;
        if (ex != null) {
            Throwable cause = getRootCause(ex);
            offline = ex instanceof ServiceUnavailableException || 
                    cause instanceof ServiceUnavailableException;
        }
        return offline;
    }
    
    protected boolean isSyncDisabled(Exception ex) {
        boolean syncDisabled = false;
        if (ex != null) {
            Throwable cause = getRootCause(ex);
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
            Throwable cause = getRootCause(ex);
            registrationRequired = cause instanceof RegistrationRequiredException || 
                    cause instanceof RegistrationNotOpenException;
            if (registrationRequired == false && (ex instanceof RegistrationRequiredException ||
                    cause instanceof RegistrationNotOpenException)) {
                registrationRequired = true;
            }
        }
        return registrationRequired;
    }
    
    protected HttpException getHttpException(Exception ex) {
        HttpException exception = null;
        if (ex != null) {
            Throwable cause = getRootCause(ex);
            if (cause instanceof HttpException) {
                exception = (HttpException) cause; 
            } else if (ex instanceof HttpException) {
                exception = (HttpException) ex;
            }
        }
        return exception;
    }
    
    protected Throwable getRootCause(Exception ex) {
        Throwable cause = ExceptionUtils.getRootCause(ex);
        if (cause == null) {
            cause = ex;
        }
        return cause;
    }

}