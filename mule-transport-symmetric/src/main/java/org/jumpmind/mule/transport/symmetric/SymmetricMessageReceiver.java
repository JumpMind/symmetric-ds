/*
 * $Id: MessageReceiver.vm 11079 2008-02-27 15:52:01 +0000 (Wed, 27 Feb 2008) tcarlson $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.jumpmind.mule.transport.symmetric;

import org.jumpmind.symmetric.SymmetricEngine;
import org.mule.api.endpoint.ImmutableEndpoint;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.lifecycle.LifecycleException;
import org.mule.api.service.Service;
import org.mule.api.transport.Connector;
import org.mule.config.i18n.CoreMessages;
import org.mule.transport.AbstractMessageReceiver;
import org.mule.transport.ConnectException;

/**
 * <code>SymmetricMessageReceiver</code> TODO document
 */
public class SymmetricMessageReceiver extends AbstractMessageReceiver {

    private SymmetricEngine engine;

    /*
     * For general guidelines on writing transports see
     * http://mule.mulesource.org/display/MULE/Writing+Transports
     */

    public SymmetricMessageReceiver(Connector connector, Service service,
            ImmutableEndpoint endpoint) throws CreateException {
        super(connector, service, endpoint);
    }

    public void doConnect() throws ConnectException {

        disposing.set(false);

        engine = new SymmetricEngine();
    }

    public void doDisconnect() throws ConnectException {
        // this will cause the server thread to quit
        disposing.set(true);

    }

    public void doStart() throws LifecycleException {
        if (engine != null && !engine.isStarting()) {
            try {
                engine.start();
            } catch (Exception e) {
                throw new LifecycleException(CoreMessages
                        .failedToStart("Symmetric Receiver"), e, this);
            }
        }
    }

    public void doStop() {
        if (engine != null) {
            /*
             * TODO: provide a way to stop the symmetricengine! try {
             * engine.stop(); } catch (Exception e) { throw new
             * LifecycleException(CoreMessages.failedToStop("Symmetric
             * Receiver"), e, this); }
             */
        }
    }

    public void doDispose() {
        engine = null;
    }

}
