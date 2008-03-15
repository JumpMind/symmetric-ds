/*
 * $Id: MessageReceiver.vm 11079 2008-02-27 15:52:01 +0000 (Wed, 27 Feb 2008) tcarlson $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.symmetric;

import org.jumpmind.symmetric.SymmetricEngineContextLoaderListener;
import org.jumpmind.symmetric.web.AckServlet;
import org.jumpmind.symmetric.web.PullServlet;
import org.jumpmind.symmetric.web.PushServlet;
import org.jumpmind.symmetric.web.RegistrationServlet;
import org.mortbay.http.HttpContext;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.util.InetAddrPort;
import org.mule.api.config.ThreadingProfile;
import org.mule.api.endpoint.ImmutableEndpoint;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.lifecycle.LifecycleException;
import org.mule.api.service.Service;
import org.mule.api.transport.Connector;
import org.mule.config.i18n.CoreMessages;
import org.mule.transport.AbstractMessageReceiver;
import org.mule.transport.ConnectException;
import org.mule.util.StringUtils;

/**
 * <code>SymmetricMessageReceiver</code> TODO document
 */
public class SymmetricMessageReceiver extends  AbstractMessageReceiver {

	private Server server;
	
    /* For general guidelines on writing transports see
       http://mule.mulesource.org/display/MULE/Writing+Transports */

    public SymmetricMessageReceiver(Connector connector, Service service,
    		ImmutableEndpoint endpoint)
            throws CreateException
    {
        super(connector, service, endpoint);
    }

    /**
     * This is all using Jetty 5.1.2 which is used by Mule.
     */
    public void doConnect() throws ConnectException
    {

    	disposing.set(false);
    	server = new Server();
    	SocketListener socketListener = new SocketListener(new InetAddrPort(endpoint.getEndpointURI()
                .getPort()));

        // apply Threading settings
        ThreadingProfile tp = connector.getReceiverThreadingProfile();
        socketListener.setMaxIdleTimeMs((int) tp.getThreadTTL());
        int threadsActive = tp.getMaxThreadsActive();
        int threadsMin = socketListener.getMinThreads();
        if (threadsMin >= threadsActive)
        {
            socketListener.setMinThreads(threadsActive - 1);
        }
        socketListener.setMaxThreads(threadsActive);
        // thread priorities are evil and gone from ThreadingProfile
        // (google for priority inversion)
        // socketListener.setThreadsPriority(tp.getThreadPriority());

        server.addListener(socketListener);

        String path = endpoint.getEndpointURI().getPath();
        if (StringUtils.isEmpty(path))
        {
            path = "/";
        }

        if (!path.endsWith("/"))
        {
            path += "/";
        }

        HttpContext context = server.getContext("/");
        context.setRequestLog(null);
       
        ServletHandler handler = new ServletHandler();
        handler.addServlet("PullServlet", path + "pull/*", PullServlet.class.getName());
        handler.addServlet("PushServlet", path + "push/*", PushServlet.class.getName());
        handler.addServlet("AckServlet", path + "ack/*", AckServlet.class.getName());
        handler.addServlet("RegistrationServlet", path + "registration/*", RegistrationServlet.class.getName());
        handler.addEventListener(new SymmetricEngineContextLoaderListener());

        context.addHandler(handler);
        // setAttribute is already synchronized in Jetty
        context.setAttribute("messageReceiver", this);
    }

    public void doDisconnect() throws ConnectException
    {
    	// this will cause the server thread to quit
        disposing.set(true);
        /* IMPLEMENTATION NOTE: Disconnects and tidies up any rources allocted
           using the doConnect() method. This method should return the
           MessageReceiver into a disconnected state so that it can be
           connected again using the doConnect() method. */

        // TODO release any resources here
    }

    public void doStart() throws LifecycleException
    {
        // Optional; does not need to be implemented. Delete if not required

        /* IMPLEMENTATION NOTE: Should perform any actions necessary to enable
           the reciever to start reciving events. This is different to the
           doConnect() method which actually makes a connection to the
           transport, but leaves the MessageReceiver in a stopped state. For
           polling-based MessageReceivers the start() method simply starts the
           polling thread, for the Axis Message receiver the start method on
           the SOAPService is called. What action is performed here depends on
           the transport being used. Most of the time a custom provider
           doesn't need to override this method. */
    	// can you detect if the engine is started?
    	if (!server.isStarted())
    	{
	    	try
	        {
	            server.start();
	        }
	        catch (Exception e)
	        {
	            throw new LifecycleException(CoreMessages.failedToStart("Symmetric Http Receiver"), e, this);
	        }
    	}
    }

    public void doStop()
    {
        // Optional; does not need to be implemented. Delete if not required

        /* IMPLEMENTATION NOTE: Should perform any actions necessary to stop
           the reciever from receiving events. */
    	
    	// how do you shutdown the SymmetricEngine?
    	// can the jobs be stopped?
    	try
        {
            server.stop();
        }
        catch (Exception e)
        {
            logger.error("Error stopping Symmetric receiver on: " + endpoint.getEndpointURI().toString(), e);
        }
    }

    public void doDispose()
    {
        // Optional; does not need to be implemented. Delete if not required

        /* IMPLEMENTATION NOTE: Is called when the Conector is being dispoed
           and should clean up any resources. The doStop() and doDisconnect()
           methods will be called implicitly when this method is called. */
    	server = null;
    }

}

