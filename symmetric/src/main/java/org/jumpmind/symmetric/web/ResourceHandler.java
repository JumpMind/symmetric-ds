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

package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.springframework.context.ApplicationContext;

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
public class ResourceHandler {

    protected ApplicationContext applicationContext;

    protected InputStream inputStream;

    protected OutputStream outputStream;

    public ResourceHandler(ApplicationContext applicationContext,
            InputStream inputStream, OutputStream outputStream) {
        this.applicationContext = applicationContext;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }

    protected IOutgoingTransport createOutgoingTransport(
            OutputStream outputStream) throws IOException {
        return new InternalOutgoingTransport(outputStream);
    }

    protected IDataLoaderService getDataLoaderService() {
        return (IDataLoaderService) getApplicationContext().getBean(
                Constants.DATALOADER_SERVICE, IDataLoaderService.class);
    }

    protected IDataService getDataService() {
        return (IDataService) getApplicationContext().getBean(
                Constants.DATA_SERVICE, IDataService.class);
    }

    protected INodeService getNodeService() {
        return (INodeService) getApplicationContext().getBean(
                Constants.NODE_SERVICE, INodeService.class);
    }

    protected IRegistrationService getRegistrationService() {
        return (IRegistrationService) getApplicationContext().getBean(
                Constants.REGISTRATION_SERVICE, IRegistrationService.class);
    }

    protected IDataExtractorService getDataExtractorService() {
        return (IDataExtractorService) getApplicationContext().getBean(
                Constants.DATAEXTRACTOR_SERVICE, IDataExtractorService.class);
    }

    protected IAcknowledgeService getAcknowledgeService() {
        return (IAcknowledgeService) getApplicationContext().getBean(
                Constants.ACKNOWLEDGE_SERVICE, IAcknowledgeService.class);
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

}