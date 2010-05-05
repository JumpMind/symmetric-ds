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

package org.jumpmind.symmetric.job;

import javax.sql.DataSource;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.StandaloneSymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.beans.factory.BeanNameAware;

abstract public class AbstractJob implements Runnable, BeanNameAware {

    DataSource dataSource;

    protected final ILog log = LogFactory.getLog(getClass());

    protected IParameterService parameterService;

    private String beanName;

    private boolean requiresRegistration = true;

    private IRegistrationService registrationService;

    
    public String getName() {
        return beanName;
    }
    
    public void run() {
        try {
            ISymmetricEngine engine = StandaloneSymmetricEngine.findEngineByName(parameterService
                    .getString(ParameterConstants.ENGINE_NAME));

            if (engine == null) {
                log.info("SymmetricEngineMissing", beanName);
            } else if (engine.isStarted()) {
                if (!requiresRegistration
                        || (requiresRegistration && registrationService.isRegisteredWithServer())) {
                    doJob();
                } else {
                    log.warn("SymmetricEngineNotRegistered");
                }
            } else {
                log.info("SymmetricEngineNotStarted");
            }
        } catch (final Throwable ex) {
            log.error(ex);
        }
    }

    abstract void doJob() throws Exception;

    public void setBeanName(final String beanName) {
        this.beanName = beanName;
    }

    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setRequiresRegistration(boolean requiresRegistration) {
        this.requiresRegistration = requiresRegistration;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

}
