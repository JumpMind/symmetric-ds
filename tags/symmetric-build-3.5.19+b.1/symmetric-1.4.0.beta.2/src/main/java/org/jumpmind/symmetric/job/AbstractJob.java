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

import java.util.Timer;
import java.util.TimerTask;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;

abstract public class AbstractJob extends TimerTask implements BeanFactoryAware, BeanNameAware {
    DataSource dataSource;

    protected static final Log logger = LogFactory.getLog(AbstractJob.class);

    private boolean needsRescheduled;

    private String rescheduleDelayParameter;

    private BeanFactory beanFactory;

    protected IParameterService parameterService;

    private String beanName;

    private boolean requiresRegistration = true;

    private SymmetricEngine engine;

    @Override
    public boolean cancel() {
        logger.info("This job, " + beanName + ", has been cancelled.");
        return super.cancel();
    }

    @Override
    public void run() {
        try {
            if (engine == null) {
                engine = SymmetricEngine.findEngineByName(parameterService.getString(ParameterConstants.ENGINE_NAME));
            }
            
            if (engine == null) {
                logger.info("Could not find a reference to the SymmetricEngine from " + beanName);
            } else if (engine.isStarted()) {
                IRegistrationService service = (IRegistrationService) beanFactory
                        .getBean(Constants.REGISTRATION_SERVICE);
                if (!requiresRegistration || (requiresRegistration && service.isRegisteredWithServer())) {
                    doJob();
                } else {
                    getLogger().warn("Did not run job because the engine is not registered.");
                }
            } else {
                logger.info("The engine is not currently started.");
            }
        } catch (final Throwable ex) {
            getLogger().error(ex, ex);
        } finally {
            if (isNeedsRescheduled() && engine != null && (engine.isStarted() || engine.isStarting())) {
                reschedule();
            }
        }
    }

    abstract void doJob() throws Exception;

    abstract Log getLogger();

    protected void reschedule() {
        final Timer timer = new Timer();
        timer.schedule((TimerTask) beanFactory.getBean(beanName), parameterService.getLong(rescheduleDelayParameter));
        if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                    "Rescheduling " + beanName + " with " + parameterService.getLong(rescheduleDelayParameter)
                            + " ms delay.");
        }
    }

    protected void printDatabaseStats() {
        if (getLogger().isDebugEnabled() && dataSource instanceof BasicDataSource) {
            final BasicDataSource ds = (BasicDataSource) dataSource;
            getLogger().debug("There are currently " + ds.getNumActive() + " active database connections.");
        }
    }

    public void setBeanFactory(final BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    public void setBeanName(final String beanName) {
        this.beanName = beanName;
    }

    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public boolean isNeedsRescheduled() {
        return needsRescheduled;
    }

    public void setNeedsRescheduled(boolean needsRescheduled) {
        this.needsRescheduled = needsRescheduled;
    }

    public void setRescheduleDelayParameter(String rescheduleDelay) {
        this.rescheduleDelayParameter = rescheduleDelay;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setRequiresRegistration(boolean requiresRegistration) {
        this.requiresRegistration = requiresRegistration;
    }

}
