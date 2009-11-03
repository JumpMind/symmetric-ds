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
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.StandaloneSymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;

abstract public class AbstractJob extends TimerTask implements BeanFactoryAware, BeanNameAware {

    DataSource dataSource;

    protected final ILog log = LogFactory.getLog(getClass());

    private boolean needsRescheduled;

    private String rescheduleDelayParameter;

    private BeanFactory beanFactory;

    protected IParameterService parameterService;

    private String beanName;

    private boolean requiresRegistration = true;

    private ISymmetricEngine engine;

    protected boolean rescheduleImmediately = false;

    private IJobManager jobManager;

    @Override
    public boolean cancel() {
        log.info("JobCancelled", beanName);
        return super.cancel();
    }

    @Override
    public void run() {
        try {
            if (engine == null) {
                engine = StandaloneSymmetricEngine.findEngineByName(parameterService.getString(ParameterConstants.ENGINE_NAME));
            }

            if (engine == null) {
                log.info("SymmetricEngineMissing", beanName);
            } else if (engine.isStarted()) {
                IRegistrationService service = (IRegistrationService) beanFactory
                        .getBean(Constants.REGISTRATION_SERVICE);
                if (!requiresRegistration || (requiresRegistration && service.isRegisteredWithServer())) {
                    doJob();
                } else {
                    log.warn("SymmetricEngineNotRegistered");
                }
            } else {
                log.info("SymmetricEngineNotStarted");
            }
        } catch (final Throwable ex) {
            log.error(ex);
        } finally {
            reschedule();
        }
    }

    abstract void doJob() throws Exception;

    protected void reschedule() {
        if (needsRescheduled && engine != null && (engine.isStarted() || engine.isStarting())) {
            final String timerName = getClass().getName().substring(getClass().getName().lastIndexOf(".") + 1)
                    .toLowerCase();
            final Timer timer = new Timer(timerName);
            timer.schedule((TimerTask) beanFactory.getBean(beanName), rescheduleImmediately ? 0 : parameterService
                    .getLong(rescheduleDelayParameter));
            jobManager.addTimer(timerName, timer);
            rescheduleImmediately = false;
            log.debug("JobRescheduling", beanName, parameterService.getLong(rescheduleDelayParameter));
        } else if (needsRescheduled) {
            log.warn("Did not reschedule because the engine was not set.");
        }
    }

    protected void printDatabaseStats() {
        if (dataSource instanceof BasicDataSource) {
            final BasicDataSource ds = (BasicDataSource) dataSource;
            log.debug("DatabaseStats", ds.getNumActive());
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

    public void setJobManager(IJobManager jobManager) {
        this.jobManager = jobManager;
    }

}
