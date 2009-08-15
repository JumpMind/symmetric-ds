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

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;

public class JobManager implements IJobManager, BeanFactoryAware {

    final ILog log = LogFactory.getLog(JobManager.class);

    private Map<String, Timer> jobs;

    private BeanFactory beanFactory;

    private IParameterService parameterService;

    private void startJob(String name) {
        log.info("JobStarting", name);
        beanFactory.getBean(name);
    }

    public synchronized void addTimer(String timerName, Timer timer) {
        if (jobs == null) {
            jobs = new HashMap<String, Timer>();
        }
        jobs.put(timerName, timer);
    }

    /**
     * Start the jobs if they are configured to be started in
     * symmetric.properties
     */
    public synchronized void startJobs() {
        if (Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PUSH_JOB))) {
            startJob(Constants.PUSH_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PULL_JOB))) {
            startJob(Constants.PULL_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_ROUTE_JOB))) {
            startJob(Constants.ROUTE_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PURGE_JOB))) {
            startJob(Constants.PURGE_JOB_TIMER);
        }

        if (Boolean.TRUE.toString()
                .equalsIgnoreCase(parameterService.getString(ParameterConstants.START_HEARTBEAT_JOB))) {
            startJob(Constants.HEARTBEAT_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_SYNCTRIGGERS_JOB))) {
            startJob(Constants.SYNC_TRIGGERS_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_STATISTIC_FLUSH_JOB))) {
            startJob(Constants.STATISTIC_FLUSH_JOB_TIMER);
        }

    }

    public synchronized void stopJobs() {
        if (jobs != null) {
            for (Timer job : jobs.values()) {
                try {
                    job.cancel();
                } catch (RuntimeException e) {
                    log.error(e);
                }
            }
        }
    }

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

}
