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
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;

abstract public class AbstractJob extends TimerTask implements BeanFactoryAware, BeanNameAware
{

    DataSource dataSource;

    private boolean needsRescheduled;

    private long rescheduleDelay;

    private BeanFactory beanFactory;

    private String beanName;

    @Override
    public void run()
    {
        try
        {
            doJob();
        }
        catch (final Throwable ex)
        {
            getLogger().error(ex, ex);
        }
        finally
        {
            if (isNeedsRescheduled())
            {
                reschedule();
            }
		}
    }

    abstract void doJob() throws Exception;

    abstract Log getLogger();

    protected void reschedule()
    {
        final Timer timer = new Timer();
        timer.schedule((TimerTask) beanFactory.getBean(beanName), getRescheduleDelay());
        if (getLogger().isDebugEnabled())
        {
            getLogger().debug("Rescheduling " + beanName + " with " + getRescheduleDelay() + " ms delay.");
        }
    }

    protected void printDatabaseStats()
    {
        if (getLogger().isDebugEnabled() && dataSource instanceof BasicDataSource)
        {
            final BasicDataSource ds = (BasicDataSource) dataSource;
            getLogger().debug("There are currently " + ds.getNumActive() + " active database connections.");
        }
    }

    public void setBeanFactory(final BeanFactory beanFactory)
    {
        this.beanFactory = beanFactory;
    }

    public void setBeanName(final String beanName)
    {
        this.beanName = beanName;
    }

    public void setDataSource(final DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    public boolean isNeedsRescheduled()
    {
        return needsRescheduled;
    }

    public void setNeedsRescheduled(final boolean needsRescheduled)
    {
        this.needsRescheduled = needsRescheduled;
    }

    public void setRescheduleDelay(final long rescheduleDelay)
    {
        this.rescheduleDelay = rescheduleDelay;
    }

    public long getRescheduleDelay()
    {
        return rescheduleDelay;
    }

}
