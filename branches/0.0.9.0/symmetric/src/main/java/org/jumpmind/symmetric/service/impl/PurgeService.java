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

package org.jumpmind.symmetric.service.impl;

import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IPurgeService;

/**
 * Simplifying assumptions:
 * <ul>
 * <li>This will run once a day.</li>
 * <li>Therefore all the queries can be run daily without issue.</li>
 * <li>This will only run in a single node environment. (It will probably be
 * ok in a multi-node env., but everything will be run "n" times)</li>
 * <li></li>
 * </li>
 * Future enhancements:
 * <ul>
 * <li>Could add fancy scheduling, but if we need to do that maybe we should
 * use Quartz. Put another way, if we need fancy scheduling, it should be done outside
 * of symmetric.</li>
 * <li>The leasing service could easily be added to ensure that this runs
 * "properly" in a clustered environment.</li>
 * </ul>
 * 
 * @author awilcox
 *
 */
public class PurgeService extends AbstractService implements IPurgeService {

    private final static Log logger = LogFactory.getLog(PurgeService.class);

    private String[] purgeSql;

    private int retentionInMinutes = 7200;

    public void purge() {
        logger.info("The symmetric purge process is about to run.");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, -retentionInMinutes);
        for (String sql : purgeSql) {
            int count = jdbcTemplate.update(sql, new Object[] { calendar
                    .getTime() });
            if (count > 0) {
                logger.info("Purged " + count + " rows after running: " + cleanSql(sql));
            }
        }
    }
    
    private String cleanSql(String sql) {
        return sql.replace('\r', ' ').replace('\n', ' ').trim();
    }

    public void setPurgeSql(String[] purgeSql) {
        this.purgeSql = purgeSql;
    }

    public void setRetentionInMinutes(int retentionInMinutes) {
        this.retentionInMinutes = retentionInMinutes;
    }

}
