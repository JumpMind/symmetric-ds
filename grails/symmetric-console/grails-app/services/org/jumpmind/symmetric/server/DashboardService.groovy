package org.jumpmind.symmetric.server;

import java.util.Calendar;
import java.util.Map;

import groovy.sql.Sql;
import java.sql.Timestamp

class DashboardService {
	boolean transactional = true
	def dataSource
	def dashboardServiceSql
		
	def getThroughput(int interval) {
		Calendar now = Calendar.getInstance()
		List results = []
		
		for (i in 0..9) {
			boolean added = false
			
			Calendar c1 = now.clone()
			Calendar c2 = now.clone()

			int offset = (9-i)*(-1)
			
			c1.add(interval, offset) 
			Date start = c1.getTime()

			
			c2.add(interval, offset + 1)
			Date end = c2.getTime()
			
			def resultSet = new Sql(dataSource).eachRow(dashboardServiceSql.get("selectThroughputSql"), 
					[new Timestamp(start.getTime()), new Timestamp(end.getTime())]) {
				ThroughputBean bean = new ThroughputBean(it.toRowResult())
				bean.begin = start
				bean.end = end
				results.add(bean)
				added = true
			}
			if (!added) {
				ThroughputBean bean = new ThroughputBean()
				bean.begin = start
				bean.end = end
				results.add(bean)
			}
			
		}

		return results
		
	}
}
