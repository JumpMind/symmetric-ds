<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:helpBalloons/>
  <title><g:message code="symmetric.dashboard.show.label" default="Dashboard"/></title>
</head>
<body>
<div class="body">
  <h1><g:message code="symmetric.dashboard.show.label" default="Dashboard"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div id="primary" class="column">
    <div id="panel-nodes" class="panel">
        <h3>Node Instances</h3>
        <div class="panel-row"> 
	       <div class="list">
	            <table width="100%">
	              <thead>
	              <tr>
	                <th>${message(code: 'symmetric.dashboard.host.hostname.label', default: 'Host Name')}</th>
	                <th>${message(code: 'symmetric.dashboard.host.ipaddress.label', default: 'IP Address')}</th>
	                <th>${message(code: 'symmetric.dashboard.host.javaversion.label', default: 'SymmetricDS')}</th>
	                <th>${message(code: 'symmetric.dashboard.host.osname.label', default: 'OS')}</th>
	                <th>${message(code: 'symmetric.dashboard.host.javaversion.label', default: 'Java')}</th>
	                <th>${message(code: 'symmetric.dashboard.host.restarttime.label', default: 'Last Restart')}</th>
	                <th>${message(code: 'symmetric.dashboard.host.heartbeattime.label', default: 'Heartbeat')}</th>
	                <th>${message(code: 'symmetric.dashboard.host.createtime.label', default: 'Created')}</th>
	              </tr>
	              </thead>
	              <tbody>
	              <g:each in="${session.overview?.nodeHosts}" status="i" var="nodeHost">
	                <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
	                  <td>
	                    ${nodeHost?.hostName}
	                  </td>
	                  <td>
	                    ${nodeHost?.ipAddress}
	                  </td>
	                  <td>
	                    ${nodeHost?.symmetricVersion}
	                  </td>
	                  <td>
	                    ${nodeHost?.osName}
	                  </td>
	                  <td>
	                    ${nodeHost?.javaVersion}
	                  </td>
	                  <td>
	                    ${nodeHost?.lastRestartTime}
	                  </td>
	                  <td>
	                    ${nodeHost?.heartbeatTime} ${nodeHost?.timezoneOffset}
	                  </td>
	                  <td>
	                    ${nodeHost?.createTime}
	                  </td>
	                </tr>
	              </g:each>
	              </tbody>
	            </table>
	          </div>
	       </div>
        </div>
    </div>
    <div id="secondary" class="column">
        <div id="panel-overview" class="panel">
            <h3>My Outgoing Batches</h3>
            <div class="panel-row">
                <g:link controller="outgoingBatch" action="list" params="[status:'ER']">All batches in error</g:link>
            </div>
            <div class="panel-row">
               
                <table class="metrics">
                  <thead>
                    <tr>
                        <th>Nodes</th>
                        <th>Metrics</th>
                        <th>Total Batches</th>
                    </tr>
                  </thead>
                  <tbody>
                    <g:each in="${outBatches}" var="b">
                    <tr>
                        <td><g:link controller="outgoingBatch" action="list" params="[nodeId:b.nodeId]">${b.nodeLabel}</g:link></td>
                        <td>
		                      <table class="metricsBar" valign="middle" align="center" cellspacing="0" cellpadding="0" border="0"> 
		                          <tr>
		                          <% percentTotal = 0 %>
		                          <g:each in="${b.statusList}" var="s" status="i">
		                              <% 
									  percent = java.lang.Math.round(b.statusListCount.get(i) / maxOutBatch * 100)
		                              title = "${s} - ${percent}% (${b.statusListCount.get(i)} batches)"
		                              %>
		                              <td width="${percent}%" class="metric-${s}"><g:link title="${title}" controller="outgoingBatch" action="list" params="[nodeId:b.nodeId,status:s]"><img  alt="${title}" class="hideOnPrint" src="../images/bar.gif" height="10" width="100%" border="0"/></g:link></td>
		                              <%percentTotal += percent%>
                                  </g:each>
                                  <g:if test="${percentTotal < 100}">
                                      <td width="${100 - percentTotal}" class="metric-empty"><img class="hideOnPrint" src="../images/bar.gif" height="10" width="100%" border="0"/></td>
                                  </g:if>
		                          </tr>
		                      </table>
                        </td>
                        <td style="text-align:center;">${b.totalBatches}</td>
                    </tr>
                    </g:each>
                  </tbody>
                  </table>
            </div>
        </div> 

        <div id="panel-overview" class="panel">
            <h3>My Incoming Batches</h3>
            <div class="panel-row">
                <g:link controller="incomingBatch" action="list" params="[status:'ER']">All batches in error</g:link>
            </div>
            <div class="panel-row">
               
                <table class="metrics">
                  <thead>
                    <tr>
                        <th>Nodes</th>
                        <th>Metrics</th>
                        <th>Total Batches</th>
                    </tr>
                  </thead>
                  <tbody>
                    <g:each in="${inBatches}" var="b">
                    <tr>
                        <td><g:link controller="incomingBatch" action="list" params="[nodeId:b.nodeId]">${b.nodeLabel}</g:link></td>
                        <td>
                              <table class="metricsBar" valign="middle" align="center" cellspacing="0" cellpadding="0" border="0"> 
                                  <tr>
                                  <% percentTotal = 0 %>
                                  <g:each in="${b.statusList}" var="s" status="i">
                                      <% 
                                      percent = java.lang.Math.round(b.statusListCount.get(i) / maxInBatch * 100)
                                      title = "${s} - ${percent}% (${b.statusListCount.get(i)} batches)"
                                      %>
                                      <td width="${percent}%" class="metric-${s}"><g:link title="${title}" controller="incomingBatch" action="list" params="[nodeId:b.nodeId,status:s]"><img  alt="${title}" class="hideOnPrint" src="../images/bar.gif" height="10" width="100%" border="0"/></g:link></td>
                                      <%percentTotal += percent%>
                                  </g:each>
                                  <g:if test="${percentTotal < 100}">
                                      <td width="${100 - percentTotal}" class="metric-empty"><img class="hideOnPrint" src="../images/bar.gif" height="10" width="100%" border="0"/></td>
                                  </g:if>
                                  </tr>
                              </table>
                        </td>
                        <td style="text-align:center;">${b.totalBatches}</td>
                    </tr>
                    </g:each>
                  </tbody>
                  </table>
            </div>
        </div>   
        <div id="panel-overview" class="panel">
            <h3>My Node Groups</h3>
            <div class="panel-row">
                <table class="metrics">
                  <thead>
                    <tr>
                        <th>Nodes</th>
                        <th>Metrics</th>
                        <th>Total Nodes</th>
                    </tr>
                  </thead>
                  <tbody>
                    <g:each in="${myNodeGroups}" var="g">
                    <%totalNodes = org.jumpmind.symmetric.grails.Node.count() %>
                    <tr>
                        <td>${g.key}</td>
                        <td>
                              <table class="metricsBar" valign="middle" align="center" cellspacing="0" cellpadding="0" border="0"> 
                                  <tr>
                                  <% percentTotal = 0%>
                                  <g:each in="${g.value}" var="s" status="i">
                                      <% 
                                      percent = java.lang.Math.round(s.value / totalNodes * 100)
                                      title = "${s.key} - ${percent}% (${s.value} nodes)"
                                      %>
                                      <td width="${percent}%" class="metric-${s.key}"><img title="${title}" alt="${title}" class="hideOnPrint" src="../images/bar.gif" height="10" width="100%" border="0"/></td>
                                      <%percentTotal += percent%>
                                  </g:each>
                                  <g:if test="${percentTotal < 100}">
                                      <td width="${100 - percentTotal}" class="metric-empty"><img class="hideOnPrint" src="../images/bar.gif" height="10" width="100%" border="0"/></td>
                                  </g:if>
                                  </tr>
                              </table>
                        </td>
                        <td style="text-align:center;"><%=g.value.collect{ it.value }.sum()%></td>
                    </tr>
                    </g:each>
                  </tbody>
                  </table>
            </div>
        </div> 
         
    </div>
</div>

</body>
</html>
