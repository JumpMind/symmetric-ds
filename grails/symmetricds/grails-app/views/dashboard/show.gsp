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
    <div id="panel-overview" class="panel">
        <h3>Overview</h3>
	    <div class="panel-row">
	       <span class="stat">
	          <g:if test="${command.started}">
                <img src="${resource(dir: 'images', file: 'arrow_up.png')}" alt="Yes" border="0"/>
              </g:if>
              <g:else>
                <img src="${resource(dir: 'images', file: 'arrow_down.png')}" alt="No" border="0"/>
              </g:else>
	       </span>
	       <g:message code="symmetric.running.label" default="Running"/>
	    </div>
	    <div class="panel-row alt">
	       <span class="stat"><g:link controller="nodeGroup" action="show" id="${command.nodeGroupId}">${command.nodeGroupId}</g:link></span>
	       <g:message code="symmetric.node.group.label" default="Group"/>
	    </div>      
	    <div class="panel-row">  
	       <span class="stat"><g:link controller="node" action="show" id="${command.nodeId}">${command.nodeId}</g:link></span>
	       <g:message code="symmetric.node.id.label" default="Node Id"/>
	    </div> 
	    <div class="panel-row alt">  
           <span class="stat"><g:link controller="node" action="list">${command.numberOfNodes}</g:link></span>
           <g:message code="symmetric.number.of.nodes.label" default="Number Of Nodes"/>
        </div>
        <div class="panel-row">  
           <span class="stat"><g:link controller="node" action="list">${command.numberOfClients}</g:link></span>
           <g:message code="symmetric.number.of.clients.label" default="Number Of Clients"/>
        </div>
    </div>
    <div id="panel-nodes" class="panel">
        <h3>Nodes</h3>
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
	              <g:each in="${command.nodeHosts}" status="i" var="nodeHost">
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
</div>
</body>
</html>
