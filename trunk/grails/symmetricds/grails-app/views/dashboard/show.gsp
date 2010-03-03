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
  <div class="dialog">
    <table>
      <tbody>
      <tr class="prop">
        <td valign="top" class="name">
          <g:message code="symmetric.running.label" default="Running"/>
          </td>
        <td valign="top" class="value">
          <g:if test="${command.started}">
            <img src="${resource(dir: 'images', file: 'arrow_up.png')}" alt="Yes" border="0"/>
          </g:if>
          <g:else>
            <img src="${resource(dir: 'images', file: 'arrow_down.png')}" alt="No" border="0"/>
          </g:else>
        </td>
      </tr>
      <tr class="prop">
        <td valign="top" class="name"><g:message code="symmetric.node.group.label" default="Group"/></td>
        <td valign="top" class="value">
          <g:link controller="nodeGroup" action="show" id="${command.nodeGroupId}">${command.nodeGroupId}</g:link>
        </td>
      </tr>
      <tr class="prop">
        <td valign="top" class="name"><g:message code="symmetric.node.id.label" default="Node Id"/></td>
        <td valign="top" class="value">
          <g:link controller="node" action="show" id="${command.nodeId}">${command.nodeId}</g:link>
        </td>
      </tr>
      <tr class="prop">
        <td valign="top" class="name"><g:message code="symmetric.number.of.nodes.label" default="Number Of Nodes"/></td>
        <td valign="top" class="value">
          <g:link controller="node" action="list">${command.numberOfNodes}</g:link>
        </td>
      </tr>
      <tr class="prop">
        <td valign="top" class="name"><g:message code="symmetric.number.of.clients.label" default="Number Of Clients"/></td>
        <td valign="top" class="value">
          <g:link controller="node" action="list">${command.numberOfClients}</g:link>
        </td>
      </tr>
<tr class="prop">
        <td valign="top" class="name" colspan="2">
          <div class="list">
            <table>
              <thead>
              <!--
    private java.lang.String nodeId;
    private java.lang.String hostName;
    private java.lang.String ipAddress;
    private java.lang.String osUser;
    private java.lang.String osName;
    private java.lang.String osArch;
    private java.lang.String osVersion;
    private int availableProcessors;
    private long freeMemoryBytes;
    private long totalMemoryBytes;
    private long maxMemoryBytes;
    private java.lang.String javaVersion;
    private java.lang.String javaVendor;
    private java.lang.String symmetricVersion;
    private java.lang.String timezoneOffset;
    private java.util.Date heartbeatTime;
    private java.util.Date lastRestartTime;
    private java.util.Date createTime;
              -->
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
        </td>
      </tr>
      </tbody>
    </table>
  </div>
</div>
</body>
</html>
