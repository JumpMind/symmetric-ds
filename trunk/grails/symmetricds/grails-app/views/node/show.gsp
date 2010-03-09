<%@ page import="org.jumpmind.symmetric.grails.Node" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'node.label', default: 'Node')}"/>
  <title><g:message code="default.show.label" args="[entityName]"/></title>
</head>
<body>
<div class="body">
  <h1><g:message code="default.show.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="dialog">
    <table>
      <tbody>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.nodeId.label" default="Node Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "nodeId")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.externalId.label" default="External Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "externalId")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.nodeGroupId.label" default="Node Group Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "nodeGroupId")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.syncUrl.label" default="Sync Url"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "syncUrl")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.symmetricVersion.label" default="Symmetric Version"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "symmetricVersion")}</td>

      </tr>



      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.databaseType.label" default="Database Type"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "databaseType")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.databaseVersion.label" default="Database Version"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "databaseVersion")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.schemaVersion.label" default="Schema Version"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "schemaVersion")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.syncEnabled.label" default="Sync Enabled"/></td>

        <td valign="top" class="value"><g:formatBoolean boolean="${nodeInstance?.syncEnabled}"/></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.batchInErrorCount.label" default="Batch In Error Count"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "batchInErrorCount")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.batchToSendCount.label" default="Batch To Send Count"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "batchToSendCount")}</td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.timezoneOffset.label" default="Timezone Offset"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "timezoneOffset")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.heartbeatTime.label" default="Heartbeat Time"/></td>

        <td valign="top" class="value"><g:formatDate date="${nodeInstance?.heartbeatTime}"/></td>

      </tr>


      <tr class="prop">
        <td valign="top" class="name"><g:message code="node.createdAtNodeId.label" default="Created At Node Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: nodeInstance, field: "createdAtNodeId")}</td>

      </tr>

      </tbody>
    </table>
  </div>
</div>
</body>
</html>
