<%@ page import="org.jumpmind.symmetric.grails.Node" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'node.label', default: 'Node')}"/>
  <title><g:message code="default.create.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
    <g:render template="/common/createMenu"/>
  <h1><g:message code="default.create.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <g:hasErrors bean="${nodeInstance}">
    <div class="errors">
      <g:renderErrors bean="${nodeInstance}" as="list"/>
    </div>
  </g:hasErrors>
  <g:form action="save" method="post">
    <div class="dialog">
      <table>
        <tbody>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="nodeId"><g:message code="node.nodeId.label" default="Node Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'nodeId', 'errors')}">
            <g:textField name="nodeId" value="${nodeInstance?.nodeId}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="schemaVersion"><g:message code="node.schemaVersion.label" default="Schema Version"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'schemaVersion', 'errors')}">
            <g:textField name="schemaVersion" value="${nodeInstance?.schemaVersion}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="databaseType"><g:message code="node.databaseType.label" default="Database Type"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'databaseType', 'errors')}">
            <g:textField name="databaseType" value="${nodeInstance?.databaseType}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="timezoneOffset"><g:message code="node.timezoneOffset.label" default="Timezone Offset"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'timezoneOffset', 'errors')}">
            <g:textField name="timezoneOffset" value="${nodeInstance?.timezoneOffset}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="symmetricVersion"><g:message code="node.symmetricVersion.label" default="Symmetric Version"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'symmetricVersion', 'errors')}">
            <g:textField name="symmetricVersion" value="${nodeInstance?.symmetricVersion}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncEnabled"><g:message code="node.syncEnabled.label" default="Sync Enabled"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'syncEnabled', 'errors')}">
            <g:checkBox name="syncEnabled" value="${nodeInstance?.syncEnabled}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="nodeGroupId"><g:message code="node.nodeGroupId.label" default="Node Group Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'nodeGroupId', 'errors')}">
            <g:textField name="nodeGroupId" value="${nodeInstance?.nodeGroupId}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="batchInErrorCount"><g:message code="node.batchInErrorCount.label" default="Batch In Error Count"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'batchInErrorCount', 'errors')}">
            <g:textField name="batchInErrorCount" value="${fieldValue(bean: nodeInstance, field: 'batchInErrorCount')}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="batchToSendCount"><g:message code="node.batchToSendCount.label" default="Batch To Send Count"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'batchToSendCount', 'errors')}">
            <g:textField name="batchToSendCount" value="${fieldValue(bean: nodeInstance, field: 'batchToSendCount')}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="databaseVersion"><g:message code="node.databaseVersion.label" default="Database Version"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'databaseVersion', 'errors')}">
            <g:textField name="databaseVersion" value="${nodeInstance?.databaseVersion}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="heartbeatTime"><g:message code="node.heartbeatTime.label" default="Heartbeat Time"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'heartbeatTime', 'errors')}">
            <g:datePicker name="heartbeatTime" precision="day" value="${nodeInstance?.heartbeatTime}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="externalId"><g:message code="node.externalId.label" default="External Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'externalId', 'errors')}">
            <g:textField name="externalId" value="${nodeInstance?.externalId}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="syncUrl"><g:message code="node.syncUrl.label" default="Sync Url"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'syncUrl', 'errors')}">
            <g:textField name="syncUrl" value="${nodeInstance?.syncUrl}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="createdAtNodeId"><g:message code="node.createdAtNodeId.label" default="Created At Node Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeInstance, field: 'createdAtNodeId', 'errors')}">
            <g:textField name="createdAtNodeId" value="${nodeInstance?.createdAtNodeId}"/>
          </td>
        </tr>

        </tbody>
      </table>
    </div>
    <div class="buttons">
      <span class="button"><g:submitButton name="create" class="save" value="${message(code: 'default.button.create.label', default: 'Create')}"/></span>
    </div>
  </g:form>
</div>
</body>
</html>
