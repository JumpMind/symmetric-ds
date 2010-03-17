<%@ page import="org.jumpmind.symmetric.grails.Channel" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'channel.label', default: 'Channel')}"/>
  <title><g:message code="default.edit.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
<g:render template="/common/editMenu" model="[id:channelInstance.channelId]"/>
  <h1><g:message code="default.edit.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <g:hasErrors bean="${channelInstance}">
    <div class="errors">
      <g:renderErrors bean="${channelInstance}" as="list"/>
    </div>
  </g:hasErrors>
  <g:form name="form" controller="channel" action="update" method="post">
    <g:hiddenField name="id" value="${channelInstance?.channelId}"/>
    <g:hiddenField name="version" value="${channelInstance?.version}"/>
    <div class="dialog">
      <table>
        <tbody>
        <tr class="prop">
          <td valign="top" class="name">
            <label for="channelId"><g:message code="channel.channelId.label" default="Channel Id"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: channelInstance, field: 'channelId', 'errors')}">
            <g:hiddenField name="channelId" value="${channelInstance?.channelId}"/>
            ${channelInstance?.channelId}
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="enabled"><g:message code="channel.enabled.label" default="Enabled"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: channelInstance, field: 'enabled', 'errors')}">
            <g:checkBox name="enabled" value="${channelInstance?.enabled}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="processingOrder"><g:message code="channel.processingOrder.label" default="Processing Order"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: channelInstance, field: 'processingOrder', 'errors')}">
            <g:textField name="processingOrder" value="${fieldValue(bean: channelInstance, field: 'processingOrder')}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="batchAlgorithm"><g:message code="channel.batchAlgorithm.label" default="Batch Algorithm"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: channelInstance, field: 'batchAlgorithm', 'errors')}">
            <g:select name="batchAlgorithm" from="${batchAlgorithms}" optionKey="key" optionValue="key" value="${channelInstance?.batchAlgorithm}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="maxBatchToSend"><g:message code="channel.maxBatchToSend.label" default="Max Batch To Send"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: channelInstance, field: 'maxBatchToSend', 'errors')}">
            <g:textField name="maxBatchToSend" value="${fieldValue(bean: channelInstance, field: 'maxBatchToSend')}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="maxBatchSize"><g:message code="channel.maxBatchSize.label" default="Max Batch Size"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: channelInstance, field: 'maxBatchSize', 'errors')}">
            <g:textField name="maxBatchSize" value="${fieldValue(bean: channelInstance, field: 'maxBatchSize')}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="extractPeriodMillis"><g:message code="channel.extractPeriodMillis.label" default="Extract Period Millis"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: channelInstance, field: 'extractPeriodMillis', 'errors')}">
            <g:textField name="extractPeriodMillis" value="${fieldValue(bean: channelInstance, field: 'extractPeriodMillis')}"/>
          </td>
        </tr>

        </tbody>
      </table>
    </div>
  </g:form>
</div>
</body>
</html>
