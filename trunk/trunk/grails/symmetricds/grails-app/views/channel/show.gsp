<%@ page import="org.jumpmind.symmetric.grails.Channel" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'channel.label', default: 'Channel')}"/>
  <title><g:message code="default.show.label" args="[entityName]"/></title>
</head>
<body>
<g:render template="/common/showMenu" model="[id:channelInstance.channelId]"/>
<div class="body">
  <h1><g:message code="default.show.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="dialog">
    <table>
      <tbody>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="channel.channelId.label" default="Channel Id"/></td>

        <td valign="top" class="value">${fieldValue(bean: channelInstance, field: "channelId")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="channel.processingOrder.label" default="Processing Order"/></td>

        <td valign="top" class="value">${fieldValue(bean: channelInstance, field: "processingOrder")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="channel.enabled.label" default="Enabled"/></td>

        <td valign="top" class="value"><g:formatBoolean boolean="${channelInstance?.enabled}"/></td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="channel.batchAlgorithm.label" default="Batch Algorithm"/></td>

        <td valign="top" class="value">${fieldValue(bean: channelInstance, field: "batchAlgorithm")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="channel.maxBatchToSend.label" default="Max Batch To Send"/></td>

        <td valign="top" class="value">${fieldValue(bean: channelInstance, field: "maxBatchToSend")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="channel.maxBatchSize.label" default="Max Batch Size"/></td>

        <td valign="top" class="value">${fieldValue(bean: channelInstance, field: "maxBatchSize")}</td>

      </tr>

      <tr class="prop">
        <td valign="top" class="name"><g:message code="channel.extractPeriodMillis.label" default="Extract Period Millis"/></td>

        <td valign="top" class="value">${fieldValue(bean: channelInstance, field: "extractPeriodMillis")}</td>

      </tr>

      </tbody>
    </table>
  </div>
</div>
</body>
</html>
