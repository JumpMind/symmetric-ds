<%@ page import="org.jumpmind.symmetric.grails.Channel" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'channel.label', default: 'Channel')}"/>
  <title><g:message code="default.list.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
<g:render template="/common/listMenu"/>
  <h1><g:message code="default.list.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="list">
    <table>
      <thead>
      <tr>
        <g:sortableColumn property="channelId" title="${message(code: 'channel.channelId.label', default: 'Channel Id')}"/>

        <g:sortableColumn property="processingOrder" title="${message(code: 'channel.processingOrder.label', default: 'Processing Order')}"/>

        <g:sortableColumn property="enabled" title="${message(code: 'channel.enabled.label', default: 'Enabled')}"/>

        <g:sortableColumn property="maxBatchToSend" title="${message(code: 'channel.maxBatchToSend.label', default: 'Max Batch To Send')}"/>

        <g:sortableColumn property="maxBatchSize" title="${message(code: 'channel.maxBatchToSend.label', default: 'Max Batch Size')}"/>

        <g:sortableColumn property="batchAlgorithm" title="${message(code: 'channel.batchAlgorithm.label', default: 'Batch Algorithm')}"/>

        <g:sortableColumn property="extractPeriodMillis" title="${message(code: 'channel.extractPeriodMillis.label', default: 'Extract Period Millis')}"/>

      </tr>
      </thead>
      <tbody>
      <g:each in="${channelInstanceList}" status="i" var="channelInstance">
        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">

          <td><g:link action="show" id="${channelInstance.channelId}">${fieldValue(bean: channelInstance, field: "channelId")}</g:link></td>

          <td>${fieldValue(bean: channelInstance, field: "processingOrder")}</td>

          <td><g:formatBoolean boolean="${channelInstance.enabled}"/></td>

          <td>${fieldValue(bean: channelInstance, field: "maxBatchToSend")}</td>

          <td>${fieldValue(bean: channelInstance, field: "maxBatchSize")}</td>

          <td>${fieldValue(bean: channelInstance, field: "batchAlgorithm")}</td>

          <td>${fieldValue(bean: channelInstance, field: "extractPeriodMillis")}</td>

        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${channelInstanceTotal}"/>
  </div>
</div>
</body>
</html>
