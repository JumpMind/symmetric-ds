<%@ page import="org.jumpmind.symmetric.grails.IncomingBatch" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'incomingBatch.label', default: 'Incoming Batch')}"/>
  <title><g:message code="default.list.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
  <h1><g:message code="default.list.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="list">
    <table>
      <thead>
      <tr>
        <th>Show Data</th>
        <g:sortableColumn property="batchId" title="${message(code: 'incomingBatch.batchId.label', default: 'Batch Id')}"/>
        <g:sortableColumn property="nodeId" title="${message(code: 'incomingBatch.nodeId.label', default: 'Node Id')}"/>
        <g:sortableColumn property="channelId" title="${message(code: 'incomingBatch.channelId.label', default: 'Channel Id')}"/>
        <g:sortableColumn property="status" title="${message(code: 'incomingBatch.status.label', default: 'Status')}"/>
        <g:sortableColumn property="networkMillis" title="${message(code: 'incomingBatch.networkMillis.label', default: 'Network ms')}"/>
        <g:sortableColumn property="filterMillis" title="${message(code: 'incomingBatch.filterMillis.label', default: 'Filter ms')}"/>
      </tr>
      </thead>
      <tbody>
      <g:each in="${incomingBatchInstanceList}" status="i" var="incomingBatchInstance">
        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
          <td><g:link controller="dataEvent" action="list" params="[batchId:incomingBatchInstance.batchId]">view</g:link></td>
          <td><g:link action="show" id="${incomingBatchInstance.batchId}">${incomingBatchInstance.batchId}</g:link></td>
          <td>${fieldValue(bean: incomingBatchInstance, field: "nodeId")}</td>
          <td>${fieldValue(bean: incomingBatchInstance, field: "channelId")}</td>
          <td>${fieldValue(bean: incomingBatchInstance, field: "status")}<div class="metric-${incomingBatchInstance.status} status-box"></div></td>
          <td>${fieldValue(bean: incomingBatchInstance, field: "networkMillis")}</td>
          <td>${fieldValue(bean: incomingBatchInstance, field: "filterMillis")}</td>
        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${incomingBatchInstanceTotal}"/>
  </div>
</div>
</body>
</html>
