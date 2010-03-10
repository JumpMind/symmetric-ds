<%@ page import="org.jumpmind.symmetric.grails.OutgoingBatch" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'outgoingBatch.label', default: 'Outgoing Batch')}"/>
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
        <g:sortableColumn property="batchId" title="${message(code: 'outgoingBatch.batchId.label', default: 'Batch Id')}"/>
        <g:sortableColumn property="nodeId" title="${message(code: 'outgoingBatch.nodeId.label', default: 'Node Id')}"/>
        <g:sortableColumn property="channelId" title="${message(code: 'outgoingBatch.channelId.label', default: 'Channel Id')}"/>
        <g:sortableColumn property="status" title="${message(code: 'outgoingBatch.status.label', default: 'Status')}"/>
        <g:sortableColumn property="dataEventCount" title="${message(code: 'outgoingBatch.dataEventCount.label', default: 'Data Event Count')}"/>
        <g:sortableColumn property="routerMillis" title="${message(code: 'outgoingBatch.routerMillis.label', default: 'Router ms')}"/>
        <g:sortableColumn property="networkMillis" title="${message(code: 'outgoingBatch.networkMillis.label', default: 'Network ms')}"/>
        <g:sortableColumn property="filterMillis" title="${message(code: 'outgoingBatch.filterMillis.label', default: 'Filter ms')}"/>
        <g:sortableColumn property="loadMillis" title="${message(code: 'outgoingBatch.loadMillis.label', default: 'Load ms')}"/>
        <g:sortableColumn property="extractMillis" title="${message(code: 'outgoingBatch.extractMillis.label', default: 'Extract ms')}"/>

      </tr>
      </thead>
      <tbody>
      <g:each in="${outgoingBatchInstanceList}" status="i" var="outgoingBatchInstance">
        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
          <td><g:link action="list" id="${outgoingBatchInstance.batchId}">view</g:link></td>
          <td><g:link action="show" id="${outgoingBatchInstance.batchId}">${fieldValue(bean: outgoingBatchInstance, field: "batchId")}</g:link></td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "nodeId")}</td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "channelId")}</td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "status")}<div class="metric-${outgoingBatchInstance.status} status-box"></div></td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "dataEventCount")}</td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "routerMillis")}</td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "networkMillis")}</td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "filterMillis")}</td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "loadMillis")}</td>
          <td>${fieldValue(bean: outgoingBatchInstance, field: "extractMillis")}</td>
        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${outgoingBatchInstanceTotal}"/>
  </div>
</div>
</body>
</html>
