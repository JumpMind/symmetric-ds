<%@ page import="org.jumpmind.symmetric.grails.Router" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'router.label', default: 'Router')}"/>
  <title><g:message code="default.list.label" args="[entityName]"/></title>
</head>
<body>
<g:render template="/common/listMenu"/>
<div class="body">
  <h1><g:message code="default.list.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="list">
    <table>
      <thead>
      <tr>

        <g:sortableColumn property="routerId" title="${message(code: 'router.routerId.label', default: 'Router Id')}"/>
        <g:sortableColumn property="sourceNodeGroupId" title="${message(code: 'router.sourceNodeGroupId.label', default: 'Source')}"/>

        <g:sortableColumn property="targetNodeGroupId" title="${message(code: 'router.targetNodeGroupId.label', default: 'Target')}"/>

        <g:sortableColumn property="routerType" title="${message(code: 'router.routerType.label', default: 'Router Type')}"/>

        <g:sortableColumn property="syncOnInsert" title="${message(code: 'router.syncOnInsert.label', default: 'Sync On Insert')}"/>

        <g:sortableColumn property="syncOnUpdate" title="${message(code: 'router.syncOnUpdate.label', default: 'Sync On Update')}"/>

        <g:sortableColumn property="syncOnDelete" title="${message(code: 'router.syncOnDelete.label', default: 'Sync On Delete')}"/>

      </tr>
      </thead>
      <tbody>
      <g:each in="${routerInstanceList}" status="i" var="routerInstance">
        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">

          <td><g:link action="show" id="${routerInstance.routerId}">${fieldValue(bean: routerInstance, field: "routerId")}</g:link></td>

          <td>${fieldValue(bean: routerInstance, field: "sourceNodeGroupId")}</td>

          <td>${fieldValue(bean: routerInstance, field: "targetNodeGroupId")}</td>

          <td>${fieldValue(bean: routerInstance, field: "routerType")}</td>

          <td><g:formatBoolean boolean="${routerInstance.syncOnInsert}"/></td>

          <td><g:formatBoolean boolean="${routerInstance.syncOnUpdate}"/></td>

          <td><g:formatBoolean boolean="${routerInstance.syncOnDelete}"/></td>

        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${routerInstanceTotal}"/>
  </div>
</div>
</body>
</html>
