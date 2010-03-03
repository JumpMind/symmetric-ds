<%@ page import="org.jumpmind.symmetric.grails.NodeGroup" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'nodeGroup.label', default: 'Group')}"/>
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
        <g:sortableColumn property="nodeGroupId" title="${message(code: 'nodeGroup.nodeGroupId.label', default: 'Node Group Id')}"/>

        <g:sortableColumn property="description" title="${message(code: 'nodeGroup.description.label', default: 'Description')}"/>

      </tr>
      </thead>
      <tbody>
      <g:each in="${nodeGroupInstanceList}" status="i" var="nodeGroupInstance">
        <tr class="${(i % 2) == 0 ? 'odd' : 'even'}">
          <td><g:link action="show" id="${nodeGroupInstance.nodeGroupId}">${fieldValue(bean: nodeGroupInstance, field: "nodeGroupId")}</g:link></td>
          <td>${fieldValue(bean: nodeGroupInstance, field: "description")}</td>
        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${nodeGroupInstanceTotal}"/>
  </div>
</div>
</body>
</html>
