<%@ page import="org.jumpmind.symmetric.grails.Node" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'node.label', default: 'Node')}"/>
  <title><g:message code="default.list.label" args="[entityName]"/></title>
  <g:javascript library="jquery"/>
  <link rel="stylesheet" href="${resource(dir: 'css', file: 'jquery.treeTable.css')}"/>
  <script type="text/javascript" src="${resource(dir: 'js/jquery', file: 'jquery.treeTable.js')}"></script>

  <script type="text/javascript">

    $(document).ready(function() {
      $(".tree").treeTable({
        initialState: "collapsed"
      });
    });

  </script>

</head>
<body>
<div class="body">
  <h1><g:message code="default.list.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <div class="list">
    <table class="tree">
      <thead>
      <tr>

        <th>${message(code: 'node.nodeId.label', default: 'Id')}</th>

        <th>${message(code: 'node.externalId.label', default: 'External Id')}</th>

        <th>${message(code: 'node.nodeGroupId.label', default: 'Group')}</th>

        <th>${message(code: 'node.databaseType.label', default: 'Database')}</th>

        <th>${message(code: 'node.timezoneOffset.label', default: 'Timezone')}</th>

        <th>${message(code: 'node.heartbeatTime.label', default: 'Heartbeat')}</th>
        
        <th>${message(code: 'node.batchStatus.label', default: 'Batch Status')}</th>
        
        
      </tr>
      </thead>
      <tbody>
      <g:each in="${nodeInstanceList}" status="i" var="nodeInstance">
        <g:if test="${nodeInstance.nodeId != nodeInstance.createdAtNodeId && nodeInstance.createdAtNodeId && !params.nodeGroupId}">
          <tr id="${nodeInstance.nodeId}" class="child-of-${nodeInstance.createdAtNodeId}"></g:if>
        <g:else>
          <tr id="${nodeInstance.nodeId}"></g:else>


        <td><g:link action="show" id="${nodeInstance.nodeId}">${fieldValue(bean: nodeInstance, field: "nodeId")}</g:link></td>

        <td>${fieldValue(bean: nodeInstance, field: "externalId")}</td>

        <td>${fieldValue(bean: nodeInstance, field: "nodeGroupId")}</td>

        <td>${fieldValue(bean: nodeInstance, field: "databaseType")}</td>

        <td>${fieldValue(bean: nodeInstance, field: "timezoneOffset")}</td>

        <td>${fieldValue(bean: nodeInstance, field: "heartbeatTime")}</td>
        
        <td><div class="metric-${nodeInstance.batchStatus} status-box"></div></td>
        
        </tr>
      </g:each>
      </tbody>
    </table>
  </div>
  <div class="paginateButtons">
    <g:paginate total="${nodeInstanceTotal}"/>
  </div>
</div>
</body>
</html>
