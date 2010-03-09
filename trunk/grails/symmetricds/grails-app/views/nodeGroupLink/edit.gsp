<%@ page import="org.jumpmind.symmetric.grails.NodeGroupLink" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <meta name="layout" content="main"/>
  <g:set var="entityName" value="${message(code: 'nodeGroupLink.label', default: 'Link')}"/>
  <title><g:message code="default.edit.label" args="[entityName]"/></title>
</head>
<body>

<div class="body">
<g:render template="/common/editMenu" model="[id:nodeGroupLinkInstance.PKAsCSV]"/>
  <h1><g:message code="default.edit.label" args="[entityName]"/></h1>
  <g:if test="${flash.message}">
    <div class="message">${flash.message}</div>
  </g:if>
  <g:hasErrors bean="${nodeGroupLinkInstance}">
    <div class="errors">
      <g:renderErrors bean="${nodeGroupLinkInstance}" as="list"/>
    </div>
  </g:hasErrors>
  <g:form name="form" controller="nodeGroupLink" action="update" method="post">
    <g:hiddenField name="id" value="${nodeGroupLinkInstance?.id}"/>
    <g:hiddenField name="version" value="${nodeGroupLinkInstance?.version}"/>
    <div class="dialog">
      <table>
        <tbody>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="sourceNodeGroup"><g:message code="nodeGroupLink.sourceNodeGroup.label" default="Source Node Group"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeGroupLinkInstance, field: 'sourceNodeGroup', 'errors')}">
            <g:select name="sourceNodeGroup.nodeGroupId" from="${org.jumpmind.symmetric.grails.NodeGroup.list()}" optionKey="nodeGroupId" optionValue="nodeGroupId" value="${nodeGroupLinkInstance?.sourceNodeGroup?.nodeGroupId}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="targetNodeGroup"><g:message code="nodeGroupLink.targetNodeGroup.label" default="Target Node Group"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeGroupLinkInstance, field: 'targetNodeGroup', 'errors')}">
            <g:select name="targetNodeGroup.nodeGroupId" from="${org.jumpmind.symmetric.grails.NodeGroup.list()}" optionKey="nodeGroupId" optionValue="nodeGroupId" value="${nodeGroupLinkInstance?.targetNodeGroup?.nodeGroupId}"/>
          </td>
        </tr>

        <tr class="prop">
          <td valign="top" class="name">
            <label for="dataEventAction"><g:message code="nodeGroupLink.dataEventAction.label" default="Data Event Action"/></label>
          </td>
          <td valign="top" class="value ${hasErrors(bean: nodeGroupLinkInstance, field: 'dataEventAction', 'errors')}">
            <g:select name="dataEventAction" from="${org.jumpmind.symmetric.model.NodeGroupLinkAction?.values()}" value="${nodeGroupLinkInstance?.dataEventAction}"/>
          </td>
        </tr>

        </tbody>
      </table>
    </div>
  </g:form>
</div>
</body>
</html>
