
<%@ page import="org.jumpmind.symmetric.grails.DataEvent" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'dataEvent.label', default: 'DataEvent')}" />
        <title><g:message code="default.edit.label" args="[entityName]" /></title>
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"><a class="home" href="${createLink(uri: '/')}">Home</a></span>
            <span class="menuButton"><g:link class="list" action="list"><g:message code="default.list.label" args="[entityName]" /></g:link></span>
            <span class="menuButton"><g:link class="create" action="create"><g:message code="default.new.label" args="[entityName]" /></g:link></span>
        </div>
        <div class="body">
            <h1><g:message code="default.edit.label" args="[entityName]" /></h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <g:hasErrors bean="${dataEventInstance}">
            <div class="errors">
                <g:renderErrors bean="${dataEventInstance}" as="list" />
            </div>
            </g:hasErrors>
            <g:form method="post" >
                <g:hiddenField name="id" value="${dataEventInstance?.id}" />
                <g:hiddenField name="version" value="${dataEventInstance?.version}" />
                <div class="dialog">
                    <table>
                        <tbody>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                  <label for="data"><g:message code="dataEvent.data.label" default="Data" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: dataEventInstance, field: 'data', 'errors')}">
                                    <g:select name="data.id" from="${org.jumpmind.symmetric.grails.Data.list()}" optionKey="id" value="${dataEventInstance?.data?.id}"  />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                  <label for="routerId"><g:message code="dataEvent.routerId.label" default="Router Id" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: dataEventInstance, field: 'routerId', 'errors')}">
                                    <g:textField name="routerId" value="${dataEventInstance?.routerId}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                  <label for="dataId"><g:message code="dataEvent.dataId.label" default="Data Id" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: dataEventInstance, field: 'dataId', 'errors')}">
                                    <g:textField name="dataId" value="${fieldValue(bean: dataEventInstance, field: 'dataId')}" />
                                </td>
                            </tr>
                        
                            <tr class="prop">
                                <td valign="top" class="name">
                                  <label for="batchId"><g:message code="dataEvent.batchId.label" default="Batch Id" /></label>
                                </td>
                                <td valign="top" class="value ${hasErrors(bean: dataEventInstance, field: 'batchId', 'errors')}">
                                    <g:textField name="batchId" value="${fieldValue(bean: dataEventInstance, field: 'batchId')}" />
                                </td>
                            </tr>
                        
                        </tbody>
                    </table>
                </div>
                <div class="buttons">
                    <span class="button"><g:actionSubmit class="save" action="update" value="${message(code: 'default.button.update.label', default: 'Update')}" /></span>
                    <span class="button"><g:actionSubmit class="delete" action="delete" value="${message(code: 'default.button.delete.label', default: 'Delete')}" onclick="return confirm('${message(code: 'default.button.delete.confirm.message', default: 'Are you sure?')}');" /></span>
                </div>
            </g:form>
        </div>
    </body>
</html>
