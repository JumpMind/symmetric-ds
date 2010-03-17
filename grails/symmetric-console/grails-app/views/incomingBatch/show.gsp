
<%@ page import="org.jumpmind.symmetric.grails.IncomingBatch" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'incomingBatch.label', default: 'IncomingBatch')}" />
        <title><g:message code="default.show.label" args="[entityName]" /></title>
    </head>
    <body>
        <div class="nav">
            <span class="menuButton"><a class="home" href="${createLink(uri: '/')}">Home</a></span>
            <span class="menuButton"><g:link class="list" action="list"><g:message code="default.list.label" args="[entityName]" /></g:link></span>
            <span class="menuButton"><g:link class="create" action="create"><g:message code="default.new.label" args="[entityName]" /></g:link></span>
        </div>
        <div class="body">
            <h1><g:message code="default.show.label" args="[entityName]" /></h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <div class="dialog">
                <table>
                    <tbody>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.id.label" default="Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "id")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.persistable.label" default="Persistable" /></td>
                            
                            <td valign="top" class="value"><g:formatBoolean boolean="${incomingBatchInstance?.persistable}" /></td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.createTime.label" default="Create Time" /></td>
                            
                            <td valign="top" class="value"><g:formatDate date="${incomingBatchInstance?.createTime}" /></td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.sqlCode.label" default="Sql Code" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "sqlCode")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.filterMillis.label" default="Filter Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "filterMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.batchId.label" default="Batch Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "batchId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.statementCount.label" default="Statement Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "statementCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.nodeId.label" default="Node Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "nodeId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.byteCount.label" default="Byte Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "byteCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.databaseMillis.label" default="Database Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "databaseMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.nodeBatchId.label" default="Node Batch Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "nodeBatchId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.failedRowNumber.label" default="Failed Row Number" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "failedRowNumber")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.lastUpdatedHostName.label" default="Last Updated Host Name" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "lastUpdatedHostName")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.status.label" default="Status" /></td>
                            
                            <td valign="top" class="value">${incomingBatchInstance?.status?.encodeAsHTML()}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.channelId.label" default="Channel Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "channelId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.sqlState.label" default="Sql State" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "sqlState")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.skipCount.label" default="Skip Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "skipCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.sqlMessage.label" default="Sql Message" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "sqlMessage")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.lastUpdatedTime.label" default="Last Updated Time" /></td>
                            
                            <td valign="top" class="value"><g:formatDate date="${incomingBatchInstance?.lastUpdatedTime}" /></td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.fallbackInsertCount.label" default="Fallback Insert Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "fallbackInsertCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.fallbackUpdateCount.label" default="Fallback Update Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "fallbackUpdateCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.networkMillis.label" default="Network Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "networkMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.missingDeleteCount.label" default="Missing Delete Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: incomingBatchInstance, field: "missingDeleteCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="incomingBatch.retry.label" default="Retry" /></td>
                            
                            <td valign="top" class="value"><g:formatBoolean boolean="${incomingBatchInstance?.retry}" /></td>
                            
                        </tr>
                    
                    </tbody>
                </table>
            </div>
            <div class="buttons">
                <g:form>
                    <g:hiddenField name="id" value="${incomingBatchInstance?.id}" />
                    <span class="button"><g:actionSubmit class="edit" action="edit" value="${message(code: 'default.button.edit.label', default: 'Edit')}" /></span>
                    <span class="button"><g:actionSubmit class="delete" action="delete" value="${message(code: 'default.button.delete.label', default: 'Delete')}" onclick="return confirm('${message(code: 'default.button.delete.confirm.message', default: 'Are you sure?')}');" /></span>
                </g:form>
            </div>
        </div>
    </body>
</html>
