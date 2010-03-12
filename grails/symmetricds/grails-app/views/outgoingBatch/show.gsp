
<%@ page import="org.jumpmind.symmetric.grails.OutgoingBatch" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'outgoingBatch.label', default: 'OutgoingBatch')}" />
        <title><g:message code="default.show.label" args="[entityName]" /></title>
    </head>
    <body>
        <div class="body">
            <h1><g:message code="default.show.label" args="[entityName]" /></h1>
            <g:if test="${flash.message}">
            <div class="message">${flash.message}</div>
            </g:if>
            <div class="dialog">
                <table>
                    <tbody>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.id.label" default="Batch Id" /></td>
                            
                            <td valign="top" class="value">${outgoingBatchInstance.batchId}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.nodeId.label" default="Node Id" /></td>
                            
                            <td valign="top" class="value"><g:link controller="node" action="show" id="${outgoingBatchInstance.nodeId}">${fieldValue(bean: outgoingBatchInstance, field: "nodeId")}</g:link></td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.channelId.label" default="Channel Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "channelId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.status.label" default="Status" /></td>
                            
                            <td valign="top" class="value">${outgoingBatchInstance?.status?.encodeAsHTML()}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.routerMillis.label" default="Router Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "routerMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.networkMillis.label" default="Network Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "networkMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.filterMillis.label" default="Filter Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "filterMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.loadMillis.label" default="Load Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "loadMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.extractMillis.label" default="Extract Millis" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "extractMillis")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.byteCount.label" default="Byte Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "byteCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.sentCount.label" default="Sent Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "sentCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.dataEventCount.label" default="Data Event Count" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "dataEventCount")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.failedDataId.label" default="Failed Data Id" /></td>
                            
                            <td valign="top" class="value">
                                        <g:if test="${outgoingBatchInstance?.failedDataId > 0}">
                                            <g:link controller="data" action="show" id="${outgoingBatchInstance.failedDataId}">${fieldValue(bean: outgoingBatchInstance, field: "failedDataId")}</g:link></td>
                                        </g:if>
                            </td>                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.sqlState.label" default="Sql State" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "sqlState")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.sqlCode.label" default="Sql Code" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "sqlCode")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.sqlMessage.label" default="Sql Message" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "sqlMessage")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.lastUpdatedHostName.label" default="Last Updated Host Name" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "lastUpdatedHostName")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.lastUpdatedTime.label" default="Last Updated Time" /></td>
                            
                            <td valign="top" class="value"><g:formatDate date="${outgoingBatchInstance?.lastUpdatedTime}" /></td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.createTime.label" default="Create Time" /></td>
                            
                            <td valign="top" class="value"><g:formatDate date="${outgoingBatchInstance?.createTime}" /></td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.batchId.label" default="Batch Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "batchId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.batchInfo.label" default="Batch Info" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "batchInfo")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="outgoingBatch.nodeBatchId.label" default="Node Batch Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: outgoingBatchInstance, field: "nodeBatchId")}</td>
                            
                        </tr>
                    
                    </tbody>
                </table>
            </div>
        </div>
    </body>
</html>
