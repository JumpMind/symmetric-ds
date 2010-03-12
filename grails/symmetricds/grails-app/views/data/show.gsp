
<%@ page import="org.jumpmind.symmetric.grails.Data" %>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="layout" content="main" />
        <g:set var="entityName" value="${message(code: 'data.label', default: 'Data')}" />
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
                            <td valign="top" class="name"><g:message code="data.id.label" default="Data Id" /></td>
                            
                            <td valign="top" class="value">${dataInstance.dataId}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.createTime.label" default="Create Time" /></td>
                            
                            <td valign="top" class="value"><g:formatDate date="${dataInstance?.createTime}" /></td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.transactionId.label" default="Transaction Id" /></td>
                            
                            <td valign="top" class="value">${dataInstance.transactionId}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.channelId.label" default="Channel Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "channelId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.dataId.label" default="Data Id" /></td>
                            
                            <td valign="top" class="value">${dataInstance.dataId}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.sourceNodeId.label" default="Source Node Id" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "sourceNodeId")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.rowData.label" default="Row Data" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "rowData")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.oldData.label" default="Old Data" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "oldData")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.externalData.label" default="External Data" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "externalData")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.tableName.label" default="Table Name" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "tableName")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.pkData.label" default="Pk Data" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "pkData")}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.eventType.label" default="Event Type" /></td>
                            
                            <td valign="top" class="value">${dataInstance?.eventType?.encodeAsHTML()}</td>
                            
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.triggerHistory.label" default="Trigger History" /></td>
                            
                            <td valign="top" class="value">${dataInstance.triggerHist?.toHtmlString()}</td>
                            
                        </tr>
                    
                    </tbody>
                </table>
            </div>
        </div>
    </body>
</html>
