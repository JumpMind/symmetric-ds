
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
                <table width="100%">
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
                            <td valign="top" class="name"><g:message code="data.data.label" default="Data" /></td>
                            
                            <td valign="top" class="value">
                                <div class="data-panel">
                                    <table>
                                        <%
                                        columns = dataInstance.triggerHist?.columnNames.split(",") 
                                        old = dataInstance.toParsedOldData() 
                                        rows = dataInstance.toParsedRowData()
										pk = dataInstance.toParsedPkData()
                                        %>
                                        <tr>
                                            <td colspan="${columns?.size()}" class="data-table-name">${dataInstance.tableName}&nbsp;(${dataInstance.eventType?.encodeAsHTML()})</span></td>
                                        </tr>
	                                    <tr class="data-table-columns">
	                                           <td class="data-column">&nbsp;</td>
	                                        <g:each in="${columns}" var="c" status="i">
	                                            <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}">${c}</td>
	                                        </g:each>
	                                    </tr><tr>
                                                <td class="data-column">PK</td>
                                            
                                            <%pkCounter = 0%>
                                            <g:each in="${pk}" var="p" status="i">
                                                <%pkCounter = i%>
                                                <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}">${p}</td>
                                            </g:each>
                                            <%pkLength = pk != null ? pk.length : 0%>
                                            <g:while test="${pkLength < columns.length}">
                                                <%pkLength++
												pkCounter++%>
                                                <td class="data-coulumn${(pkCounter % 2) == 0 ? ' alt' : ''}">&nbsp;</td>
                                            </g:while>
                                        </tr><tr>
                                                <td class="data-column">New</td>
                                            <g:each in="${rows}" var="r" status="i">
                                                <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}${old == null || rows[i] != old[i] ? ' diff' : ''}">${r}</td>
                                            </g:each>
                                        </tr><tr>
                                                <td class="data-column">Old</td>
                                            <g:each in="${old}" var="o" status="i">
                                                <td class="data-coulumn${(i % 2) == 0 ? ' alt' : ''}">${o}</td>
                                            </g:each>
                                        </tr>
	                                </table>
	                            </div>
                     
                            </td>                           
                        </tr>
                    
                        <tr class="prop">
                            <td valign="top" class="name"><g:message code="data.externalData.label" default="External Data" /></td>
                            
                            <td valign="top" class="value">${fieldValue(bean: dataInstance, field: "externalData")}</td>
                            
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
