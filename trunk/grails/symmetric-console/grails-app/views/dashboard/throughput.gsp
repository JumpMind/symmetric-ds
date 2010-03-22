<html>
  <head>
    <title>Throughput</title>
    <meta name="layout" content="main" />
    
    
    
  </head>
  <body>
      <g:form action="throughput">
        <g:select name="interval" from="${intervals}" value="${params.interval}" optionKey="value" optionValue="key"/>
        <g:submitButton name="submit" value="Select Interval"/>
      </g:form>
      <table width="100%"> 
        <tr>
            <th>Begin</th>
            <th>End</th>
            <th>Channel</th>
            <th>Total Data Events</th>
            <th>Extract MS / Data Event</th>
            <th>Filter MS / Data Event</th>
        </tr>    
    <g:each in="${throughput}" var="t">
        <tr>
            <td>${t.begin}</td>
            <td>${t.end}</td>
            <td>${t.channelId}</td>
            <td>${t.totalDataEventCount}</td>
            <td>${t.extractMs}</td>
            <td>${t.filterMs}</td>
        </tr>
    </g:each>
    </table>
    
    <ofchart:chart name="demo-chart" url="${createLink(action:'throughput')}" width="800" height="400"/>
  </body>
</html>