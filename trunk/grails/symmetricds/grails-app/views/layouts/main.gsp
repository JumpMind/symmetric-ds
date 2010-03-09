<html>
<head>
  <title><g:layoutTitle default="SymmetricDS"/></title>
  <link rel="stylesheet" href="${resource(dir: 'css', file: 'symmetric.css')}"/>  

  <link rel="shortcut icon" href="${resource(dir: 'images', file: 'favicon.ico')}" type="image/x-icon"/>
  <g:layoutHead/>
  <g:javascript library="application"/>
</head>
<body>
<div id="page">

    <div id="topbar">
        <g:render template="/common/topbar"/>
    </div>

    <div id="content"> 
        <div id="main-content"> 
        <g:render template="/common/navbar"/>
        <g:layoutBody/>
    </div>

  </div>

  <!--
  <div id="footer">
    <g:render template="/common/footer"/>
  </div>
  -->

</div>
</body>
</html>