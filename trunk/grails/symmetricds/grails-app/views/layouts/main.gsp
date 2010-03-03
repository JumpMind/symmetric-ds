<html>
<head>
  <title><g:layoutTitle default="SymmetricDS"/></title>
  <link rel="stylesheet" href="${resource(dir: 'css', file: 'main.css')}"/>
  <link rel="shortcut icon" href="${resource(dir: 'images', file: 'favicon.ico')}" type="image/x-icon"/>
  <g:layoutHead/>
  <g:javascript library="application"/>
</head>
<body>
<div id="page">

  <div id="topbar">
    <g:render template="/common/topbar"/>
  </div>

  <!--
  <div id='header'>
  </div>
      -->

  <div id='main'>

    <div id='navbar'>
      <g:render template="/common/navbar"/>
    </div>

    <div id="content">
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