<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title></title>
     <!-- Latest compiled and minified CSS -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-select/1.12.4/css/bootstrap-select.min.css">

    <!-- Latest compiled and minified JavaScript -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-select/1.12.4/js/bootstrap-select.min.js"></script>

    <!-- (Optional) Latest compiled and minified JavaScript translation files -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-select/1.12.4/js/i18n/defaults-*.min.js"></script>

    <link href="https://fonts.googleapis.com/css?family=Pavanam" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css?family=Open+Sans" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css?family=Oswald" rel="stylesheet">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">

    <script src="https://code.jquery.com/jquery-1.12.4.js"></script>
    <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js" integrity="sha384-Tc5IQib027qvyjSMfHjOMaLkfuWVxZxUPnCJA7l2mCWNIpG9mGCD8wGNIcPD7Txa" crossorigin="anonymous"></script>
    <script src="https://rawgit.com/stidges/jquery-searchable/master/dist/jquery.searchable-1.0.0.min.js"></script>
    <link rel="stylesheet" type="text/css" href="/static/main.css">
    <link rel="stylesheet" href="/static/tagsinput/dist/bootstrap-tagsinput.css">

    <script src="static/tagsinput/dist/bootstrap-tagsinput.min.js"></script>
    <script src="static/tagsinput/dist/bootstrap-tagsinput/bootstrap-tagsinput-angular.min.js"></script>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.2.21/cytoscape.js"></script>

    <script src="https://cdn.rawgit.com/cpettitt/dagre/v0.7.4/dist/dagre.min.js"></script>
    <script src="https://cdn.rawgit.com/cytoscape/cytoscape.js-dagre/1.5.0/cytoscape-dagre.js"></script>

    <script src="http://cdnjs.cloudflare.com/ajax/libs/qtip2/2.2.0/jquery.qtip.min.js"></script>
    <link href="http://cdnjs.cloudflare.com/ajax/libs/qtip2/2.2.0/jquery.qtip.min.css" rel="stylesheet" type="text/css" />
    <script src="https://cdn.rawgit.com/cytoscape/cytoscape.js-qtip/2.7.0/cytoscape-qtip.js"></script>

</head>
<body>
<div id="container">
    <%include file="../modules/head.mako" />
    ${next.body()}
</div>

</body>
</html>
