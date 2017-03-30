<%@page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<html>
<head>
<!-- Latest compiled and minified CSS -->
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css">
<!-- Optional theme -->
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css">
<!-- Latest compiled and minified JavaScript -->
<script src="//code.jquery.com/jquery-1.11.0.min.js"></script>
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>

<script type="text/javascript">
    $(function() {
        $("#default_button").click(function() {
            alert("onClick default_button");
        });
    });

    $(function() {
        $("#primary_button").click(function() {
            alert("onClick primary_button");
        });
    });

    $(function() {
        $("#success_button").click(function() {
            alert("onClick success_button");
        });
    });

    $(function() {
        $("#menu_link1").click(function() {
            alert("onClick menu_link1");
        });
    });

    $(function() {
        $("#menu_link2").click(function() {
            alert("onClick menu_link2");
        });
    });
</script>
</head>
<body>
<h2>JSTL List Example</h2>

<c:if test="${not empty names}">
    <ul>
        <c:forEach var="nameValue" items="${names}">
            <li>${nameValue}</li>
        </c:forEach>
    </ul>
</c:if>
<button type="button" id="default_button" class="btn btn-lg btn-default">Default</button>
<button type="button" id="primary_button" class="btn btn-lg btn-primary">Primary</button>
<button type="button" id="success_button" class="btn btn-lg btn-success">Success</button>
<button type="button" class="btn btn-lg btn-info">Info</button>
<button type="button" class="btn btn-lg btn-warning">Warning</button>
<button type="button" class="btn btn-lg btn-danger">Danger</button>
<button type="button" class="btn btn-lg btn-link">Link</button>

<div class="btn-group">
    <!-- 버튼태그 -->
    <button class="btn btn-default dropdown-toggle" type="button" data-toggle="dropdown">
        메뉴버튼
        <span class="caret"></span>
    </button>
    <!--메뉴버튼 클릭시 하단 표출된 리스트 영역  -->
    <ul class="dropdown-menu" >
        <!-- 메뉴1 -->
        <li><a id="menu_link1" href="#">메뉴1</a></li>
        <!-- 메뉴2 -->
        <li><a id="menu_link2" href="#">메뉴2</a></li>
    </ul>
</div>
</body>
</html>