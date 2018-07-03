<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
	<title>高德地图API使用</title>
	<link rel="stylesheet" href="http://cache.amap.com/lbs/static/main1119.css"/>
    <script src="http://webapi.amap.com/maps?v=1.4.4&key=b4d44f8f786605c6b2c015e0357e83ff"></script>
</head>

<body>
	<div id="container"></div>
	<script>
		var map = new AMap.Map("container", {
			resizeEnable: true,
			center: [116.418261, 39.921984],
			//mapStyle: 'fresh'
			zoom: 11
		});
		
		//map.setFeatures("road"); //单一种类要素显示
		//map.setFeatures(['road', 'point']); //多个种类要素显示
		
		//测量距离
		map.plugin(["AMap.MouseTool"], function(){
			var mousetool = new AMap.MouseTool(map);
			mousetool.rule();
		});
		
		//工具条
		AMap.plugin(['AMap.ToolBar'], function(){
			map.addControl(new AMap.ToolBar);
		});
		
		//热力图：静态数据
		var heatmap;
		var points =[
			{"lng":116.191031,"lat":39.988585,"count":100},
			{"lng":116.389275,"lat":39.925818,"count":110},
			{"lng":116.287444,"lat":39.810742,"count":102},
			{"lng":116.481707,"lat":39.940089,"count":130},
			{"lng":116.410588,"lat":39.880172,"count":140},
			{"lng":116.394816,"lat":39.91181,"count":150},
			{"lng":116.416002,"lat":39.952917,"count":160}
		];
		
		map.plugin(["AMap.Heatmap"],function() {      //加载热力图插件
			heatmap = new AMap.Heatmap(map, {
				radius: 50, //给定半径
				opacity: [0, 0.8] //透明度
			});    //在地图对象叠加热力图
			heatmap.setDataSet({
				data:points,
				max:100
			}); //设置热力图数据集
			//具体参数见接口文档
		});
	</script>
</body>
</html>