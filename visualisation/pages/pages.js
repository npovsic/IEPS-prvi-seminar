let svg = d3.select("svg#pack"),
	margin = 20,
	diameter = +svg.attr("width"),
	g = svg.append("g").attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

let color = d3.scaleLinear()
	.domain([-1, 5])
	.range(["hsl(152,80%,80%)", "hsl(228,30%,40%)"])
	.interpolate(d3.interpolateHcl);

let pack = d3.pack()
	.size([diameter - margin, diameter - margin])
	.padding(2);

d3.json("data_3000.json", function(error, root) {
	if (error) throw error;

	root = d3.hierarchy(root)
		.sum(function(d) { return d.size; })
		.sort(function(a, b) { return b.value - a.value; });

	let focus = root,
		nodes = pack(root).descendants(),
		view;

	// tooltip
	let tooltip = d3.select("body")
		.append("div")
		.attr("class", "tooltip")
		.style("position", "absolute")
		.style("z-index", "10")
		.style("visibility", "hidden");


	// define circle to present site/page
	let circle = g.selectAll("circle")
		.data(nodes)
		.enter().append("circle")
		.attr("class", function(d) { return d.parent ? d.children ? "node" : "node node--leaf" : "node node--root"; })
		.style("fill", function(d) {
			if (d.data.type) {

				if (d.data.type === 'DISALLOWED') return 'hsl(0, 0%, 74%)';
				else if (d.data.type === 'ERROR') return 'hsl(0, 73%, 77%)';
				else if (d.data.type === 'DUPLICATE') return 'hsl(54, 100%, 88%)';

			}

			return d.children ? color(d.depth) : null; })

		.on("mouseover", function (d) {
			let pageType = d.data.type ? d.data.type : "";

			tooltip.html(
				"<p>" + d.data.name + "</p>" +
				"<p class='bold'>" + pageType + "</p>"
			)
			tooltip.style("visibility", "visible");
		})
		.on("mousemove", function(){return tooltip.style("top", (event.pageY-10)+"px").style("left",(event.pageX+10)+"px");})
		.on("mouseout", function(){return tooltip.style("visibility", "hidden");})
		.on("click", function(d) { if (focus !== d) zoom(d), d3.event.stopPropagation(); })
		.on("contextmenu", function (d, i) {
			d3.event.preventDefault();
			d3.event.stopPropagation();
			// react on right-clicking
			window.open(d.data.name, '_blank')
		});

	// text on circle
	let text = g.selectAll("text")
		.data(nodes)
		.enter().append("text")
		.attr("class", "label")
		.style("fill-opacity", function(d) { return d.parent === root ? 1 : 0; })
		.style("display", function(d) { return d.parent === root ? "inline" : "none"; })
		.text(function(d) { return d.data.name; });

	let node = g.selectAll("circle,text");

	svg
		.style("background", color(-1))
		.on("click", function() { zoom(root); });

	zoomTo([root.x, root.y, root.r * 2 + margin]);

	function zoom(d) {
		let focus0 = focus; focus = d;

		let transition = d3.transition()
			.duration(d3.event.altKey ? 7500 : 750)
			.tween("zoom", function(d) {
				let i = d3.interpolateZoom(view, [focus.x, focus.y, focus.r * 2 + margin]);
				return function(t) { zoomTo(i(t)); };
			});

		transition.selectAll("text")
			.filter(function(d) { return d.parent === focus || this.style.display === "inline"; })
			.style("fill-opacity", function(d) { return d.parent === focus && d.children ? 1 : 0; })
			.on("start", function(d) { if (d.parent === focus) this.style.display = "inline"; })
			.on("end", function(d) { if (d.parent !== focus) this.style.display = "none"; });
	}

	function zoomTo(v) {
		let k = diameter / v[2]; view = v;
		node.attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - v[1]) * k + ")"; });
		circle.attr("r", function(d) { return d.r * k; });
	}
});