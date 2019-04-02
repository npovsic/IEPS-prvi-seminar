// http://blog.thomsonreuters.com/index.php/mobile-patent-suits-graphic-of-the-day/
d3.json("links_data.json", function(error, root) {

    let links = root.links;

    let nodes = {};

    // Compute the distinct nodes from the links.
    links.forEach(function(link) {
      link.source = nodes[link.source] || (nodes[link.source] = {name: link.source});
      link.target = nodes[link.target] || (nodes[link.target] = {name: link.target});
    });

    let width = 1920,
        height = 1080;

    let force = d3.layout.force()
        .nodes(d3.values(nodes))
        .links(links)
        .size([width, height])
        .linkDistance(300)
        .charge(-100)
        .on("tick", tick)
        .start();

    let svg = d3.select("div#network").append("svg")
        .attr("width", width)
        .attr("height", height);

    // Per-type markers, as they don't inherit styles.
    svg.append("defs").selectAll("marker")
        .data(["suit", "licensing", "resolved"])
      .enter().append("marker")
        .attr("id", function(d) { return d; })
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 15)
        .attr("refY", -1.5)
        .attr("markerWidth", 6)
        .attr("markerHeight", 6)
        .attr("orient", "auto")
      .append("path")
        .attr("d", "M0,-5L10,0L0,5");

	// tooltip
	let tooltip = d3.select("body")
		.append("div")
		.attr("class", "tooltip")
		.style("position", "absolute")
		.style("z-index", "10")
		.style("visibility", "hidden");

    let path = svg.append("g").selectAll("path")
        .data(force.links())
      .enter().append("path")
        .attr("class", function(d) { return "link " + d.type; })
        .attr("marker-end", function(d) { return "url(#" + d.type + ")"; });

    let circle = svg.append("g").selectAll("circle")
        .data(force.nodes())
      .enter().append("circle")
        .attr("r", 10)
        .style("fill", 'hsl(174, 100%, 27%)')
		.on("mouseover", function (d) {

		    console.log("D: ", d);

			tooltip.html(
				"<p>" + d.name + "</p>"
			)
			tooltip.style("visibility", "visible");
		})
		.on("mousemove", function(){return tooltip.style("top", (event.pageY-10)+"px").style("left",(event.pageX+10)+"px");})
		.on("mouseout", function(){return tooltip.style("visibility", "hidden");})
		.on("contextmenu", function (d, i) {
			d3.event.preventDefault();
			d3.event.stopPropagation();
			// react on right-clicking
			window.open(d.name, '_blank')
		})
        .call(force.drag);

/*    let text = svg.append("g").selectAll("text")
        .data(force.nodes())
      .enter().append("text")
        .attr("x", 8)
        .attr("y", ".31em")
        .text(function(d) { return d.name; });*/

    // Use elliptical arc path segments to doubly-encode directionality.
    function tick() {
      path.attr("d", linkArc);
      circle.attr("transform", transform);
      //text.attr("transform", transform);
    }

    function linkArc(d) {
      let dx = d.target.x - d.source.x,
          dy = d.target.y - d.source.y,
          dr = Math.sqrt(dx * dx + dy * dy);
      return "M" + d.source.x + "," + d.source.y + "A" + dr + "," + dr + " 0 0,1 " + d.target.x + "," + d.target.y;
    }

    function transform(d) {
      return "translate(" + d.x + "," + d.y + ")";
    }
});

function getRandomColor() {
    return "#"+((1<<24)*Math.random()|0).toString(16);
}