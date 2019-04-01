/**
 *
 * EXAMPLE
 * https://d3indepth.com/layouts/
 *
 **/
let root = d3.hierarchy(DATA);


let treeLayout = d3.tree();

treeLayout.size([400, 200]);

treeLayout(root);


// Nodes
d3.select('svg#network g.nodes')
	.selectAll('circle.node')
	.data(root.descendants())
	.enter()
	.append('circle')
	.classed('node', true)
	.attr('cx', function(d) {return d.x;})
	.attr('cy', function(d) {return d.y;})
	.attr('r', 4);

// Links
d3.select('svg#network g.links')
	.selectAll('line.link')
	.data(root.links())
	.enter()
	.append('line')
	.classed('link', true)
	.attr('x1', function(d) {return d.source.x;})
	.attr('y1', function(d) {return d.source.y;})
	.attr('x2', function(d) {return d.target.x;})
	.attr('y2', function(d) {return d.target.y;});