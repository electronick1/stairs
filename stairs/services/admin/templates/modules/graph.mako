<style>

#cy {
  width: 100%;
  height: 700px;
  left: 0;
  top: 0;
  z-index: 999;
}

h1 {
  opacity: 0.5;
  font-size: 1em;
}

</style>
<div class="content">


     <div id="cy"></div>
</div>



<%
nodes = []
edges = []

update_nodes = {}

from stairs.core.worker.pipeline_graph import dfs
from uuid import uuid4

for g_item in dfs(worker.pipeline.graph.root):
    nodes.append(g_item.p_component.name)
    if g_item.p_component.update_pipe_data:
        id = str(uuid4())
        update_nodes[g_item.p_component.name] = id
        nodes.append(id)

for g_item in dfs(worker.pipeline.graph.root):
    g_item_name = g_item.p_component.name

    if g_item_name in update_nodes:
        g_item_name = update_nodes[g_item_name]

    for edge in g_item.next:
        edge_name = edge.p_component.name

        is_worker = edge.p_component.as_worker

        if edge_name in update_nodes:
            edges.append((g_item_name, edge_name, is_worker))
            edges.append((g_item_name, update_nodes[edge_name], is_worker))
            edges.append((edge_name, update_nodes[edge_name], is_worker))
        else:
            edges.append((g_item_name,
                          edge_name,
                          is_worker))
%>



<script>

    $(function () {
        var cy = window.cy = cytoscape({
            container: document.getElementById('cy'),

            boxSelectionEnabled: false,
            autounselectify: true,

            layout: {
                name: 'dagre'
            },

            style: [
                {
                    selector: 'node',
                    style: {
                        'content': 'data(id)',
                        'text-opacity': 0.5,
                        'text-valign': 'center',
                        'text-halign': 'right',
                        'background-color': '#11479e'
                    }
                },

                {
                    selector: 'edge',
                    style: {
                        'curve-style': 'bezier',
                        'width': 2,
                        'line-color': '#9dbaea',
                        'source-arrow-color': '#9dbaea',
                        'target-arrow-color': '#9dbaea'
                    }
                },
                {
                    selector: "edge.worker",
                    style: {
                        'line-style': 'dotted',
                        'source-arrow-shape': 'diamond',
                        'target-arrow-shape': 'diamond'
                    }

                }
            ],

            elements: {
                 nodes: [
                     % for node in nodes:
                         {data: {id: "${node}"},
                          classes: "no_label"},
                     % endfor
                 ],
                 edges: [
                     % for edge in edges:
                         {data: {source: "${edge[0]}",
                                 target: "${edge[1]}"},
                         %if edge[2]:
                             classes: 'worker',
                         %endif
                         },
                     % endfor

                 ]
            },

            ready: function(){
              window.cy = this;
            },

        });

        % for update_id in update_nodes.values():
            ##  cy.style().selector("node.no_label").style("content", "123");
            cy.$('#${update_id}').style("color", "white");
        % endfor


        cy.$('#n4').qtip({
            content: 'Hello!',
            position: {
                my: 'top center',
                at: 'bottom center'
            },
            style: {
                classes: 'qtip-bootstrap',
                tip: {
                    width: 16,
                    height: 8
                }
            }
        });
        cy.zoom({
          level: 1, // the zoom level
          position: { x: 0, y: 0 }
        });

        cy.on('tap', 'node', function(evt){
            var node = evt.target;
            console.log( 'tapped ' + node.id() );
            $("#pipeline_info").hide();
            $(".worker_info").hide();
            $("div[node_id='"+node.id()+"']").show();

        });

    });
</script>
