<style>
    #pipeline_info p{
        margin:0;
        padding:0;
        color: #7a7a7a;
        font-size: 15px;
    }
    #pipeline_info h5{
        color: rgb(51, 122, 183);
        font-size: 16px;
        font-weight: 800;
    }

    .worker_info h5{
        color: rgb(51, 122, 183);
        font-size: 16px;
        font-weight: 800;
    }
    .worker_info p{
        margin:0;
        padding:0;
        color: #7a7a7a;
        font-size: 15px;
    }
    .back_to_pipeline{
        font-size: 50px;
        margin-top: -15px;
    }
</style>

<div id="pipeline_info">
    <h3 style="margin-top: -0px !important; color:rgb(51, 122, 183);">
        ${app.app_name}
    </h3>
    <h5 style="margin-top: -10px !important;">
        ${worker.key()}
    </h5>

    <div class="row" style="margin-top: 30px;">
        <div class="col-md-9">
            <p>In queue: </p>
            <p>Processed: </p>
            <p>Workers: </p>
        </div>
         <div class="col-md-3">
            <p>0 </p>
            <p>12 </p>
            <p>3</p>
        </div>
    </div>

    <div class="row">
##         <button>Start new worker</button> <button>Kill one worker</button>
    </div>

</div>

<%
from stairs.core.worker.pipeline_graph import dfs
%>

% for g_item in dfs(worker.pipeline.graph.root):
    <div node_id="${g_item.p_component.name}" class="worker_info" style="display: none">
        <button type="button" class="close back_to_pipeline" aria-label="Close">
          <span aria-hidden="true">&times;</span>
        </button>

        <h3 style="margin-top: -0px !important; color:rgb(51, 122, 183);">
        ${app.app_name}
        </h3>
        <h5 style="margin-top: -5px !important;">
            ${worker.key()}:${g_item.p_component.name}
        </h5>

        <div class="row" style="margin-top: 30px;">
            <div class="col-md-9">
                <p>As worker: </p>
                <p>Processed: </p>
                <p>Workers: </p>
            </div>
             <div class="col-md-3">
                <p>${g_item.p_component.as_worker} </p>
                <p>12 </p>
                <p>3</p>
            </div>
        </div>

##         % if g_item.p_component.as_worker:
## ##             <button>Start new worker</button> <button>Kill one worker</button>
## ##         % endif


    </div>
% endfor

<div style="margin-top: 50px">

</div>
<%include file="filters.mako">
</%include>


<script>
    $(".back_to_pipeline").on("click", function(){
        $(".worker_info").hide();
        $("#pipeline_info").show();
    });
</script>
