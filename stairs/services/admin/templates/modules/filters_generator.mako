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
    #pipeline_info{
        padding-bottom: 50px;
    }


</style>

<div id="pipeline_info">
    <h3 style="margin-top: -0px !important; color:rgb(51, 122, 183);">
        ${app.app_name}
    </h3>
    <h5 style="margin-top: -10px !important;">
        ${producer.key()}
    </h5>


    <div class="row" style="margin-top: 30px">
        <div class="col-md-6">
            <p>Next pipelines: </p>
        </div>
         <div class="col-md-6">
            <p>XXX pipeline </p>
        </div>
    </div>

</div>

<%include file="filters.mako">
</%include>


