<%inherit file="base.mako" />

<%block name="body" >
<div class="row" style="margin-top: 80px">

    <div class="col-md-2 col-md-offset-2" id="tags_container">
        ${next.left_sidebar()}
    </div>

    <div class=" col-md-6">
        ${next.main_content()}
    </div>

</div>
</%block>
