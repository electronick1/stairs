<%inherit file="base/base_left.mako" />

<%block name="left_sidebar" >
    <%include file="modules/filters_generator.mako" />
</%block >




<%block name="main_content" >
    <div class="content">

        <div class="row">
            <div class="col-md-4" align="center">
                <h1>100</h1>
                <p>chunks</p>
            </div>
            <div class="col-md-4" align="center">
                <h1>1230</h1>
                <p>generated</p>
            </div>
            <div class="col-md-4" align="center">
                <h1>30</h1>
                <p>items per second</p>
            </div>
        </div>
    </div>

</%block>
