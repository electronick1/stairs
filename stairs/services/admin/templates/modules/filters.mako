<style>
    .tags_item{
        font-size: 17px;
    }
    .tags_item .component_type{
        margin-bottom: 10px;
        margin-left: 10px;
    }
    .component_type p{
        margin: 0;
        padding: 0;
        border-bottom: 1px solid #1997c6;
    }

    .component_type a{
        font-weight: normal;
        margin: 0;
        padding: 0;
    }
</style>

<h3 style="margin-top: -0px !important;">
    <a href="{% url 'app:squads' %}">Apps</a>
</h3>
% for app in apps:
    <div class="tags_item">
        <p class="tags_item"># ${app.app_name}</p>

        <div class="component_type">
            <p>Generators:</p>
            % for producer in app.components.producers.values():
                <div><a href="/generator/${app.app_name}/${producer.key()}"> ${producer.key()}</a></div>
            % endfor
        </div>
        <div class="component_type">
            <p >Pipelines:</p>
            % for pipeline in app.components.pipelines.values():
                <a href="/pipeline/${app.app_name}/${pipeline.key()}">${pipeline.key()}</a>
            % endfor
        </div>

    </div>
% endfor
