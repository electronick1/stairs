
<nav id="header" class="app-navbar">
    <div class="row">

        <div class="col-md-4 col-md-offset-2">
            <div class="col-md-6">
                <a href="{% url 'app:index' %}" class="logo">Stairs</a>
            </div>
            <div class="col-md-2 header_item_wr">
                <a href="{% url 'app:squads' %}" class="header_item">Pipelines</a>
            </div>
             <div class="col-md-2 header_item_wr">
                <a href="{% url 'app:teams' %}" class="header_item">Stats</a>
            </div>

        </div>


    </div>
</nav>
