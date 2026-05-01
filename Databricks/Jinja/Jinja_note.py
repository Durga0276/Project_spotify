# Databricks notebook source
pip install jinja2

# COMMAND ----------

parameters = [
    
    {
        "table": "spotify.silver.factstream",
        "alias" : "fact",
        "col" : "fact.stream_id,fact.listen_duration"
    },
    {
        "table": "spotify.silver.dimuser",
        "alias" : "user",
        "col" : "user.user_id, user.user_name",
        "condition" : "fact.user_id=user.user_id"
    },
    {
        "table": "spotify.silver.dimtrack",
        "alias" : "track",
        "col" : "track.track_id,track.track_name",
        "condition":"fact.track_id=track.track_id"
    }    
]

# COMMAND ----------

query_text = """
        select 
            {% for param in parameters %}
                {{param.col}}
                    {% if not loop.last %}
                    ,
                    {%endif%}
            {% endfor %}
        from
            {% for param in parameters %}
                {% if loop.first %}
                    {{param['table']}} as {{param['alias']}}
                {% endif %}
            {% endfor %}
        {% for param in parameters %}
            {% if not loop.first %}
                left join
                {{param['table']}} as {{param['alias']}}
                on {{param['condition']}}
            {% endif %}
        {% endfor %}
"""

# COMMAND ----------

from jinja2 import Template 
jn_sql=Template(query_text)
QJ= jn_sql.render(parameters=parameters)
print(QJ)

# COMMAND ----------

display(spark.sql(QJ))