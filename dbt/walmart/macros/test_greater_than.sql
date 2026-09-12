{% test greater_than(model, column_name, threshold) %}

    -- dbt tests look for failing rows. 
    -- If it should be greater than the threshold, we query for rows that are less than or equal to it.
    select *
    from {{ model }}
    where {{ column_name }} <= {{ threshold }}

{% endtest %}