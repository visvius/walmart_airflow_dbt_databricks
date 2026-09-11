{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

SELECT 
 *,
 current_timestamp() as processed_at
FROM
    {{ source('walmart_databricks', 'orders') }}
WHERE
    is_active = 'Y'

{% if is_incremental() %}
    AND updated_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}