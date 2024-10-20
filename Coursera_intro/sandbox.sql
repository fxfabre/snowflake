USE SCHEMA TASTY_BYTES.RAW_POS;


select count(*) from menu;
SELECT TOP 10 * FROM menu m;

SELECT TOP 10
    m.menu_id,
    m.item_category,
    ing.index,
    ing.value
FROM menu m,
    LATERAL flatten(input => m.menu_item_health_metrics_obj['menu_item_health_metrics']) det,
    LATERAL flatten(input => det.value['ingredients']) ing;


SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b', 'What kind of literature was Marianne Moore known for ?');

