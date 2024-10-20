create table garden_plants.veggies.vegetable_details(
    plant_name varchar(25), root_depth_code varchar(1)
);

USE DATABASE GARDEN_PLANTS;
USE SCHEMA VEGGIES;

SELECT * FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS;

DELETE
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS
WHERE plant_name = 'Spinach'
AND ROOT_DEPTH_CODE = 'D';
