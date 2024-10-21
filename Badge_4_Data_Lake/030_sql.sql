USE DATABASE zenas_athleisure_db;
USE SCHEMA products;

--create an internal table for some sweatsuit info
create or replace table zenas_athleisure_db.products.sweatsuits (
	color_or_style varchar(25),
	file_name varchar(50),
	price number(5,2)
);

--fill the new table with some data
insert into  zenas_athleisure_db.products.sweatsuits
          (color_or_style, file_name, price)
values
    ('Burgundy', 'burgundy_sweatsuit.png',65),
    ('Charcoal Grey', 'charcoal_grey_sweatsuit.png',65),
    ('Forest Green', 'forest_green_sweatsuit.png',64),
    ('Navy Blue', 'navy_blue_sweatsuit.png',65),
    ('Orange', 'orange_sweatsuit.png',65),
    ('Pink', 'pink_sweatsuit.png',63),
    ('Purple', 'purple_sweatsuit.png',64),
    ('Red', 'red_sweatsuit.png',68),
    ('Royal Blue',	'royal_blue_sweatsuit.png',65),
    ('Yellow', 'yellow_sweatsuit.png',67);


select * from directory(@sweatsuits);


CREATE OR REPLACE VIEW PRODUCT_LIST AS
    select INITCAP(REPLACE(REPLACE(relative_path, '_', ' '), '.png')) as product_name,
        sweatsuit.file_name, sweatsuit.color_or_style, sweatsuit.price, dir_stage.file_url
    from directory(@sweatsuits) as dir_stage
    INNER JOIN zenas_athleisure_db.products.sweatsuits AS sweatsuit
        ON sweatsuit.file_name = dir_stage.relative_path;


CREATE OR REPLACE VIEW zenas_athleisure_db.products.catalog AS
    select *
    from product_list p
    cross join sweatsuit_sizes;


-- Add a table to map the sweatsuits to the sweat band sets
create table zenas_athleisure_db.products.upsell_mapping
(
    sweatsuit_color_or_style varchar(25),
    upsell_product_code varchar(10)
);
SELECT * FROM zenas_athleisure_db.products.upsell_mapping;

--populate the upsell table
insert into zenas_athleisure_db.products.upsell_mapping(
    sweatsuit_color_or_style, upsell_product_code
) VALUES
('Charcoal Grey','SWT_GRY'),('Forest Green','SWT_FGN'),('Orange','SWT_ORG'),
('Pink', 'SWT_PNK'),('Red','SWT_RED'),('Yellow', 'SWT_YLW');



-- Zena needs a single view she can query for her website prototype
create view catalog_for_website as
select color_or_style
,price
,file_name
, get_presigned_url(@sweatsuits, file_name, 3600) as file_url
,size_list
,coalesce('Consider: ' ||  headband_description || ' & ' || wristband_description, 'Consider: White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, file_name
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, file_name
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code;

SELECT * FROM catalog_for_website;
