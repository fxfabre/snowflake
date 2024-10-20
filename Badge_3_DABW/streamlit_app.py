import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd
import streamlit.components.v1 as components

# Some streamlit prompt / display functions
#option = st.selectbox("Choose your favorite fruit :", ["aa", "bb"])
#st.write("You selected:", option)
#st.dataframe(data=df_fuits, use_container_width=True)

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")
st.write("""Choose the fruits you want in your custom Smoothie!""")

name_on_order = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))
pd_df = my_dataframe.to_pandas()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients :',
    my_dataframe,
    max_selections=5,
)

ingredients_string = ''
if ingredients_list:
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '

        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.subheader(fruit_chosen + ' Nurition Information (Serving Per 100g)')
        try:
            fruityvice_response = requests.get("https://fruityvice.com/api/fruit/" + search_on)
            fvv = pd.DataFrame(fruityvice_response.json(), columns=['nutritions'])
            components.html(fvv.to_html(header=False))
        except Exception:
            pass

    time_to_insert = st.button('Submit Order')
    if time_to_insert:
        my_insert_stmt = f"""
            insert into smoothies.public.orders(ingredients, name_on_order, )
            values ('{ingredients_string}', '{name_on_order}')
        """
        session.sql(my_insert_stmt).collect()
        st.success(f"""Your Smoothie is ordered, {name_on_order} !""", icon="✅")
