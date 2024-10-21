# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched
from snowflake.snowpark.context import get_active_session

# option = st.selectbox("Choose your favorite fruit :", ["aa", "bb"])
# st.write("You selected:", option)
# st.dataframe(data=df_fuits, use_container_width=True)


# Write directly to the app
st.title(":cup_with_straw: Pending orders ! :cup_with_straw:")
st.write("""Orders to be done""")

session = get_active_session()
df_orders = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == False)
editable_df = st.data_editor(df_orders)

submitted = st.button("Submit")
if submitted:
    og_dataset = session.table("smoothies.public.orders")
    edited_dataset = session.create_dataframe(editable_df)
    og_dataset.merge(
        edited_dataset,
        (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
        [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
    )
