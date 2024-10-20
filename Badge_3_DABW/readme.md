# snow_smoothies
- Badge 3: Data Application Builders Workshop
- https://learn.snowflake.com/en/courses/uni-ess-dabw/

Steps :
- Create a DB with snowflake
- Create an app with https://share.streamlit.io

Sample credentials to access snowflake db from streamlit app :
- warning : there is a dash in the account id
```ini
[connections.snowflake]
account = "org-name"
user = "login_name"
password = "login_password"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"
database = "SMOOTHIES"
schema = "PUBLIC"
client_session_keep_alive = true
```
