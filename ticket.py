import streamlit as st
import pandas as pd
import os
from io import BytesIO
from snowflake.connector import connect
from test import add_keywords_to_snowflake_1

# Snowflake connection details
SNOWFLAKE_USER = "Satendra"
SNOWFLAKE_PASSWORD = "Pass@123"
SNOWFLAKE_ACCOUNT = "gcb87239"
SNOWFLAKE_WAREHOUSE = "COE"
SNOWFLAKE_DATABASE = "TICKET_DATA_COE"
SNOWFLAKE_SCHEMA = "TICKET_DATA"
SNOWFLAKE_STAGE = 'TICKET'

def connect_to_snowflake():
    return connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def get_table_list():
    conn = connect_to_snowflake()
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
    conn.close()
    return [table[1] for table in tables]

def add_keywords(table_name, keywords):
    conn = connect_to_snowflake()
    with conn.cursor() as cur:
        try:
            alter_command = f"""  
                ALTER TABLE {table_name}  
                ADD COLUMN CLASSES ARRAY;  
            """
            cur.execute(alter_command)
          
            update_command = f"""  
                UPDATE {table_name}  
                SET CLASSES=ARRAY_CONSTRUCT({', '.join([f"'{keyword}'" for keyword in keywords])});  
            """
            cur.execute(update_command)
            st.success("Keywords added Successfully.You can move to next **tab** _Categorize your data_.")
            st.write(f"**Keywords added to table** _{table_name}_ : :blue[{keywords}]")
        except Exception as E:
            st.warning(":green[Keywords already exists for the selected table]. You can move to next **tab** :- _Categorize your data_.")   
    conn.close()

def categorizing_data(table_name,selected_column,categorized_table_name):
    conn = connect_to_snowflake()
    # categorized_table_name = f"{table_name}_categorize"
    try:
        with conn.cursor() as cur:   
            cortex_function = f"""    
                CREATE  OR REPLACE TABLE {categorized_table_name} AS    
                SELECT *,    
                    SNOWFLAKE.CORTEX.CLASSIFY_TEXT("{selected_column}", CLASSES)['label'] AS classification    
                FROM {table_name} limit 100;    
            """
            cur.execute(cortex_function)
        conn.close()
            
            
        st.success("Categorization Done!!!")    
    except Exception as e:
        st.error(e)


def show_categorized_data(table_name):
    conn = connect_to_snowflake()
    with conn.cursor() as cur:
        classified = f"Select * from {table_name}"
        classified_Data = cur.execute(classified)
        st.write(classified_Data)
        df = pd.read_sql(classified,conn)
    conn.close()
    return df

def stage_and_load_to_snowflake(temp_file_path, table_name):
    conn = connect_to_snowflake()
    with conn.cursor() as cur:
        # PUT command to upload file to Snowflake stage
        put_cmd = f"PUT file://{temp_file_path} @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
        cur.execute(put_cmd)

        create_ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
                USING TEMPLATE ( SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                    LOCATION=>'@{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}/{temp_file_path}.gz'
                    , FILE_FORMAT=>'csv_format'
                    )
                    ));
        """
        cur.execute(create_ddl)

        copy_command = f"""
            COPY INTO {table_name} FROM
            @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}/{temp_file_path}.gz
            FILE_FORMAT = (
                                TYPE = 'CSV',
                                FIELD_DELIMITER = ',',
                                FIELD_OPTIONALLY_ENCLOSED_BY = '"',
                                TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
                                )
                                ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_command)
    conn.close()

def handle_csv(uploaded_file):
    
    df = pd.read_csv(uploaded_file)
    st.dataframe(df)
    safe_file_name = uploaded_file.name.replace(" ", "_")
    temp_file_path = f"{safe_file_name}"
    with open(temp_file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    # Ask for table name
    table_name = st.text_input(f"**Enter Snowflake table name:**", "default_table_name")

    # Button to stage CSV and load into Snowflake
    if st.button("Stage CSV and Load into Snowflake"):
        stage_and_load_to_snowflake(temp_file_path, table_name)
        st.success(f"CSV file staged and loaded to Snowflake table _{table_name}_ successfully!")

    # Clean up temporary file
    os.remove(temp_file_path)

def handle_excel(uploaded_file):
    xls = pd.ExcelFile(uploaded_file)
    sheet_names = xls.sheet_names
    selected_sheet = st.selectbox("Select the sheet to preview and upload", sheet_names)

    if selected_sheet:
        df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
        st.write(f"Preview of the selected sheet '{selected_sheet}':")
        st.dataframe(df)

        # Convert to CSV button
        if st.button("Convert to CSV"):
            # Save DataFrame to CSV in bytes
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Display download button after conversion
            st.download_button(
                label="Download CSV",
                data=csv_buffer,
                file_name=f"{selected_sheet}.csv",
                mime="text/csv"
            )

        # Prompt user to upload the converted CSV file
        st.write("Please upload the converted CSV file to proceed with loading into Snowflake.")
        converted_csv_file = st.file_uploader("Upload the converted CSV file", type=["csv"])

        if converted_csv_file is not None:
            df_csv = pd.read_csv(converted_csv_file)
            st.dataframe(df_csv)
            safe_csv_file_name = converted_csv_file.name.replace(" ", "_")
            temp_csv_file_path = f"{safe_csv_file_name}"
            with open(temp_csv_file_path, "wb") as f:
                f.write(converted_csv_file.getbuffer())

            # Ask for table name
            table_name = st.text_input("Enter Snowflake table name for the uploaded CSV:", "default_table_name")

            # Button to stage CSV and load into Snowflake
            if st.button("Stage Uploaded CSV and Load into Snowflake"):
                stage_and_load_to_snowflake(temp_csv_file_path, table_name)
                st.success(f"CSV file staged and loaded to Snowflake table '{table_name}' successfully!")

            # Clean up temporary file
            os.remove(temp_csv_file_path)


def display_table_data(conn, selected_table):
    try:
        show_data_query = f"SELECT * FROM {selected_table} LIMIT 100"
        with conn.cursor() as cur:
            cur.execute(show_data_query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            data_df = pd.DataFrame(rows, columns=columns)
        st.dataframe(data_df)
    except Exception as e:
        st.warning("No Table Selected.")

def add_keywords_to_snowflake():
    with st.spinner():
        conn = connect_to_snowflake()
        table_list = get_table_list()
        
        try:
            selected_table = st.selectbox("Select a table:", table_list, index=None, placeholder="Select your table...")
            display_table_data(conn, selected_table)
        except Exception as e:
            st.warning("No Table Selected.")

    if selected_table is not None:
    # Get and display existing keywords from session state
        keywords = st.session_state.get('keywords', [])
        st.write(f"Enter keywords for your table **{selected_table}** one by one:")

        # Input field for new keyword
        new_keyword = st.text_input("Enter keyword:", key="new_keyword")
    
        # Add keyword to session state
        if st.button("Add Keyword"):
            with st.spinner():
                if new_keyword:
                    keywords.append(new_keyword)
                    st.session_state['keywords'] = keywords
                    # st.experimental_rerun()
                    st.write("Keywords entered so far:", keywords)
            
                else:
                    st.warning("You haven't entered any keyword.")

        # Add keywords to Snowflake table
        if st.button("Add Keywords to Snowflake Uploaded table"):
            with st.spinner():
                try:
                    # st.write(keywords)
                    add_keywords(selected_table, keywords)
                    # st.write(f"**Keywords added to table** _{selected_table}_ : :blue[{keywords}]")
                    st.session_state['keywords'] = []  # Reset keywords after adding
                except Exception as e:
                    st.error(e)



def categorize_data():
    conn = connect_to_snowflake()
    table_list = get_table_list()

    try:
        selected_table = st.selectbox("Select a table:", table_list, index=None, placeholder="Select your table...")
        display_table_data(conn, selected_table)
    except Exception as e:
        st.warning("No Table Selected.")

    st.subheader("Select the column based on which you want to categorize your data.")

    # Fetch columns for selected table
    with conn.cursor() as cur:
        get_column_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{selected_table}'"
        cur.execute(get_column_query)
        columns = cur.fetchall()


    column_names = [col[0] for col in columns]

    
        # Initialize session state for selected_column if it doesn't exist
    if 'selected_column' not in st.session_state:
        st.session_state.selected_column = None

    # Callback function to update session state
    def update_selected_column():
        st.session_state.selected_column = st.session_state.temp_selected_column
    try:
    # Create a selectbox for column selection with a temporary session state variable
        st.selectbox(
            "Choose a column for categorization:",
            column_names,
            index=0 if st.session_state.selected_column is None else column_names.index(st.session_state.selected_column),
            key='temp_selected_column',
            on_change=update_selected_column,
            placeholder="Select the column for categorization..."
        )
    
        st.write(f"Selected Column: {st.session_state.selected_column}")
    except Exception as e:
        st.warning("No Column Selected.")    

    

    categorized_table_name = f"{selected_table}_CATEGORIZE"
    if st.session_state.selected_column:
        if st.button("Start Categorization"):
            with st.spinner('Categorizing data...'):
                categorizing_data(selected_table, st.session_state.selected_column,categorized_table_name)
            # st.success("Categorization Done")

        if st.button("Show Categorized Data"):
            df = show_categorized_data(categorized_table_name)
            st.warning(f":green[Categorized table name in Snowflake -] **{categorized_table_name}**")
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            st.download_button(
                label="Download CSV",
                data=csv_buffer,
                file_name=f"{categorized_table_name}.csv",
                mime="text/csv"
            )






               












   




       
























   




       
























