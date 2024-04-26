from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st
from openai import OpenAI


client = OpenAI(api_key=st.secrets["openai_api_key"])
# Google Cloud Storage setup
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
bigquery_client = bigquery.Client(location="EU", credentials=credentials)

# Constants
EMBEDDING_MODEL = "text-embedding-ada-002"
GPT_MODEL = "gpt-4-turbo"

def remove_code_markers(text):
    # Replace the specific substrings
    clean_text = text.replace("```sql", "").replace("```", "")
    return clean_text

def execute_query(query):
    if "nvalid" in query:  # Correct method to check for a substring
        return "no query result"

    # Clean the query to remove specific code markers
    cleaned_query = remove_code_markers(query)

    # Debug print statement to show the cleaned query
    print(cleaned_query)

    # Execute the SQL query using BigQuery client
    query_job = bigquery_client.query(cleaned_query)  # Using the cleaned query

    # Wait for the query to complete
    query_job.result()

    # Get the query results and convert to a markdown formatted table
    results = query_job.to_dataframe()
    markdown_table = results.to_markdown(index=False)

    return markdown_table

# Streamlit UI
st.title("Ask your data")

user_input = st.text_input("Din fråga?", key="user_input")
if user_input:
    
    
    # Prepare the prompt for GPT-4 in Swedish
    instructions_prompt = f"""
    You have a BigQuery table named `dnb_ab_falkenberg` in the dataset `dnb_data`.
    The table contains information about companies, including `foretag` (company name), `omsattning`(in ksek), `anstallda`, and `bransch_grov` and  `bokslutsar`(string. eg. '2021').
    Write an executable SQL query to retrieve the full order based on the given question:
    {user_input}

    Default to latest year 2022. 
    Reply with executable SQL.
    If the given input is an invalid question, reply: "Invalid question given the data"
    ```

    """

    
    table = ""
    # Stream the GPT-4 reply
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        completion = client.chat.completions.create(
            model=GPT_MODEL,
            messages=[{"role": "system", "content": instructions_prompt}],
            stream=True
        )
        for chunk in completion:
            if chunk.choices[0].finish_reason == "stop": 
                message_placeholder.markdown(full_response)
                table=execute_query(full_response)
                break
            full_response += chunk.choices[0].delta.content
            message_placeholder.markdown(full_response + "▌")
    
    if table:
        with st.chat_message("assistant"):
            st.write(table)
            analysis_prompt = f"""
                You received a query from an analyst.
                They executed a SQL query and provided the results in Markdown table format.
                Analyze the table and explain the result. Answer the analyst's question: {user_input}

                The sql that was run was this: 
                {full_response}

                The result was this Markdown Table:

                ```
                {table}
                ```
                Provide a detailed but short answer, adressing the question and the provided sql and table. Reply in the language of: {user_input}
                """
    if analysis_prompt:
        with st.chat_message("assistant"):
            analysis_placeholder = st.empty()
            final_response = ""
            completion = client.chat.completions.create(
                model=GPT_MODEL,
                messages=[{"role": "system", "content": analysis_prompt}],
                stream=True
            )
            for chunk in completion:
                if chunk.choices[0].finish_reason == "stop": 
                    analysis_placeholder.markdown(final_response)
                    break
                final_response += chunk.choices[0].delta.content
                analysis_placeholder.markdown(final_response + "▌")