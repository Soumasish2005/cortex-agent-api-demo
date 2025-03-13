import json
import requests
import streamlit as st
import snowflake.connector
import sseclient 


# replace these values in your .streamlit.toml file, not here!
HOST = st.secrets["snowflake"]["host"]
ACCOUNT = st.secrets["snowflake"]["account"]
USER =st.secrets["snowflake"]["user"]
PASSWORD = st.secrets["snowflake"]["password"]
ROLE = st.secrets["snowflake"]["role"]
WAREHOUSE= "SALES_INTELLIGENCE_WH"

print("Snowflake Host:", HOST)  # Debugging
if not HOST:
    st.error("HOST is not set in .streamlit.toml!")

# variables shared across pages

AGENT_API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds
URL = f"https://{HOST}{AGENT_API_ENDPOINT}"
print("Requesting URL:", URL)
CORTEX_SEARCH_SERVICES = "sales_intelligence.data.sales_conversation_search"
SEMANTIC_MODELS = "@sales_intelligence.data.models/sales_metrics_model.yaml" 

def run_snowflake_query(query):
    try:
        df = session.sql(query.replace(';',''))
        
        return df

    except Exception as e:
        st.error(f"Error executing SQL: {str(e)}")
        return None, None

def agent_api_call(query: str, limit: int = 10):
    text = ""
    sql = ""
    
    payload = {
        "model": "llama3.1-70b",
        "messages": [{"role": "user", "content": [{"type": "text", "text": query}]}],
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}},
            {"tool_spec": {"type": "cortex_search", "name": "search1"}}
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {"name": CORTEX_SEARCH_SERVICES, "max_results": limit}
        }
    }
    
    resp = requests.post(
        url=URL,
        json=payload,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True
    )

    if resp.status_code < 400:
        print("Response Status Code:", resp.status_code)
        print("Response Headers:", resp.headers)
        print("Response Content:", resp.text[:500]) 
        if resp.status_code != 200:
            st.error(f"Request failed: {resp.status_code} - {resp.text}")
            st.stop()

        if "text/event-stream" not in resp.headers.get("Content-Type", ""):
            st.error("Response is not an SSE stream")
            st.write(resp.text)  # Debugging output
            st.stop()
       
        client = sseclient.SSEClient(resp)

        for event in client.events():
            try:
                parsed = json.loads(event.data)
                print("Parsed Event:", parsed)  # Debugging step

                if "delta" in parsed and "content" in parsed["delta"]:
                    content = parsed["delta"]["content"]

                    if isinstance(content, list) and len(content) > 0:
                        first_content = content[0]

                        if "type" in first_content:
                            if first_content["type"] == "text":
                                text = first_content.get("text", "")
                                yield text

                            elif first_content["type"] == "tool_use" and len(content) > 1:
                                tool_results = content[1].get("tool_results", {}).get("content", [{}])
                                
                                if len(tool_results) > 0 and "json" in tool_results[0]:
                                    json_data = tool_results[0]["json"]
                                    text = json_data.get("text", "")
                                    sql = json_data.get("sql", "")
                                    
                                    yield text
                                    yield f"\n\n`{sql}`"
                                
            except json.JSONDecodeError:
                print("Error parsing JSON:", event.data)
                continue

def main():

    with st.sidebar:
        if st.button("Reset Conversation", key="new_chat"):
            st.session_state.messages = []
            st.rerun()

    st.title("Intelligent Sales Assistant")

    # connection
    if 'CONN' not in st.session_state or st.session_state.CONN is None:

        try: 
            st.session_state.CONN = snowflake.connector.connect(
                user=USER,
                password=PASSWORD,
                account=ACCOUNT,
                host=HOST,
                port=443,
                role=ROLE,
                warehouse=WAREHOUSE
            )  
            st.info('Snowflake Connection established!', icon="💡")    
        except:
            st.error('Connection not established. Check that you have correctly entered your Snowflake credentials!', icon="🚨")    



    #try: 
    #  Initialize session state
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message['role']):
            st.markdown(message['content'].replace("•", "\n\n-"))

    if user_input := st.chat_input("What is your question?"):

            # Add user message to chat       
            with st.chat_message("user"):
                st.markdown(user_input)
                st.session_state.messages.append({"role": "user", "content": user_input})

            # Get response from API
            with st.spinner("Processing your request..."):
                
                response = agent_api_call(user_input, 1)
                text = st.write(response)

                # Add assistant response to chat
                if text:
                    st.session_state.messages.append({"role": "assistant", "content": text})

      
if __name__ == "__main__":
    main()