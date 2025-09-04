import streamlit as st
import logging
import json

from neo4j_transfer import (
    Neo4jCredentials,
    TransferSpec,
    transfer,
    transfer_generator,
    get_node_and_relationship_counts,
    get_node_labels,
    get_relationship_types,
    validate_credentials,
    undo,
    reset_target_db
)
import os
from public_creds import public_creds
from streamlit.runtime.scriptrunner import RerunException
from dotenv import load_dotenv

load_dotenv()

# get neo4j transfer logger
logger = logging.getLogger("neo4j_transfer")
logger.setLevel(logging.WARNING)

# Configure Transfer package logging
# Setup
st.set_page_config(
    page_title="Neo4j Transfer Tool", layout="wide", initial_sidebar_state="collapsed"
)

s_nodes = False
s_relationships = False
s_credentials = False
t_credentials = False

TRANSFER_LOG_KEY = "transfer_log"
NODE_LABELS_KEY = "node_labels"
RELATIONSHIP_TYPES_KEY = "relationship_types"
SOURCE_URI_KEY = "NEO4J_URI"
SOURCE_USER_KEY = "NEO4J_USERNAME"
SOURCE_PASSWORD_KEY = "NEO4J_PASSWORD"
SOURCE_DATABASE_KEY = "NEO4J_DATABASE"
TARGET_URI_KEY = "TARGET_NEO4J_URI"
TARGET_USER_KEY = "TARGET_NEO4J_USERNAME"
TARGET_PASSWORD_KEY = "TARGET_NEO4J_PASSWORD"
TARGET_DATABASE_KEY = "TARGET_NEO4J_DATABASE"
COUNTS_KEY = "counts"

if TRANSFER_LOG_KEY not in st.session_state:
    # Store a list of dictionaries containing transfer data
    st.session_state[TRANSFER_LOG_KEY] = []
if NODE_LABELS_KEY not in st.session_state:
    st.session_state[NODE_LABELS_KEY] = None
if RELATIONSHIP_TYPES_KEY not in st.session_state:
    st.session_state[RELATIONSHIP_TYPES_KEY] = None

node_labels = None
rel_types = None


# Load source database info from .env, if present
if SOURCE_URI_KEY not in st.session_state:
    d_s_uri = os.environ.get(SOURCE_URI_KEY, None)
    st.session_state[SOURCE_URI_KEY] = d_s_uri
if SOURCE_USER_KEY not in st.session_state:
    d_s_user = os.environ.get(SOURCE_USER_KEY, "neo4j")
    st.session_state[SOURCE_USER_KEY] = d_s_user
if SOURCE_PASSWORD_KEY not in st.session_state:
    d_s_password = os.environ.get(SOURCE_PASSWORD_KEY, None)
    st.session_state[SOURCE_PASSWORD_KEY] = d_s_password
if SOURCE_DATABASE_KEY not in st.session_state:
    d_s_db = os.environ.get(SOURCE_DATABASE_KEY, "neo4j")
    st.session_state[SOURCE_DATABASE_KEY] = d_s_db

# Initialize counts in session state
if COUNTS_KEY not in st.session_state:
    st.session_state.counts = {'nodes': 0, 'relationships': 0}

logger = logging.getLogger("neo4j_transfer")
logger.setLevel(logging.DEBUG)


@st.cache_data(ttl="5m")
def get_nodes(_creds) -> list[str]:
    try:
        s_nodes = get_node_labels(_creds)
        return s_nodes
    except Exception as e:
        st.error(
            f"Problem getting source nodes from database with creds: {_creds}: {e}"
        )


@st.cache_data(ttl="5m")
def get_relationships(_creds) -> list[str]:
    try:
        rels = get_relationship_types(_creds)
        return rels
    except Exception as e:
        st.error(
            f"Problem getting relationships from database with creds: {_creds}: {e}"
        )


def credentials_valid(creds) -> bool:
    try:
        validate_credentials(creds)
    except Exception as e:
        st.error(f"Problem connecting with database with creds: {creds}: {e}")


# Start UI
c1, c2, c3 = st.columns(3)
with c1:
    st.header("Source Neo4j Database")

    s_db = st.selectbox("Type", options=public_creds.keys(), key="source_db", help="Select Custom for your own source database or one of the public database options. See https://neo4j.com/docs/getting-started/appendix/example-data/ for more information on public datasets.")
    if s_db:
        st.session_state[SOURCE_URI_KEY] = public_creds[s_db][SOURCE_URI_KEY]
        st.session_state[SOURCE_USER_KEY] = public_creds[s_db][SOURCE_USER_KEY]
        st.session_state[SOURCE_PASSWORD_KEY] = public_creds[s_db][SOURCE_PASSWORD_KEY]
        st.session_state[SOURCE_DATABASE_KEY] = public_creds[s_db][SOURCE_DATABASE_KEY]

    s_uri = st.text_input(
        "URI",
        st.session_state[SOURCE_URI_KEY],
        key="s_uri",
        help="If targeting a local db instance. Use Ngrok or other tunneling service. Once up and running, add 'bolt://<ngrok_tcp_address>' in this field.",
    )
    s_user = st.text_input("Username", st.session_state[SOURCE_USER_KEY], key="s_user")
    s_password = st.text_input(
        "Password",
        st.session_state[SOURCE_PASSWORD_KEY],
        key="s_password",
        type="password",
    )
    s_db = st.text_input("Database", st.session_state[SOURCE_DATABASE_KEY], key="s_db")
    if not bool(s_uri) or not bool(s_password):
        st.info(f"Enter source database info")

    # Create credentials object
    s_creds = Neo4jCredentials(
        uri=s_uri, username=s_user, password=s_password, database=s_db
    )

    if st.button("Connect"):
        # Create a status container for connection messages
        status_container = st.empty()
        
        # Show initial connection status
        status_container.info("🔄 Attempting to connect to the source database...")
        
        try:
            
            # Validate connection
            status_container.info("🔍 Validating database connection...")
            validate_credentials(s_creds)
            
            # Get node labels
            status_container.info("📊 Retrieving node labels...")
            node_labels = get_nodes(s_creds)
            
            # Get relationship types
            status_container.info("🔗 Retrieving relationship types...")
            rel_types = get_relationships(s_creds)
            
            # Log for debugging
            print(f"node_labels returned: {node_labels}")
            print(f"rel_types returned: {rel_types}")
            
            # Update session state
            st.session_state[NODE_LABELS_KEY] = node_labels
            st.session_state[RELATIONSHIP_TYPES_KEY] = rel_types
            st.session_state[SOURCE_URI_KEY] = s_uri
            st.session_state[SOURCE_USER_KEY] = s_user
            st.session_state[SOURCE_PASSWORD_KEY] = s_password
            st.session_state[SOURCE_DATABASE_KEY] = s_db
            
            # Show success message
            status_container.success("✅ Successfully connected to the source database!")
            
            # Small delay to show the success message before refreshing
            import time
            time.sleep(1)
            st.rerun()
            
        except Exception as e:
            # Show detailed error message
            error_msg = f"❌ Failed to connect to the database: {str(e)}"
            logging.error(error_msg)
            status_container.error(error_msg)
            st.stop()

with c2:
    # Display source data options
    node_options = st.session_state.get(NODE_LABELS_KEY, None)
    relationship_options = st.session_state.get(RELATIONSHIP_TYPES_KEY, None)
    if node_options is None or relationship_options is None:
        st.stop()

    st.header("Transfer Options")
    # st.write("Transfer Options")
    st.write("Deselect Nodes or Relationship types to remove from transfer")

    # Update the multiselect widgets to use on_change
    get_nodes = st.multiselect(
        "Nodes",
        options=st.session_state[NODE_LABELS_KEY],
        default=st.session_state[NODE_LABELS_KEY],
        key='selected_nodes_widget'
    )

    get_relationships = st.multiselect(
        "Relationships",
        options=st.session_state[RELATIONSHIP_TYPES_KEY],
        default=st.session_state[RELATIONSHIP_TYPES_KEY],
        key='selected_rels_widget'
    )

    st.info("Refreshing counts...")
    total_nodes, total_rels = get_node_and_relationship_counts(s_creds, get_nodes, get_relationships)

    # Display counts
    col1, col2 = st.columns(2)
    col1.metric(
        "Nodes to Transfer", 
        f"{total_nodes:,}"
    )
    col2.metric(
        "Relationships to Transfer", 
        f"{total_rels:,}"
    )

    # Show warning if nothing is selected
    if len(get_nodes) == 0 and len(get_relationships) == 0:
        st.warning("Please select at least one node or relationship")

with c3:
    st.header("Target Neo4j Database")

    # Optionally load target database credentials from .env
    t_s_uri = os.environ.get("TARGET_NEO4J_URI", None)
    t_s_user = os.environ.get("TARGET_NEO4J_USERNAME", "neo4j")
    t_s_password = os.environ.get("TARGET_NEO4J_PASSWORD", None)
    t_s_db = os.environ.get("TARGET_NEO4J_DATABASE", "neo4j")

    t_uri = st.text_input(
        "URI",
        t_s_uri,
        key="t_uri",
        help="If targeting a local db instance. Use Ngrok or other tunneling service. Once up and running, add 'bolt://<ngrok_tcp_address>' in this field.",
    )
    t_user = st.text_input("Username", t_s_user, key="t_user")
    t_password = st.text_input(
        "Password", t_s_password, key="t_password", type="password"
    )
    t_db = st.text_input("Database", t_s_db, key="t_db")
    if t_uri and t_password:
        t_creds = Neo4jCredentials(
            uri=t_uri, username=t_user, password=t_password, database=t_db
        )
        credentials_valid(t_creds)

        add_default_data = st.checkbox(
            "Add default properties",
            value=True,
            help="Adds transfer detail properties to transferred nodes and relationships. Following key-values will be added: _original_element_id and _transfer_timestamp",
        )

        overwrite_target = st.checkbox(
            "⚠️ Purge target database prior to transfer (can not be undone)",
            value=False,
            help="Purge all current data in the target database prior to transferring data from the source database. Deletes ALL data on target database!",
        )

        spec = TransferSpec(
            node_labels=get_nodes,
            relationship_types=get_relationships,
            should_append_data=add_default_data
        )

        if st.button("Start Transfer"):
            if len(get_nodes) == 0:
                st.warning("Select at least one node label to start a transfer")
            else:
                status_container = st.empty()
                purge_status_container = st.empty()
                # progress_indicator = st.progress(0.0)

                try:
                    source_creds = Neo4jCredentials(
                        uri=st.session_state[SOURCE_URI_KEY],
                        username=st.session_state[SOURCE_USER_KEY],
                        password=st.session_state[SOURCE_PASSWORD_KEY],
                        database=st.session_state[SOURCE_DATABASE_KEY],
                    )

                    # Show initial status
                    status_container.info("Starting transfer process...")
                    
                    if overwrite_target:
                        try:
                            purge_result = reset_target_db(t_creds)
                            purge_status_container.success("Target database purged successfully!")
                        except Exception as e:
                            purge_status_container.error(f"Failed to purge target database: {str(e)}")
                            st.stop()

                    
                    progress_container = st.empty()
                    progress_indicator = progress_container.progress(0.0)

                    # Add a stop button
                    stop_button = st.button("⏹️ Stop Transfer", key="stop_transfer")

                    # Start the transfer
                    transfer_gen = transfer_generator(source_creds, t_creds, spec)  # Changed target_creds to t_creds
                    controller = next(transfer_gen)  # Get the controller

                    for result in transfer_gen:
                        if stop_button:
                            controller.request._stop()
                            status_container.warning("Transfer stopped by user")
                            break
                            
                        if result is None:
                            raise ValueError("Missing result from transfer generator")
                        
                        completion = result.float_completed()
                        progress_text = f"Upload {round(completion * 100)}% complete"
                        
                        # Update the progress indicator
                        progress_indicator.progress(
                            completion,
                            text=progress_text
                        )
                        
                        # Force Streamlit to update the UI
                        progress_container.empty()  # Clear the previous progress
                        progress_indicator = progress_container.progress(completion, text=progress_text)
                        
                        # Rerun to update the UI and check the stop button
                        st.rerun()

                    # Store list of transfer ids so an undo option is possible
                    st.session_state[TRANSFER_LOG_KEY].insert(
                        0, {"transfer_spec": spec.dict(), "result": result.dict()}
                    )
                    msg = f"✅ Transfer complete - {result}"
                    logging.info(msg)
                    st.success(msg)
                except Exception as e:
                    msg = f"❌ Problem during transfer: {str(e)}"
                    logging.error(msg)
                    st.error(msg)
                    if 'progress_indicator' in locals():
                        progress_indicator.empty()
                    if 'status_container' in locals():
                        status_container.error("Transfer failed. See error details above.")

    else:
        st.info(f"Enter target database info")

with st.sidebar:
    st.header("Transfer Log")
    logs = st.session_state[TRANSFER_LOG_KEY]
    if len(logs) == 0:
        st.write("<No prior transfers yet>")
    for log in logs:
        ts = log["transfer_spec"]["timestamp"]
        with st.expander(f"{ts}"):
            st.code(f"{log}")
            if st.button("Undo", key=ts):
                u_spec = TransferSpec(**log["transfer_spec"])
                result = undo(t_creds, u_spec)
                st.info(result.__dict__)
