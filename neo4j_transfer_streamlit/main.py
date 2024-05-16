import streamlit as st
import logging
import json

from neo4j_transfer import (
    Neo4jCredentials,
    TransferSpec,
    transfer,
    get_node_labels,
    get_relationship_types,
    validate_credentials,
    undo,
)
import os

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
SOURCE_URI_KEY = "source_uri"
SOURCE_USER_KEY = "source_user"
SOURCE_PASSWORD_KEY = "source_password"
SOURCE_DATABASE_KEY = "source_database"

if TRANSFER_LOG_KEY not in st.session_state:
    # Store a list of dictionaries containing transfer data
    st.session_state[TRANSFER_LOG_KEY] = []
if NODE_LABELS_KEY not in st.session_state:
    st.session_state[NODE_LABELS_KEY] = None
if RELATIONSHIP_TYPES_KEY not in st.session_state:
    st.session_state[RELATIONSHIP_TYPES_KEY] = None

node_labels = None
rel_types = None

# Optionally load source database info from .env
if SOURCE_URI_KEY not in st.session_state:
    d_s_uri = os.environ.get("NEO4J_URI", None)
    st.session_state[SOURCE_URI_KEY] = d_s_uri
if SOURCE_USER_KEY not in st.session_state:
    d_s_user = os.environ.get("NEO4J_USERNAME", "neo4j")
    st.session_state[SOURCE_USER_KEY] = d_s_user
if SOURCE_PASSWORD_KEY not in st.session_state:
    d_s_password = os.environ.get("NEO4J_PASSWORD", None)
    st.session_state[SOURCE_PASSWORD_KEY] = d_s_password
if SOURCE_DATABASE_KEY not in st.session_state:
    d_s_db = os.environ.get("NEO4J_DATABASE", "neo4j")
    st.session_state[SOURCE_DATABASE_KEY] = d_s_db

logger = logging.getLogger("neo4j_transfer")
logger.setLevel(logging.DEBUG)


@st.cache_data(ttl="5m")
def nodes(_creds) -> list[str]:
    try:
        s_nodes = get_node_labels(_creds)
        return s_nodes
    except Exception as e:
        st.error(
            f"Problem getting source nodes from database with creds: {_creds}: {e}"
        )


@st.cache_data(ttl="5m")
def relationships(_creds) -> list[str]:
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
    st.write("Source Neo4j Database")

    # with st.form("Source Neo4j Database"):

    s_uri = st.text_input("URI", st.session_state[SOURCE_URI_KEY], key="s_uri")
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

    # submitted = st.form_submit_button("Connect")
    # if submitted:
    if st.button("Connect"):
        try:
            s_creds = Neo4jCredentials(
                uri=s_uri, username=s_user, password=s_password, database=s_db
            )
            validate_credentials(s_creds)
            node_labels = nodes(s_creds)
            rel_types = relationships(s_creds)
            print(f"node_labels returned: {node_labels}")
            print(f"rel_types returned: {rel_types}")
            st.session_state[NODE_LABELS_KEY] = node_labels
            st.session_state[RELATIONSHIP_TYPES_KEY] = rel_types
            st.session_state[SOURCE_URI_KEY] = s_uri
            st.session_state[SOURCE_USER_KEY] = s_user
            st.session_state[SOURCE_PASSWORD_KEY] = s_password
            st.session_state[SOURCE_DATABASE_KEY] = s_db
            st.success("Connection successful")
        except Exception as e:
            st.error(f"Problem connecting with database: {e}")
            st.stop()

with c2:
    # Display source data options
    node_options = st.session_state.get(NODE_LABELS_KEY, None)
    relationship_options = st.session_state.get(RELATIONSHIP_TYPES_KEY, None)
    if node_options is None or relationship_options is None:
        st.stop()

    # if node_labels is None or rel_types is None:
    #     st.stop()

    st.write("Transfer Options")

    nodes = st.multiselect("Nodes", options=st.session_state[NODE_LABELS_KEY])
    relationships = st.multiselect(
        "Relationships", options=st.session_state[RELATIONSHIP_TYPES_KEY]
    )

    if len(nodes) == 0 and len(relationships) == 0:
        st.info(f"Select nodes and/or relationships to transfer")
with c3:
    st.write("Target Neo4j Database")

    # Optionally load target database credentials from .env
    t_s_uri = os.environ.get("TARGET_NEO4J_URI", None)
    t_s_user = os.environ.get("TARGET_NEO4J_USERNAME", "neo4j")
    t_s_password = os.environ.get("TARGET_NEO4J_PASSWORD", None)
    t_s_db = os.environ.get("TARGET_NEO4J_DATABASE", "neo4j")

    t_uri = st.text_input("URI", t_s_uri, key="t_uri")
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
            help="Adds transfer detail properties to transferred nodes and relationships. Following key-values will be added: _transfer_element_id and _transfer_timestamp",
        )

        spec = TransferSpec(
            node_labels=nodes,
            relationship_types=relationships,
            should_append_data=add_default_data,
        )

        if st.button("Start Transfer"):
            if len(nodes) == 0:
                st.warning("Select at least one node label to start a transfer")
            else:
                try:
                    source_creds = Neo4jCredentials(
                        uri=st.session_state[SOURCE_URI_KEY],
                        username=st.session_state[SOURCE_USER_KEY],
                        password=st.session_state[SOURCE_PASSWORD_KEY],
                        database=st.session_state[SOURCE_DATABASE_KEY],
                    )
                    result = transfer(source_creds, t_creds, spec)

                    # Store list of transfer ids so an undo option is possible
                    st.session_state[TRANSFER_LOG_KEY].insert(
                        0, {"transfer_spec": spec.dict(), "result": result.dict()}
                    )
                    st.success(f"Transfer complete - {result}")
                except Exception as e:
                    st.error(f"Problem transferring: {e}")

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
            if st.button("Undo"):
                u_spec = TransferSpec(**log["transfer_spec"])
                result = undo(t_creds, u_spec)
                st.info(result.__dict__)
