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

if TRANSFER_LOG_KEY not in st.session_state:
    # Store a list of dictionaries containing transfer data
    st.session_state[TRANSFER_LOG_KEY] = []

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

    # Optionally load source database credentials from .env
    d_s_uri = os.environ.get("NEO4J_URI", None)
    d_s_user = os.environ.get("NEO4J_USERNAME", "neo4j")
    d_s_password = os.environ.get("NEO4J_PASSWORD", None)
    d_s_db = os.environ.get("NEO4J_DATABASE", "neo4j")

    s_uri = st.text_input("URI", d_s_uri, key="s_uri")
    s_user = st.text_input("Username", d_s_user, key="s_user")
    s_password = st.text_input(
        "Password", d_s_password, key="s_password", type="password"
    )
    s_db = st.text_input("Database", d_s_db, key="s_db")
    if s_uri and s_password:
        s_creds = Neo4jCredentials(
            uri=s_uri, username=s_user, password=s_password, database=s_db
        )
        s_nodes = nodes(s_creds)
        s_relationships = relationships(s_creds)
    else:
        st.info(f"Enter source database info")
with c2:
    st.write("Transfer Options")
    # Display source data options
    if s_credentials == None:
        st.info(f"Enter source database info")
        st.stop()
    else:
        nodes = st.multiselect("Nodes", options=s_nodes)
        relationships = st.multiselect("Relationships", options=s_relationships)

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
                    result = transfer(s_creds, t_creds, spec)

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
