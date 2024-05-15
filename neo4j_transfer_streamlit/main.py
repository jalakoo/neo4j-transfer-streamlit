import streamlit as st
import logging

from neo4j_transfer import (
    Neo4jCredentials,
    TransferSpec,
    transfer,
    get_node_labels,
    get_relationship_types,
    validate_credentials,
)
import os

st.set_page_config(layout="wide")
c1, c2, c3 = st.columns(3)
s_nodes = False
s_relationships = False
s_credentials = False
t_credentials = False

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
with c2:
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

with c3:
    st.write("Transfer Options")

    # Display source data options
    if s_credentials == None:
        st.info(f"Enter source database info")
        st.stop()
    else:
        nodes = st.multiselect("Nodes", options=s_nodes)
        relationships = st.multiselect("Relationships", options=s_relationships)

    # Present transfer button
    if t_creds == False:
        st.info(f"Enter target database info")
    else:
        spec = TransferSpec(node_labels=nodes, relationship_types=relationships)
        if st.button("Transfer"):
            try:
                result = transfer(s_creds, t_creds, spec)
                st.success(f"Transfer complete - {result}")
            except Exception as e:
                st.error(f"Problem transferring: {e}")
