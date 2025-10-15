import streamlit as st
import logging
import json
import time
from typing import Optional, Dict, Any, List
from datetime import datetime

from neo4j_transfer import (
    Neo4jCredentials,
    TransferSpec,
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
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("neo4j_transfer")

# Page configuration
st.set_page_config(
    page_title="Neo4j Transfer Tool", 
    layout="wide", 
    initial_sidebar_state="collapsed"
)

# Constants
class SessionKeys:
    TRANSFER_LOG = "transfer_log"
    NODE_LABELS = "node_labels"
    RELATIONSHIP_TYPES = "relationship_types"
    SOURCE_URI = "NEO4J_URI"
    SOURCE_USER = "NEO4J_USERNAME"
    SOURCE_PASSWORD = "NEO4J_PASSWORD"
    SOURCE_DATABASE = "NEO4J_DATABASE"
    TARGET_URI = "TARGET_NEO4J_URI"
    TARGET_USER = "TARGET_NEO4J_USERNAME"
    TARGET_PASSWORD = "TARGET_NEO4J_PASSWORD"
    TARGET_DATABASE = "TARGET_NEO4J_DATABASE"
    PURGE_CONFIRMED = "purge_confirmed"
    SHOW_REFRESH_MESSAGE = "show_refresh_message"
    PREVIOUS_NODES = "previous_nodes"
    PREVIOUS_RELS = "previous_rels"

# Session state initialization
def init_session_state():
    """Initialize session state variables"""
    defaults = {
        SessionKeys.TRANSFER_LOG: [],
        SessionKeys.NODE_LABELS: None,
        SessionKeys.RELATIONSHIP_TYPES: None,
        SessionKeys.SOURCE_URI: os.environ.get("NEO4J_URI", ""),
        SessionKeys.SOURCE_USER: os.environ.get("NEO4J_USERNAME", "neo4j"),
        SessionKeys.SOURCE_PASSWORD: os.environ.get("NEO4J_PASSWORD", ""),
        SessionKeys.SOURCE_DATABASE: os.environ.get("NEO4J_DATABASE", "neo4j"),
        SessionKeys.TARGET_URI: os.environ.get("TARGET_NEO4J_URI", ""),
        SessionKeys.TARGET_USER: os.environ.get("TARGET_NEO4J_USERNAME", "neo4j"),
        SessionKeys.TARGET_PASSWORD: os.environ.get("TARGET_NEO4J_PASSWORD", ""),
        SessionKeys.TARGET_DATABASE: os.environ.get("TARGET_NEO4J_DATABASE", "neo4j"),
        SessionKeys.SHOW_REFRESH_MESSAGE: True,
        SessionKeys.PREVIOUS_NODES: None,
        SessionKeys.PREVIOUS_RELS: None,
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

# Cached functions
@st.cache_data(ttl=300)  # 5 minutes
def get_cached_node_labels(uri: str, username: str, database: str) -> List[str]:
    """Get node labels with caching (password not cached for security)"""
    try:
        # Note: We don't cache the password for security reasons
        temp_creds = Neo4jCredentials(uri=uri, username=username, password="", database=database)
        return get_node_labels(temp_creds)
    except Exception as e:
        st.error(f"Problem getting node labels: {e}")
        return []

@st.cache_data(ttl=300)
def get_cached_relationship_types(uri: str, username: str, database: str) -> List[str]:
    """Get relationship types with caching (password not cached for security)"""
    try:
        temp_creds = Neo4jCredentials(uri=uri, username=username, password="", database=database)
        return get_relationship_types(temp_creds)
    except Exception as e:
        st.error(f"Problem getting relationship types: {e}")
        return []

# Validation functions
def validate_uri(uri: str) -> bool:
    """Validate Neo4j URI format"""
    if not uri or not uri.strip():
        return False
    
    # Check if URI has a valid scheme
    valid_schemes = ['bolt', 'bolt+ssc', 'bolt+s', 'neo4j', 'neo4j+ssc', 'neo4j+s']
    uri_lower = uri.lower().strip()
    
    for scheme in valid_schemes:
        if uri_lower.startswith(f"{scheme}://"):
            return True
    
    return False

def validate_connection(creds: Neo4jCredentials) -> bool:
    """Validate database connection"""
    try:
        # First validate URI format
        if not validate_uri(creds.uri):
            st.error(f"Invalid URI format: '{creds.uri}'. Must start with bolt://, neo4j://, or their secure variants.")
            return False
            
        validate_credentials(creds)
        return True
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return False

def validate_required_fields(*fields) -> bool:
    """Check if all required fields are filled"""
    return all(bool(field) for field in fields)

# Dialog components
@st.dialog("⚠️ Confirm Database Purge")
def confirm_purge_dialog():
    """Confirmation dialog for database purge"""
    st.error("**This action cannot be undone!**")
    st.write("You are about to **permanently delete ALL data** in the target database:")
    
    st.markdown("""
    - All nodes and properties
    - All relationships  
    - All indexes
    - All constraints
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state[SessionKeys.PURGE_CONFIRMED] = False
            st.rerun()
    
    with col2:
        if st.button("🗑️ Yes, Permit Purges", use_container_width=True, type="primary"):
            st.session_state[SessionKeys.PURGE_CONFIRMED] = True
            st.rerun()

# UI Components
def render_source_database_section():
    """Render the source database configuration section"""
    st.header("Source Neo4j Database")

    # Database type selector
    db_type = st.selectbox(
        "Type", 
        options=list(public_creds.keys()), 
        key="source_db_type",
        help="Select a public dataset or 'Custom' for your own database"
    )
    
    if db_type and db_type in public_creds and db_type != "custom":
        # Auto-populate from selected public database (but not for custom)
        creds = public_creds[db_type]
        st.session_state[SessionKeys.SOURCE_URI] = creds[SessionKeys.SOURCE_URI]
        st.session_state[SessionKeys.SOURCE_USER] = creds[SessionKeys.SOURCE_USER]
        st.session_state[SessionKeys.SOURCE_PASSWORD] = creds[SessionKeys.SOURCE_PASSWORD]
        st.session_state[SessionKeys.SOURCE_DATABASE] = creds[SessionKeys.SOURCE_DATABASE]

    # Connection fields
    s_uri = st.text_input("URI", st.session_state[SessionKeys.SOURCE_URI], key="s_uri")
    s_user = st.text_input("Username", st.session_state[SessionKeys.SOURCE_USER], key="s_user")
    s_password = st.text_input("Password", st.session_state[SessionKeys.SOURCE_PASSWORD], 
                              key="s_password", type="password")
    s_db = st.text_input("Database", st.session_state[SessionKeys.SOURCE_DATABASE], key="s_db")
    
    # Update session state with current input values
    st.session_state[SessionKeys.SOURCE_URI] = s_uri
    st.session_state[SessionKeys.SOURCE_USER] = s_user
    st.session_state[SessionKeys.SOURCE_PASSWORD] = s_password
    st.session_state[SessionKeys.SOURCE_DATABASE] = s_db

    if not validate_required_fields(s_uri, s_password):
        st.info("Enter source database connection details")
        return None, False

    s_creds = Neo4jCredentials(uri=s_uri, username=s_user, password=s_password, database=s_db)
    
    if st.button("Connect to Source", key="connect_source"):
        return connect_to_source_database(s_creds, s_uri, s_user, s_password, s_db)
    
    return s_creds, bool(st.session_state[SessionKeys.NODE_LABELS])

def connect_to_source_database(creds: Neo4jCredentials, uri: str, user: str, password: str, db: str):
    """Handle source database connection"""
    status_container = st.empty()
    
    try:
        status_container.info("🔄 Connecting to source database...")
        
        if not validate_connection(creds):
            return None, False
            
        status_container.info("📊 Retrieving schema information...")
        
        # Get schema information (using actual creds with password)
        node_labels = get_node_labels(creds)
        rel_types = get_relationship_types(creds)
        
        # Update session state
        st.session_state.update({
            SessionKeys.NODE_LABELS: node_labels,
            SessionKeys.RELATIONSHIP_TYPES: rel_types,
            SessionKeys.SOURCE_URI: uri,
            SessionKeys.SOURCE_USER: user,
            SessionKeys.SOURCE_PASSWORD: password,
            SessionKeys.SOURCE_DATABASE: db,
            SessionKeys.SHOW_REFRESH_MESSAGE: True
        })
        
        # Clear previous selections
        for key in [SessionKeys.PREVIOUS_NODES, SessionKeys.PREVIOUS_RELS, 
                   'selected_nodes_widget', 'selected_rels_widget']:
            st.session_state.pop(key, None)
        
        status_container.success("✅ Successfully connected!")
        time.sleep(1)
        st.rerun()
        
    except Exception as e:
        status_container.error(f"❌ Connection failed: {str(e)}")
        return None, False

def render_transfer_options_section(s_creds: Neo4jCredentials):
    """Render the transfer options section"""
    if not st.session_state[SessionKeys.NODE_LABELS]:
        st.info("Connect to source database first")
        return None, None, 0, 0

    st.header("Transfer Options")
    st.write("Select the data to transfer:")

    # Multiselect widgets
    selected_nodes = st.multiselect(
        "Node Labels",
        options=st.session_state[SessionKeys.NODE_LABELS],
        default=st.session_state[SessionKeys.NODE_LABELS],
        key='selected_nodes_widget'
    )

    selected_rels = st.multiselect(
        "Relationship Types",
        options=st.session_state[SessionKeys.RELATIONSHIP_TYPES],
        default=st.session_state[SessionKeys.RELATIONSHIP_TYPES],
        key='selected_rels_widget'
    )

    # Check for changes and get counts
    selections_changed = (
        st.session_state.get(SessionKeys.PREVIOUS_NODES) != selected_nodes or 
        st.session_state.get(SessionKeys.PREVIOUS_RELS) != selected_rels
    )

    # Show refresh message and get counts
    if st.session_state.get(SessionKeys.SHOW_REFRESH_MESSAGE) or selections_changed:
        with st.spinner("Calculating number of Nodes and Relationships for Transfer..."):
            total_nodes, total_rels = get_node_and_relationship_counts(
                s_creds, selected_nodes, selected_rels
            )
        
        # Update session state
        st.session_state.update({
            SessionKeys.PREVIOUS_NODES: selected_nodes.copy(),
            SessionKeys.PREVIOUS_RELS: selected_rels.copy(),
            SessionKeys.SHOW_REFRESH_MESSAGE: False
        })
    else:
        # Use cached counts
        total_nodes, total_rels = get_node_and_relationship_counts(
            s_creds, selected_nodes, selected_rels
        )

    # Display counts
    col1, col2 = st.columns(2)
    col1.metric("Nodes to Transfer", f"{total_nodes:,}")
    col2.metric("Relationships to Transfer", f"{total_rels:,}")

    if not selected_nodes and not selected_rels:
        st.warning("Please select at least one node label or relationship type")

    return selected_nodes, selected_rels, total_nodes, total_rels

def render_target_database_section(selected_nodes: List[str], selected_rels: List[str]):
    """Render the target database and transfer section"""
    st.header("Target Neo4j Database")

    # Target session state is already initialized in init_session_state()

    # Connection fields
    t_uri = st.text_input("URI", st.session_state[SessionKeys.TARGET_URI], key="t_uri")
    t_user = st.text_input("Username", st.session_state[SessionKeys.TARGET_USER], key="t_user")
    t_password = st.text_input("Password", st.session_state[SessionKeys.TARGET_PASSWORD], key="t_password", type="password")
    t_db = st.text_input("Database", st.session_state[SessionKeys.TARGET_DATABASE], key="t_db")
    
    # Update session state with current input values
    st.session_state[SessionKeys.TARGET_URI] = t_uri
    st.session_state[SessionKeys.TARGET_USER] = t_user
    st.session_state[SessionKeys.TARGET_PASSWORD] = t_password
    st.session_state[SessionKeys.TARGET_DATABASE] = t_db

    if not validate_required_fields(t_uri, t_password):
        st.info("Enter target database connection details")
        return

    t_creds = Neo4jCredentials(uri=t_uri, username=t_user, password=t_password, database=t_db)
    
    if not validate_connection(t_creds):
        return

    # Transfer options
    add_metadata = st.checkbox(
        "Add transfer metadata",
        value=True,
        help="Adds _original_element_id and _transfer_timestamp to transferred items"
    )

    purge_target = st.checkbox(
        "⚠️ Purge target database before transfer",
        value=False,
        help="WARNING: This will delete ALL existing data in the target database"
    )

    # Transfer execution
    execute_transfer(t_creds, selected_nodes, selected_rels, add_metadata, purge_target)

def execute_transfer(t_creds: Neo4jCredentials, selected_nodes: List[str], 
                    selected_rels: List[str], add_metadata: bool, purge_target: bool):
    """Execute the transfer process"""
    if st.button("Start Transfer", type="primary", use_container_width=True):
        if not selected_nodes:
            st.warning("Select at least one node label to transfer")
            return

        # Handle purge confirmation
        if purge_target and not st.session_state.get(SessionKeys.PURGE_CONFIRMED, False):
            confirm_purge_dialog()
            return

        # Reset confirmation state
        st.session_state.pop(SessionKeys.PURGE_CONFIRMED, None)

        # Create transfer specification
        spec = TransferSpec(
            node_labels=selected_nodes,
            relationship_types=selected_rels,
            should_append_data=add_metadata
        )

        # Execute transfer
        run_transfer(t_creds, spec, purge_target)

def run_transfer(t_creds: Neo4jCredentials, spec: TransferSpec, purge_target: bool):
    """Run the actual transfer process"""
    status_container = st.empty()
    progress_container = st.empty()
    
    try:
        source_creds = Neo4jCredentials(
            uri=st.session_state[SessionKeys.SOURCE_URI],
            username=st.session_state[SessionKeys.SOURCE_USER],
            password=st.session_state[SessionKeys.SOURCE_PASSWORD],
            database=st.session_state[SessionKeys.SOURCE_DATABASE],
        )

        status_container.info("Starting transfer...")

        # Handle database purge
        if purge_target:
            with st.spinner("Purging target database..."):
                reset_target_db(t_creds)
            st.success("Target database purged successfully!")

        # Initialize progress tracking via placeholder for reliable updates
        progress_placeholder = progress_container.empty()
        progress_placeholder.progress(0.0)
        
        # Execute transfer
        transfer_gen = transfer_generator(source_creds, t_creds, spec)
        result = None

        # Some generators yield a controller first; consume it if present
        try:
            first_yield = next(transfer_gen)
            # If the first yield has a controller-like interface, ignore for progress
            # and continue iterating for progress updates
        except StopIteration:
            first_yield = None
        
        for result in transfer_gen:
            # Log yielded object shape
            logger.info(
                "Transfer yield: type=%s, has_float_completed=%s, has_model_dump=%s, has_dict=%s",
                type(result).__name__,
                hasattr(result, "float_completed"),
                hasattr(result, "model_dump"),
                hasattr(result, "dict"),
            )

            # Update progress if possible
            try:
                completion = result.float_completed()
                logger.debug("Transfer progress: completion=%.3f (%d%%)", completion, int(completion * 100))
            except Exception as e:
                # If the yielded item doesn't have progress, skip update
                logger.warning(
                    "Skipping progress update for yield type=%s due to error: %s",
                    type(result).__name__, str(e)
                )
                continue
            completion = max(0.0, min(1.0, float(completion)))
            progress_text = f"Transfer {int(completion * 100)}% complete"
            logger.info("Transfer progress: completion=%.3f (%d%%)", completion, int(completion * 100))
            try:
                progress_placeholder.progress(completion, text=progress_text)
            except TypeError:
                # Fallback for Streamlit versions that don't support 'text' kwarg
                progress_placeholder.progress(completion)
            # Yield a tiny pause to allow the UI to render
            time.sleep(0.01)

        # Log successful transfer
        if result:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state[SessionKeys.TRANSFER_LOG].insert(0, {
                "timestamp": timestamp,
                "transfer_spec": spec.model_dump() if hasattr(spec, 'dict') else str(spec),
                "result": result.model_dump() if hasattr(result, 'dict') else str(result)
            })
            # Ensure the bar shows complete state
            try:
                progress_placeholder.progress(1.0, text="Transfer 100% complete")
            except TypeError:
                progress_placeholder.progress(1.0)
            
            st.success(f"✅ Transfer completed successfully! {result}")
            
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        st.error(f"❌ Transfer failed: {str(e)}")
    finally:
        progress_container.empty()

def render_sidebar():
    """Render the sidebar with transfer history"""
    with st.sidebar:
        st.header("Transfer History")
        
        logs = st.session_state[SessionKeys.TRANSFER_LOG]
        if not logs:
            st.write("*No transfers yet*")
            return

        for idx, log in enumerate(logs):
            timestamp = log.get("timestamp", "Unknown time")
            
            with st.expander(f"Transfer {timestamp}"):
                st.json(log)
                
                if st.button(f"Undo Transfer", key=f"undo_{idx}"):
                    try:
                        # Resolve target credentials from session or environment defaults
                        resolved_t_uri = (
                            st.session_state.get(SessionKeys.TARGET_URI)
                            or os.environ.get("TARGET_NEO4J_URI", "")
                        )
                        resolved_t_user = (
                            st.session_state.get(SessionKeys.TARGET_USER)
                            or os.environ.get("TARGET_NEO4J_USERNAME", "neo4j")
                        )
                        resolved_t_password = (
                            st.session_state.get(SessionKeys.TARGET_PASSWORD)
                            or os.environ.get("TARGET_NEO4J_PASSWORD", "")
                        )
                        resolved_t_db = (
                            st.session_state.get(SessionKeys.TARGET_DATABASE)
                            or os.environ.get("TARGET_NEO4J_DATABASE", "neo4j")
                        )
                        
                        if not resolved_t_uri:
                            raise ValueError("Target database URI is not set. Configure target connection first.")
                        
                        t_creds = Neo4jCredentials(
                            uri=resolved_t_uri,
                            username=resolved_t_user,
                            password=resolved_t_password,
                            database=resolved_t_db
                        )
                        
                        spec = TransferSpec(**log.get("transfer_spec", {}))
                        result = undo(t_creds, spec)
                        st.success(f"Transfer undone: {result}")
                    except Exception as e:
                        st.error(f"Undo failed: {e}")

# Main application
def main():
    """Main application entry point"""
    init_session_state()
    
    st.title("Neo4j Transfer Tool")
    st.markdown("Transfer data between Neo4j databases with ease.")

    # Create main layout
    col1, col2, col3 = st.columns(3)

    with col1:
        s_creds, source_connected = render_source_database_section()

    with col2:
        if source_connected and s_creds:
            selected_nodes, selected_rels, node_count, rel_count = render_transfer_options_section(s_creds)
        else:
            st.info("Connect to source database to see transfer options")
            selected_nodes, selected_rels = None, None

    with col3:
        if selected_nodes is not None:
            render_target_database_section(selected_nodes, selected_rels)
        else:
            st.info("Configure source database and select data to transfer")

    # Render sidebar
    render_sidebar()

if __name__ == "__main__":
    main()