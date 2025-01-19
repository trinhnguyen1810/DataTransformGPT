import streamlit as st
import pandas as pd
from src.core import TransformCore
from config import MAX_FILE_SIZE, SUPPORTED_FORMATS

# Initialize
if 'transformer' not in st.session_state:
    st.session_state.transformer = TransformCore()

# Page Config
st.set_page_config(page_title="transform", layout="wide")

# Main App
st.title("transform")
st.markdown("Transform your data using natural language")

@st.cache_data
def load_large_file(file):
    try:
        if file.name.endswith('.csv'):
            return pd.read_csv(file)
        else:
            return pd.read_excel(file)
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
        return None


# File Upload with size check
uploaded_file = st.file_uploader("Upload your dataset", type=SUPPORTED_FORMATS)

if uploaded_file:
    # Check file size first
    file_size = uploaded_file.size
    if file_size > MAX_FILE_SIZE:
        st.error(f"File size ({file_size/1024/1024:.1f}MB) exceeds the maximum limit of 50MB")
    else:
        try:
            # Load file with progress indication
            with st.spinner("Loading file..."):
                df = load_large_file(uploaded_file)
                
            if df is not None:
                # Data Preview
                st.subheader("Data Preview")
                st.dataframe(df.head())
                
                # Operation Selection
                operation_type = st.radio(
                    "Select Operation Type",
                    ["Transform Existing Columns", "Generate New Column", "Both"],
                    help="Choose what type of operation to perform"
                )
                
                # Initialize variables
                column_commands = {}
                new_column_config = None
                
                # Transform Existing Columns UI
                if operation_type in ["Transform Existing Columns", "Both"]:
                    st.divider()
                    st.subheader("Transform Existing Columns")
                    
                    # Column Selection
                    selected_columns = st.multiselect(
                        "Select columns to transform",
                        df.columns.tolist()
                    )
                    
                    if selected_columns:
                        for col in selected_columns:
                            with st.expander(f"Transform {col}", expanded=True):
                                # Output option
                                output_type = st.radio(
                                    f"Output for {col}",
                                    ["Replace original", "Create new column"],
                                    key=f"output_{col}"
                                )
                                
                                # New column name if needed
                                if output_type == "Create new column":
                                    new_name = st.text_input(
                                        f"New column name",
                                        key=f"name_{col}",
                                        placeholder=f"{col}_transformed"
                                    )
                                else:
                                    new_name = col
                                
                                # Transformation command
                                command = st.text_area(
                                    f"Transformation command",
                                    key=f"command_{col}",
                                    placeholder="Example: 'Convert to numbered steps'"
                                )
                                
                                if command:  # Only add if command is provided
                                    column_commands[col] = {
                                        'command': command,
                                        'output': output_type,
                                        'new_name': new_name or f"{col}_transformed"
                                    }
                
                # Generate New Column UI
                if operation_type in ["Generate New Column", "Both"]:
                    st.divider()
                    st.subheader("Generate New Column")
                    
                    # New column configuration
                    new_col_name = st.text_input(
                        "Name for new column",
                        placeholder="e.g., cooking_time, difficulty_level"
                    )
                    
                    if new_col_name:
                        # Select reference columns
                        reference_cols = st.multiselect(
                            "Select columns to base generation on",
                            df.columns.tolist(),
                            help="Which columns should be used to generate the new column?"
                        )
                        
                        if reference_cols:
                            # Description of what to generate
                            generation_prompt = st.text_area(
                                "Describe what to generate",
                                placeholder="e.g., 'Generate estimated cooking time in minutes'"
                            )
                            
                            if generation_prompt:
                                st.warning("⚠️ Note: Generated data is estimated and should be verified for accuracy.")
                                new_column_config = {
                                    'name': new_col_name,
                                    'reference_cols': reference_cols,
                                    'prompt': generation_prompt
                                }
                
                # Row Selection
                if column_commands or new_column_config:
                    st.divider()
                    st.subheader("Row Selection")
                    use_specific_rows = st.checkbox("Apply to specific rows only?")
                    search_description = None
                    
                    if use_specific_rows:
                        search_description = st.text_input(
                            "Describe what rows to find",
                            placeholder="Example: 'Find recipes that mention quick or easy'"
                        )
                    
                    # Transform Button
                    st.divider()
                    if st.button("Transform Data", type="primary"):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        def update_progress(progress):
                            progress_bar.progress(progress)
                            status_text.text(f"Processing... {progress*100:.0f}%")
                        
                        try:
                            with st.spinner("Processing..."):
                                result_df = df.copy()
                                error = None
                                
                                # Handle transformations
                                if column_commands:
                                    result_df, error = st.session_state.transformer.process_dataframe(
                                        df=result_df,
                                        column_commands=column_commands,
                                        search_description=search_description,
                                        progress_callback=update_progress
                                    )
                                
                                # Handle new column generation
                                if not error and new_column_config:
                                    result_df, error = st.session_state.transformer.generate_column(
                                        df=result_df,
                                        reference_columns=new_column_config['reference_cols'],
                                        new_column_name=new_column_config['name'],
                                        generation_prompt=new_column_config['prompt'],
                                        progress_callback=update_progress
                                    )
                                
                                if error:
                                    st.error(error)
                                else:
                                    st.success("Transformation completed!")
                                    
                                    # Results View
                                    st.subheader("Results")
                                    
                                    # Calculate columns to display
                                    original_cols = list(column_commands.keys()) if column_commands else []
                                    transformed_cols = [cmd['new_name'] for cmd in column_commands.values()]
                                    if new_column_config:
                                        transformed_cols.append(new_column_config['name'])
                                    
                                    # Show original vs transformed
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.markdown("**Original Data**")
                                        if original_cols:
                                            st.dataframe(df[original_cols])
                                        else:
                                            st.info("No columns were transformed")
                                    
                                    with col2:
                                        st.markdown("**Transformed Data**")
                                        st.dataframe(result_df[transformed_cols])
                                    
                                    # Download option
                                    st.download_button(
                                        "Download Results",
                                        result_df.to_csv(index=False),
                                        "transformed_data.csv",
                                        "text/csv",
                                        use_container_width=True
                                    )
                                    
                        except Exception as e:
                            st.error(f"Error during transformation: {str(e)}")
                        finally:
                            progress_bar.empty()
                            status_text.empty()
        
        except Exception as e:
            st.error(f"Error: {str(e)}")

# Cleanup
def cleanup():
    if 'transformer' in st.session_state:
        st.session_state.transformer.close()

import atexit
atexit.register(cleanup)