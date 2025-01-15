import streamlit as st
import pandas as pd
from src.core import TransformCore

# Initialize
if 'transformer' not in st.session_state:
    st.session_state.transformer = TransformCore()

# Page Config
st.set_page_config(page_title="transform", layout="wide")

# Main App
st.title("transform")
st.markdown("Transform your data using natural language")

# File Upload
uploaded_file = st.file_uploader("Upload your dataset", type=['csv', 'xlsx'])

if uploaded_file:
    try:
        # Read file
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        # Data Preview
        st.subheader("Data Preview")
        st.dataframe(df.head())
        
        # Column Selection
        selected_columns = st.multiselect(
            "Select columns to transform",
            df.columns.tolist()
        )
        
        if selected_columns:
            # Column-specific commands
            st.subheader("Transformation Commands")
            
            column_commands = {}
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
            
            # Generate New Column Section
            st.divider()
            st.subheader("Generate New Column")
            add_new_column = st.checkbox("Generate a new column based on existing data")

            if add_new_column:
                # Specify new column
                new_col_name = st.text_input(
                    "Name for new column",
                    placeholder="e.g., cooking_time, difficulty_level"
                )
                
                # Select reference columns
                reference_cols = st.multiselect(
                    "Select columns to base generation on",
                    df.columns.tolist(),
                    help="Which columns should be used to generate the new column?"
                )
                
                # Description of what to generate
                generation_prompt = st.text_area(
                    "Describe what to generate",
                    placeholder="e.g., 'Generate estimated cooking time in minutes based on the instructions'"
                )
                
                if generation_prompt:
                    st.warning("⚠️ Note: Generated data is estimated and should be verified for accuracy.")
            
            # Row Selection
            st.subheader("Row Selection")
            use_specific_rows = st.checkbox("Transform specific rows only?")
            search_description = None
            
            if use_specific_rows:
                search_description = st.text_input(
                    "Describe what rows to find",
                    placeholder="Example: 'Find recipes that mention quick or easy'"
                )
                
                if search_description:
                    st.info("This will use NLP to find relevant rows based on your description")
            
            # Transform Button
            if st.button("Transform"):
                with st.spinner("Processing..."):
                    # Handle regular transformations
                    if column_commands:
                        result_df, error = st.session_state.transformer.process_dataframe(
                            df=df,
                            column_commands=column_commands,
                            search_description=search_description
                        )
                        if error:
                            st.error(error)
                            
                    # Handle new column generation if requested
                    if add_new_column and new_col_name and reference_cols and generation_prompt:
                        if 'result_df' not in locals():
                            result_df = df.copy()
                            
                        result_df, gen_error = st.session_state.transformer.generate_column(
                            result_df,
                            reference_cols,
                            new_col_name,
                            generation_prompt
                        )
                        if gen_error:
                            st.error(gen_error)
                    
                    # Show results if we have any
                    if 'result_df' in locals():
                        st.subheader("Results")
                        
                        # Show original vs transformed
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Original Data**")
                            st.dataframe(df[selected_columns])
                        
                        with col2:
                            st.markdown("**Transformed Data**")
                            # Get all columns to display
                            display_cols = []
                            if column_commands:
                                display_cols.extend(cmd['new_name'] for cmd in column_commands.values())
                            if add_new_column and new_col_name:
                                display_cols.append(new_col_name)
                            st.dataframe(result_df[display_cols])
                        
                        # Download option
                        csv = result_df.to_csv(index=False)
                        st.download_button(
                            "Download Results",
                            csv,
                            "transformed_data.csv",
                            "text/csv"
                        )
    
    except Exception as e:
        st.error(f"Error: {str(e)}")

# Cleanup
def cleanup():
    if 'transformer' in st.session_state:
        st.session_state.transformer.close()

import atexit
atexit.register(cleanup)