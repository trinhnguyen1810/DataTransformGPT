# DataTransformGPT

A data transformation tool that leverages Snowflake Cortex and Mistral LLM for intelligent data manipulation using natural language commands.

## Core Features

### Column Transformations
- Transform existing columns with natural language
- Replace or create new columns
- Bulk transformations with custom commands

### Data Generation
- Generate new columns based on existing data
- Smart data inference with accuracy indicators
- Context-aware data generation

### Intelligent Row Selection
- Natural language row filtering
- Context-based row matching
- Custom selection criteria

## Installation

1. **Clone and Setup**
```bash
git clone [your-repo]
cd transform_app
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure Snowflake**

Create `.env` file:
```plaintext
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Usage

### Start App
```bash
streamlit run app.py
```

### Transform Data
1. Upload CSV/Excel file
2. Select columns
3. Enter transformation commands
4. Generate new data (optional)
5. Download results

### Example Commands

#### Transform Existing Data
- "Convert text to numbered steps"
- "Break into detailed bullet points"
- "Make instructions more professional"

#### Generate New Data
- "Generate cooking time based on instructions"
- "Create difficulty rating from steps"
- "Extract key metrics from text"

## Technical Stack

- **Frontend**: Streamlit
- **Data Processing**: Pandas
- **LLM**: Mistral on Snowflake Cortex
- **Storage**: Snowflake

## Requirements

- Python 3.8+
- Snowflake account with Cortex access
- Mistral LLM access via Snowflake
- Required Python packages:
  - streamlit
  - snowflake-connector-python
  - pandas
  - python-dotenv

## Limitations

- Max file size: 5MB
- Supported formats: CSV, Excel
- Generated data requires verification
- Active Snowflake connection needed

## Future Improvements

- Batch processing support
- Custom transformation templates
- Advanced data validation
- More output formats
- Transformation history
