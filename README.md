# DataTransformer

DataTransformer is a powerful data transformation tool that leverages Snowflake's Mistral LLM capabilities to intelligently transform and generate data using natural language commands.

I realized that while many projects focus on natural language to SQL (which helps query data), there's a gap in actually enriching and transforming the data itself. What if we could use AI not just to query data, but to understand it deeply enough to generate new insights and transformations? This is where DataTransformer comes in - it's not about querying existing data, it's about intelligently enriching it. By combining Snowflake's Mistral LLM with distributed processing, we can analyze context across multiple columns to generate new, meaningful data that wasn't explicitly present in the original dataset. Furthermore, it can transform your old data into your desired output (even filter rows using human-like search criteria). I also ensure that data integrity is maintained and that the transformed or newly generated data is as close as possible to the ground truth of the actual data itself.

## Features

- **Natural Language Data Transformation**: Transform data columns using simple English commands
- **Distributed Processing**: Handles large datasets efficiently using Redis-based distributed processing
- **Smart Row Filtering**: Filter rows using natural language descriptions
- **Column Generation**: Generate new columns based on existing data using natural language prompts
- **Batch Processing**: Optimized chunk-based processing for better performance
- **Fallback Mode**: Gracefully handles situations when distributed processing isn't available

## Technical Implementation

### Core Components:
- **Snowflake Mistral LLM**: Powers natural language understanding and data transformation
- **Redis**: Manages distributed task queue and result aggregation
- **Streamlit**: Provides intuitive user interface
- **Pandas**: Handles data manipulation and processing

### Architecture:
- **Distributed Core**: Splits large datasets into chunks for parallel processing
- **Worker Processes**: Handle individual data chunks independently
- **Task Queue**: Manages work distribution and progress tracking
- **Result Aggregation**: Combines processed chunks while maintaining data integrity

## Usage

1. Upload your dataset (CSV or Excel)
2. Choose operation type:
   - Transform existing columns
   - Generate new columns
   - Both
3. For transformations:
   - Select columns to transform
   - Enter natural language commands
   - Choose output options (replace or new column)
4. For generation:
   - Name your new column
   - Select reference columns
   - Describe what to generate
5. Optionally filter rows using natural language
6. Click "Transform Data" to process
7. Download transformed results

## Installation

1. **Clone and Setup**
```bash
git clone https://github.com/trinhnguyen1810/DataTransformGPT.git
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

### Start Redis (optional - for distributed mode)
```bash
brew services start redis
```

### Start worker processes (optional - for distributed mode)
```bash
 ./scripts/start_workers.sh
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

 I architected the solution with three main components:
- Frontend Layer: Built with Streamlit for an intuitive user interface
- Processing Layer: Implemented distributed processing using Redis for scalability
- AI Layer: Leveraged Snowflake's Mistral LLM for natural language understanding and data transformation
The system uses a distributed architecture where tasks are split into chunks and processed in parallel, with results aggregated back to provide a seamless user experience.

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
- Support additional formats, including non-relational ones like JSON.
- Combine datasets to generate data that maintains the ground truth of both sources.
- Improve scalability and performance for faster processing.
- Implement transformation history tracking.
