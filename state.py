# state.py
import streamlit as st
import pandas as pd

TIERS = ["Stage", "L0 / Raw", "L1 / Curated", "L2 / Data Product"]


@st.cache_data
def load_rate_card_data():
    """Loads the Databricks rate card from a specific Excel file."""
    try:
        data = pd.read_csv(
            'final_out.csv',
            usecols=['Compute type', 'Instance', 'vCPU', 'Memory (GB)', 'DBU/hour', 'Rate/hour', 'onDemandLinuxHr']
        ) 
        s3_data = pd.read_excel(
            'S3_Storage_cost.xlsx'
        )
        sql_data = pd.read_csv(
            'SQL_warehouse - Sheet1.csv'
        )
        
        # data for Databricks Jobs/Pipelines
        df = data[data['Compute type'].isin([ 'DLT Advanced Compute Photon', 'Jobs Compute', 'Jobs Compute Photon', 'DLT Advanced Compute'])] # Filter for Photon and All-Purpose compute types
        # data for SQL Warehouses
        df_sql = sql_data[sql_data['Compute type'].isin(['SQL Pro Compute', 'SQL Compute'])]
       # data for develoment cost
        df_dev = data[data['Compute type'].isin(['All-Purpose Compute'])]
        # s3 df
        s3_df = s3_data.copy()
        if df.empty or df_sql.empty or df_dev.empty or s3_df.empty:
            st.error("The data is empty or invalid.")
            return None, None, None, None
        return df,df_sql, df_dev, s3_df
    except FileNotFoundError:
        st.error("Rate card file not found. Please ensure 'enterprise_plan.xlsx' is in the same directory.")
        return None, None, None, None
    except Exception as e:
        st.error(f"An error occurred while loading the rate card: {e}")
        return None,None, None, None
    


def populate_global_data(df, df_sql, df_dev, s3_df):
    """
    Populates global dictionaries and lists from the loaded DataFrame.
    This includes grouping instances by their compute type.
    """
    global FLAT_RATE_CARD, FLAT_INSTANCE_LIST, INSTANCE_PRICES, COMPUTE_TYPE_LIST, SQL_WAREHOUSE_SIZES_BY_TYPE, SQL_WAREHOUSE_TYPES_FROM_DATA

    FLAT_RATE_CARD = {
        row['Instance']: row for _, row in df.iterrows()
    }
    FLAT_INSTANCE_LIST = {
        f"{row['Instance']} | {row['vCPU']} CPUs | {row['Memory (GB)']}GB": row['Instance']
        for _, row in df.iterrows()
    }

    COMPUTE_TYPE_LIST = df['Compute type'].unique().tolist()
    
    INSTANCE_PRICES = {}
    for compute_type, group in df.groupby('Compute type'):
        INSTANCE_PRICES[compute_type] = {
            f"{row['Instance']} | {row['vCPU']} CPUs | {row['Memory (GB)']}GB": row['Instance']
            for _, row in group.iterrows()
        }
    # Create a mapping 
    type_name_map = {
        'SQL Compute': 'SQL Compute',
        'SQL Pro Compute': 'SQL Pro Compute'
    }
    
    seen_types = set()
    SQL_WAREHOUSE_TYPES_FROM_DATA = []
    
    for t in df_sql['Compute type'].unique().tolist():
        new_name = type_name_map.get(t, t)
        if new_name not in seen_types:
            SQL_WAREHOUSE_TYPES_FROM_DATA.append(new_name)
            seen_types.add(new_name)
    
    SQL_RATES_BY_TYPE_AND_INSTANCE = {}
    SQL_WAREHOUSE_SIZES_BY_TYPE = {}
    SQL_FLAT_INSTANCE_LIST = {}

    # New dictionary to map driver instance to max worker nodes
    SQL_WORKER_COUNTS_BY_DRIVER = {
        '2X-Small': 1, 'X-Small': 2, 'Small': 4, 'Medium': 8, 'Large': 16,
        'X-Large': 32, '2X-Large': 64, '3X-Large': 128, '4X-Large': 128
    }
 
    for _, row in df_sql.iterrows():
        compute_type_mapped = type_name_map.get(row['Compute type'], row['Compute type'])
        instance_name = row['Instance']
        formatted_size_string = f"{instance_name} - {row['DBU/hour']} DBUs - ${row['Rate/hour']}/hr"
        if compute_type_mapped not in SQL_RATES_BY_TYPE_AND_INSTANCE:
            SQL_RATES_BY_TYPE_AND_INSTANCE[compute_type_mapped] = {}
            SQL_WAREHOUSE_SIZES_BY_TYPE[compute_type_mapped] = {}
        SQL_RATES_BY_TYPE_AND_INSTANCE[compute_type_mapped][instance_name] = row.to_dict()
        SQL_WAREHOUSE_SIZES_BY_TYPE[compute_type_mapped][formatted_size_string] = instance_name
        SQL_FLAT_INSTANCE_LIST[formatted_size_string] = instance_name

        #==> Deveplopment Cost Data
    FLAT_RATE_CARD_DEV = {
        row['Instance']: row for _, row in df_dev.iterrows()
    }
    FLAT_INSTANCE_LIST_DEV= {
        f"{row['Instance']} | {row['DBU/hour']} DBUs | {row['Rate/hour']}/hr": row['Instance']
        for _, row in df_dev.iterrows()
    }
    
    s3_pricing = {}
    for _, row in s3_df.iterrows():
        s3_pricing[row['S3_storage']] = {
            'Rate/GB_50TB': row['Rate/GB_50TB'],
            'Rate/GB_500TB': row['Rate/GB_500TB'],
            'Rate/GB_over500TB': row['Rate/GB_over500TB']
        }

    return {
        'FLAT_RATE_CARD': FLAT_RATE_CARD,
        'FLAT_INSTANCE_LIST': FLAT_INSTANCE_LIST,
        'INSTANCE_PRICES': INSTANCE_PRICES,
        'COMPUTE_TYPE_LIST': COMPUTE_TYPE_LIST,
        
        # Store the new tier-specific data for Jobs/Pipelines
        'COMPUTE_TYPES_L0_Stage': df[df['Compute type'].isin(['DLT Advanced Compute Photon', 'DLT Advanced Compute'])]['Compute type'].unique().tolist(),
        'INSTANCE_PRICES_L0_Stage': {ct: {f"{row['Instance']} | {row['vCPU']} CPUs | {row['Memory (GB)']}GB": row['Instance'] for _, row in group.iterrows()} for ct, group in df[df['Compute type'].isin(['DLT Advanced Compute Photon', 'DLT Advanced Compute'])].groupby('Compute type')},
        'COMPUTE_TYPES_L2_L1': df[df['Compute type'].isin(['Jobs Compute', 'Jobs Compute Photon'])]['Compute type'].unique().tolist(),
        'INSTANCE_PRICES_L2_L1': {ct: {f"{row['Instance']} | {row['vCPU']} CPUs | {row['Memory (GB)']}GB": row['Instance'] for _, row in group.iterrows()} for ct, group in df[df['Compute type'].isin(['Jobs Compute', 'Jobs Compute Photon'])].groupby('Compute type')},


        # sql values ...
        'SQL_RATES_BY_TYPE_AND_INSTANCE': SQL_RATES_BY_TYPE_AND_INSTANCE,
        'SQL_FLAT_INSTANCE_LIST': SQL_FLAT_INSTANCE_LIST,
        'SQL_WAREHOUSE_TYPES_FROM_DATA': SQL_WAREHOUSE_TYPES_FROM_DATA,
        'SQL_WAREHOUSE_SIZES_BY_TYPE': SQL_WAREHOUSE_SIZES_BY_TYPE,
        'SQL_WORKER_COUNTS_BY_DRIVER': SQL_WORKER_COUNTS_BY_DRIVER

        # DEVELOPMENT COST DATA
        ,'FLAT_RATE_CARD_DEV': FLAT_RATE_CARD_DEV,
        'FLAT_INSTANCE_LIST_DEV': FLAT_INSTANCE_LIST_DEV,

        #S3 data
        'S3_PRICING': s3_pricing
    }

def initialize_state():
    
    # Load and populate global data first
    if 'global_data_populated' not in st.session_state or not st.session_state.global_data_populated:
        df, df_sql,df_dev, s3_df = load_rate_card_data()
        if df is None or df_sql.empty:
        # Handle the error gracefully
            st.error("The jobs or SQL dataframes are empty. Please check your data source.")
            return
            
        # Populate the global data dictionary in session state
        st.session_state.global_data = populate_global_data(df, df_sql, df_dev, s3_df)
        st.session_state.global_data_populated = True

    if 'dbx_jobs' not in st.session_state:
        st.session_state.dbx_jobs = {}
        global_data = st.session_state.global_data

        for tier in TIERS:
            if tier in ["L0 / Raw", "Stage"]:
                default_compute_type = global_data['COMPUTE_TYPES_L0_Stage'][0] if global_data['COMPUTE_TYPES_L0_Stage'] else None
                instance_prices_for_tier = global_data['INSTANCE_PRICES_L0_Stage']
            elif tier in ["L2 / Data Product","L1 / Curated"]:
                default_compute_type = global_data['COMPUTE_TYPES_L2_L1'][0] if global_data['COMPUTE_TYPES_L2_L1'] else None
                instance_prices_for_tier = global_data['INSTANCE_PRICES_L2_L1']
            else:
                default_compute_type = None
                instance_prices_for_tier = {}
            
            default_instance_list = list(instance_prices_for_tier.get(default_compute_type, {}).keys())
            default_instance = default_instance_list[0] if default_instance_list else None

            # Use a DataFrame instead of a list of dicts for easier editing
            st.session_state.dbx_jobs[tier] = pd.DataFrame([{
                "Job Name": f"{tier.replace('/', ' ')} Job 1",
                "Runtime (hrs)": 0.0,
                "Runs/Month": 0.0,
                "Compute type": default_compute_type,
                "Instance Type": default_instance,
                "Nodes": 1,
            }])
            
    # S3 state
    if 's3_calc_method' not in st.session_state:
        st.session_state.s3_calc_method = "Direct Storage[Recommended]"

    # Fetch the list of S3 storage classes from your loaded data
    global_data = st.session_state.get('global_data', {})
    s3_pricing_data = global_data.get('S3_PRICING', {})
    s3_classes_list = list(s3_pricing_data.keys())
    
    # Safely get the first class as the default
    default_s3_class = s3_classes_list[0] if s3_classes_list else "Standard"
    
    if 's3_direct' not in st.session_state:
        st.session_state.s3_direct = {
            "Landing Zone": {"class": default_s3_class, "amount": 0, "unit": "GB", "monthly_growth_percent": 0.0},
            "Stage": {"class": default_s3_class, "amount": 0, "unit": "GB", "monthly_growth_percent": 0.0},
            "L0 / Raw": {"class": default_s3_class, "amount": 0, "unit": "GB", "monthly_growth_percent": 0.0},
            "L1 / Curated": {"class": default_s3_class, "amount": 0, "unit": "GB",  "monthly_growth_percent": 0.0},
            "L2 / Data Product": {"class": default_s3_class, "amount": 0, "unit": "GB",  "monthly_growth_percent": 0.0},
        }
    
    # Ensure existing s3_direct entries have 'monthly_growth_percent'
    for zone, config in st.session_state.s3_direct.items():

        if 'monthly_growth_percent' not in config:
            config['monthly_growth_percent'] = 0.0

    if 's3_table_based' not in st.session_state:
        st.session_state.s3_table_based = {
            "Source System Table": [{"Table Name": "Source_system_Table_1", "Records": 0, "Columns": 0, "Table" : 0, "Avg_Column_length" : 0}], 
            "L0 / Raw":  [{"Table Name": "Bronze_Table_1", "Records": 0, "Columns": 0, "Table" : 0, "Avg_Column_length" : 0 }], 
            "L1 / Curated":  [{"Table Name": "Silver_Table_1", "Records": 0, "Columns": 0, "Table" : 0, "Avg_Column_length" : 0}], 
            "L2 / Data Product":    [{"Table Name": "Gold_Table_1", "Records": 0, "Columns": 0,"Table" : 0, "Avg_Column_length" : 0}], 
        }
    else: # Ensure existing entries also get 'Columns' if they are old format        
        for zone_name, table_configs in st.session_state.s3_table_based.items():
            if isinstance(table_configs, dict) and "records" in table_configs:
                st.session_state.s3_table_based[zone_name] = [{
                    "Table Name": f"{zone_name.replace(' / ', '_')} Table 1",
                    "Records": table_configs.get("records", 0),
                    "Columns": 0 ,# Default new column count,
                    "Table" : 0, 
                    "Avg_Column_length" : 0
                }]
            elif isinstance(table_configs, list):
                # Ensure each item in the list has 'Columns' AND 'Table'
                for i, table_config in enumerate(table_configs):
                    if 'Columns' not in table_config:
                        st.session_state.s3_table_based[zone_name][i]['Columns'] = 0
                    if 'Table' not in table_config: # New check for the 'Table' column
                        st.session_state.s3_table_based[zone_name][i]['Table'] = 0
                    if 'Avg_Column_length' not in table_config: # New check for the 'Table' column
                        st.session_state.s3_table_based[zone_name][i]['Avg_Column_length'] = 0

#------------------------------------------------------------------------------------------------------------------
    # SQL Warehouse state
    global_data = st.session_state.get('global_data', {})
    sql_warehouse_types = global_data.get('SQL_WAREHOUSE_TYPES_FROM_DATA', [])
    sql_warehouse_sizes_by_type = global_data.get('SQL_WAREHOUSE_SIZES_BY_TYPE', {})

    if 'sql_warehouses' not in st.session_state:
            # FIX: Access the dictionaries correctly
        default_type = sql_warehouse_types[0] if sql_warehouse_types else None
        default_size = next(iter(sql_warehouse_sizes_by_type.get(default_type, {})), None)
    
        st.session_state.sql_warehouses = [{
            "id": "warehouse_0", 
            "name": "Primary BI Warehouse", 
            "type": default_type, 
            "size": default_size,
            'SQL_nodes': 1,
            "hours_per_day": 0, 
            "days_per_month": 0, 
            "auto_suspend": True, 
            "suspend_after": 10
        }]

# --------------------------------------------------
    # Development Cost state
    global_data = st.session_state.get('global_data', {})
    if 'dev_costs' not in st.session_state:
        # Safely get the list of instance names for 'All-Purpose Compute'
        dev_instance_list = list(global_data.get('FLAT_INSTANCE_LIST_DEV', {}).keys())
        default_instance = dev_instance_list[0] if dev_instance_list else None
        
        st.session_state.dev_costs = pd.DataFrame([{ 
            "Compute_type": "All-Purpose Compute",
            "Driver type": default_instance,
            "Worker Type": default_instance, 
            "Nodes": 1,
            "hr_per_month": 0, 
            "no_of_Month": 0,
            "DBX": 0.0,
        }])
    #---------------------------------------------------------------
    #Ensure existing SQL warehouses have 'type'
    for warehouse in st.session_state.sql_warehouses:
        if 'type' not in warehouse:
            warehouse['type'] = sql_warehouse_types[0]

    # Monthly Growth Rate for Databricks (used in overall projection, but no longer an input in summary)
    if 'monthly_growth_percent' not in st.session_state:
        st.session_state.monthly_growth_percent = 0.0

    # Theme state
    if 'theme' not in st.session_state:
        st.session_state.theme = 'Dark' if st.session_state.get('dark_mode', False) else 'Light'