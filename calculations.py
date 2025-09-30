# calculations.py
import streamlit as st
import pandas as pd
import state as s
def calculate_databricks_costs_for_tier(jobs_df):
    """Calculates the costs for a given list of job dictionaries."""
    if jobs_df.empty:
        cols = ["Job Name", "Runtime (hrs)", "Runs/Month", "Compute type", "Instance Type", "Nodes", "DBU", "DBX", "EC2"]
        return pd.DataFrame(columns=cols), 0, 0

    df = jobs_df.copy()
    
    def get_rates(row):
        instance_name = s.FLAT_INSTANCE_LIST.get(row['Instance Type'])
        rate_card_row = s.FLAT_RATE_CARD.get(instance_name)
        if rate_card_row is not None:
            return rate_card_row.get('DBU/hour', 0), rate_card_row.get('Rate/hour', 0),rate_card_row.get('onDemandLinuxHr', 0)
        return 0, 0

    df[['dbu_per_hour', 'rate_per_hour', 'EC2_hr_rate']] = df.apply(lambda row: pd.Series(get_rates(row)), axis=1)
    
    # Calculate DBU units
    df['DBU_Units'] = (df["Nodes"] + 1) * df["Runtime (hrs)"] * df["Runs/Month"]
    
    # Calculate costs
    df['DBU'] = df['dbu_per_hour'] * (df["Nodes"] + 1) * df["Runtime (hrs)"] * df["Runs/Month"]
    df['EC2'] = df['EC2_hr_rate'] * (df["Nodes"] + 1) * df["Runtime (hrs)"] * df["Runs/Month"]
    df['DBX'] = df['rate_per_hour'] * (df["Nodes"] + 1) * df["Runtime (hrs)"] * df["Runs/Month"]
    
    total_dbx_cost = df['DBX'].sum()
    total_ec2_cost = df['EC2'].sum()
    total_dbus = df['DBU'].sum()
    
    # Clean up intermediate columns before returning
    df = df.drop(columns=['dbu_per_hour', 'rate_per_hour', 'DBU_Units', 'EC2_hr_rate'], errors='ignore')

    
    return df, total_dbx_cost, total_ec2_cost,total_dbus 

def calculate_s3_cost_per_zone():
    """
    Calculates S3 cost for each individual zone, the total current cost,
    and the total 12-month projected cost.
    """
    current_costs_per_zone = {}
    total_s3_cost = 0
    total_table_cost = 0
    total_quarterly_cost = 0
    total_half_yearly_cost = 0
    total_yearly_cost = 0

    global_data = st.session_state.get('global_data', {})
    S3_PRICING = global_data.get('S3_PRICING', {})
    bpc = 1
    cr = 0.5

    if st.session_state.s3_calc_method == "Direct Storage[Recommended]":
        s3_direct_tiers = ["Landing Zone", "L0 / Raw", "L1 / Curated", "L2 / Data Product"]
        if st.session_state.get('enable_s3_stage', True):
            s3_direct_tiers.insert(1, "Stage")
        
        for zone in s3_direct_tiers:
            config = st.session_state.s3_direct.get(zone, {})
            
            storage_gb = config.get("amount", 0) * 1024 if config.get("unit") == "TB" else config.get("amount", 0)
            pricing_rates = S3_PRICING.get(config.get("class"), {})

            if storage_gb <= 50 * 1024:
                rate_per_gb = pricing_rates.get('Rate/GB_50TB', 0)
            elif storage_gb <= 500 * 1024:
                rate_per_gb = pricing_rates.get('Rate/GB_500TB', 0)
            else:
                rate_per_gb = pricing_rates.get('Rate/GB_over500TB', 0)
            
            zone_current_cost = storage_gb * rate_per_gb
            current_costs_per_zone[zone] = zone_current_cost
            total_s3_cost += zone_current_cost

            monthly_growth_percent = config.get("monthly_growth_percent", 0.0)
            if monthly_growth_percent > 0:
                growth_factor = 1 + (monthly_growth_percent / 100)
                quarterly_projected_cost = zone_current_cost * ((growth_factor**3 - 1) / (growth_factor - 1))
                half_yearly_projected_cost = zone_current_cost * ((growth_factor**6 - 1) / (growth_factor - 1))
                yearly_projected_cost = zone_current_cost * ((growth_factor**12 - 1) / (growth_factor - 1))
            else:
                quarterly_projected_cost = zone_current_cost * 3
                half_yearly_projected_cost = zone_current_cost * 6
                yearly_projected_cost = zone_current_cost * 12
            
            # Store the new costs in the session state directly
            st.session_state.s3_direct[zone]['quarterly_cost'] = quarterly_projected_cost
            st.session_state.s3_direct[zone]['half_yearly_cost'] = half_yearly_projected_cost
            st.session_state.s3_direct[zone]['yearly_cost'] = yearly_projected_cost
            
            total_quarterly_cost += quarterly_projected_cost
            total_half_yearly_cost += half_yearly_projected_cost
            total_yearly_cost += yearly_projected_cost
            
    else:
        total_table_cost = 0 
        standard_pricing = 0.023

        for zone, list_of_table_configs in st.session_state.s3_table_based.items():
            zone_estimated_gb = 0
            if isinstance(list_of_table_configs, list):
                for table_config in list_of_table_configs:
                    if isinstance(table_config, dict):
                        records = float(table_config.get("Records", 0) or 0)
                        num_columns = float(table_config.get("Columns", 0) or 0)
                        num_tables = float(table_config.get("Table", 0) or 0)
                        num_length = float(table_config.get("Avg_Column_length", 0) or 0)

                        # Size_bytes ≈ R × C × L × bpc × cr
                        size_bytes = records * num_columns * num_length * bpc * cr
                        
                        # Convert bytes to GB: bytes / (1024^3)
                        estimated_gb_for_table = size_bytes / (1024 ** 3)
                        
                        # Add the estimated size for all tables in the zone
                        zone_estimated_gb += estimated_gb_for_table * num_tables
                        
            zone_current_cost = zone_estimated_gb * standard_pricing
            current_costs_per_zone[zone] = zone_current_cost
            total_table_cost += zone_current_cost



    return current_costs_per_zone, total_s3_cost, total_quarterly_cost, total_half_yearly_cost, total_yearly_cost, total_table_cost

def calculate_sql_warehouse_cost():
    """Calculates total DBU and EC2 cost and total DBUs from session state."""
    total_sql_dbu_cost = 0
    total_sql_ec2_cost = 0
    total_dbus = 0
    
    global_data = st.session_state.get('global_data', {})
    sql_rates_by_type_and_instance = global_data.get('SQL_RATES_BY_TYPE_AND_INSTANCE', {})
    sql_flat_instance_list = global_data.get('SQL_FLAT_INSTANCE_LIST', {})

    for warehouse in st.session_state.sql_warehouses:
        sql_nodes = warehouse.get("SQL_nodes", 1)
        
        if warehouse.get("hours_per_day", 0) > 0 and warehouse.get("days_per_month", 0) > 0 and sql_nodes > 0:
            warehouse_type = warehouse.get("type")
            size_string = warehouse.get("size")
            
            instance_name = sql_flat_instance_list.get(size_string)
            rates = sql_rates_by_type_and_instance.get(warehouse_type, {}).get(instance_name, {})
            
            dbu_rate_per_hr = rates.get('Rate/hour', 0)
            dbt_per_hr = rates.get('DBU/hour', 0)
            ec2_rate_per_hr = rates.get('onDemandLinuxHr', 0)
            
            hours_per_month = warehouse.get("hours_per_day", 0) * warehouse.get("days_per_month", 0)
            
            # Calculate costs for both DBU and EC2
            dbu_cost = dbu_rate_per_hr * hours_per_month * sql_nodes
            ec2_cost =  dbu_rate_per_hr + (ec2_rate_per_hr *sql_nodes)
            dbus_used = dbt_per_hr * hours_per_month * sql_nodes
            
            total_sql_dbu_cost += dbu_cost
            total_sql_ec2_cost += ec2_cost
            total_dbus += dbus_used
            
    return total_sql_dbu_cost, total_sql_ec2_cost, total_dbus

def calculate_dev_costs():
    if 'dev_costs' not in st.session_state or st.session_state.dev_costs.empty:
        # Return a DataFrame with all columns, initialized to handle the empty state
        dev_df = pd.DataFrame(columns=[
            "Compute_type", "Driver type", "Worker Type", "Nodes", "hr_per_month", 
            "no_of_Month", "DBX", "EC2", "Total"
        ])
        return 0.0, 0.0, dev_df

    dev_df = st.session_state.dev_costs.copy()
    global_data = st.session_state.global_data
    
    def get_rates(instance_key):
        instance_name = global_data['FLAT_INSTANCE_LIST_DEV'].get(instance_key)
        rate_info = global_data['FLAT_RATE_CARD_DEV'].get(instance_name, {})
        return rate_info.get('Rate/hour', 0.0), rate_info.get('onDemandLinuxHr', 0.0)

    # Use .get() with a default value to handle cases where columns might be missing
    driver_rates = dev_df['Driver type'].apply(lambda x: get_rates(x))
    worker_rates = dev_df['Worker Type'].apply(lambda x: get_rates(x))
    
    dev_df['driver_dbu_rate'] = driver_rates.apply(lambda x: x[0])
    dev_df['driver_ec2_rate'] = driver_rates.apply(lambda x: x[1])
    dev_df['worker_dbu_rate'] = worker_rates.apply(lambda x: x[0])
    dev_df['worker_ec2_rate'] = worker_rates.apply(lambda x: x[1])

    dev_df['DBX'] = (dev_df['driver_dbu_rate'] + (dev_df['worker_dbu_rate'] * dev_df['Nodes'])) * dev_df['hr_per_month'] * dev_df['no_of_Month']
    dev_df['EC2'] = (dev_df['driver_ec2_rate'] + (dev_df['worker_ec2_rate'] * dev_df['Nodes'])) * dev_df['hr_per_month'] * dev_df['no_of_Month']
    
    # Calculate the Total Cost within the calculation function
    dev_df['Total'] = dev_df['DBX'] + dev_df['EC2']

    dev_df = dev_df.drop(columns=['driver_dbu_rate', 'driver_ec2_rate', 'worker_dbu_rate', 'worker_ec2_rate'], errors='ignore')

    st.session_state.dev_costs = dev_df
    
    total_dbx_cost = dev_df['DBX'].sum()
    total_ec2_cost = dev_df['EC2'].sum()
    
    return total_dbx_cost, total_ec2_cost, dev_df