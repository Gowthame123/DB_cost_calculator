# main.py
import streamlit as st
import state as s
from calculations import calculate_databricks_costs_for_tier, calculate_s3_cost_per_zone, calculate_sql_warehouse_cost, calculate_dev_costs
from ui_components import render_summary_column, render_databricks_tab, render_s3_tab, render_sql_warehouse_tab, render_configuration_guide, render_export_button , render_devepoment_tools, render_calcu_explain 
from file_exportor import generate_consolidated_excel_export 
import io 
import pandas as pd


# --- Page Configuration ---
st.set_page_config(
    page_title="Cloud Cost Calculator",
    page_icon="🧮",
    layout="wide"
)

# --- 1. Initialize Session State ---
s.initialize_state()
df_rate_card, df_sql_rate_card, df_dev, s3_data = s.load_rate_card_data()

# Check if data loaded successfully (either df could be None)
if df_rate_card is None or df_sql_rate_card is None or df_dev is None:
    st.stop()

# Now, pass both dataframes to populate_global_data()
s.populate_global_data(df_rate_card, df_sql_rate_card, df_dev, s3_data)

# This is for Databricks overall growth, not S3 per-zone growth
if 'monthly_growth_percent' not in st.session_state:
    st.session_state.monthly_growth_percent = 0.0

if 'theme' not in st.session_state:
    st.session_state.theme = 'light'
    
# --- 2. Perform All Calculations ---
calculated_dbx_data = {}

# A safe way to handle the toggle is to build a list of active tiers first.
# Ensure 'enable_bronze' is initialized
if 'enable_bronze' not in st.session_state:
    st.session_state.enable_bronze = True

active_tiers = s.TIERS.copy()
if not st.session_state.enable_bronze:
    active_tiers.remove("L0 / RAW")

for tier in active_tiers:
    # Use .get() to safely retrieve the DataFrame, defaulting to an empty DataFrame if the key doesn't exist.
    jobs_df = st.session_state.dbx_jobs.get(tier, pd.DataFrame())
    if not jobs_df.empty:
        df_with_costs, dbu_cost, ec2_cost, _ = calculate_databricks_costs_for_tier(jobs_df)
        calculated_dbx_data[tier] = {
            "df": df_with_costs,
            "dbu_cost": dbu_cost,
            "ec2_cost": ec2_cost
        }
    else:
        # If the tier is active but has no jobs, initialize it with empty costs
        calculated_dbx_data[tier] = {
            "df": pd.DataFrame(),
            "dbu_cost": 0,
            "ec2_cost": 0
        }

# This line unpacks the return values, which are now correctly handled
s3_costs_per_zone, s3_cost, total_quarterly_cost, total_half_yearly_cost, projected_s3_cost_12_months, total_table_cost = calculate_s3_cost_per_zone()
sql_dbu_cost, sql_ec2_cost, sql_dbu = calculate_sql_warehouse_cost()
dev_dbx_cost, dev_ec2_cost, _ = calculate_dev_costs()
dev_cost = dev_dbx_cost + dev_ec2_cost
databricks_total_cost = sum(data['dbu_cost'] + data['ec2_cost'] for data in calculated_dbx_data.values())
total_cost = databricks_total_cost + s3_cost + sql_dbu_cost + sql_ec2_cost + dev_cost + total_table_cost
quarterly_total_cost = total_cost * 3
half_yearly_total_cost = total_cost * 6
yearly_total_cost = total_cost * 12
# --- 3. Render Main Layout ---
title_col, controls_col = st.columns([4, 1])

with title_col:
    st.title("☁️ Cloud Cost Calculator")
    st.caption("Databricks & AWS Cost Estimation")

with controls_col:
    # Arrange theme toggle and export button horizontally
    export_col, theme_col = st.columns(2)

    with export_col:
        # Generate Excel file content
        render_export_button(
            calculated_dbx_data, # Pass the local variable here
            st.session_state.s3_calc_method,
            st.session_state.s3_direct,
            st.session_state.s3_table_based,
            st.session_state.sql_warehouses, 
            st.session_state.dev_costs,
            s3_cost,  
            sql_dbu_cost,
            sql_ec2_cost, 
            databricks_total_cost, 
            dev_cost, 
            total_monthly_summarized_cost=total_cost,
            total_quarterly_cost = quarterly_total_cost,
            total_half_yearly_cost = half_yearly_total_cost,
            total_yearly_cost_summarized=yearly_total_cost
        )
    with theme_col:
        # Custom theme toggle using a button
        if st.session_state.theme == 'light':
            button_label = "🌙"
            new_theme = 'dark'
        else:
            button_label = "☀️"
            new_theme = 'light'

        if st.button(button_label):
            st.session_state.theme = new_theme
            # Set Streamlit's internal theme option
            st.config.set_option("theme.base", new_theme)
            st.rerun() # Rerun to apply the theme change immediately

# Apply the current theme setting
st.config.set_option("theme.base", st.session_state.theme)

main_col, summary_col = st.columns([3, 1])
active_tab = st.session_state.get("active_tab")
with main_col:
    tab1, tab2, tab3 ,tab4, tab5= st.tabs(["Databricks & Compute", "S3 Storage", "SQL Warehouse", "Development Cost", "Calculation Explation"])

    with tab1:
        # render_databricks_tab(FLAT_RATE_CARD, FLAT_INSTANCE_LIST, INSTANCE_PRICES, COMPUTE_TYPE_LIST)
        render_databricks_tab()
        render_configuration_guide()
    with tab2:
        # Pass the projected_s3_cost_12_months to render_s3_tab
        render_s3_tab(s3_costs_per_zone, s3_cost, total_quarterly_cost, total_half_yearly_cost, projected_s3_cost_12_months, total_table_cost)
    with tab3:
        render_sql_warehouse_tab(sql_dbu_cost, sql_ec2_cost, sql_dbu)
    with tab4:
        render_devepoment_tools() 
    with tab5:
        render_calcu_explain()          


with summary_col:
    # Pass the projected_s3_cost_12_months to render_summary_column
    render_summary_column(total_cost, databricks_total_cost, s3_cost, sql_dbu, projected_s3_cost_12_months, quarterly_total_cost, half_yearly_total_cost, yearly_total_cost, dev_cost,total_table_cost)
