# file_exportor.py
import io
import pandas as pd
import openpyxl
import streamlit as st
import numpy as np

def generate_consolidated_excel_export(calculated_dbx_data, s3_calc_method, s3_direct_config, s3_table_based_config, sql_warehouses_config, dev_costs_config, s3_cost,sql_dbu_cost, sql_ec2_cost, databricks_total_cost, dev_cost, total_monthly_summarized_cost, total_quarterly_cost,half_yearly_total_cost,total_yearly_cost_summarized):
    """
    Generates a consolidated Excel file with multiple sheets for different cost categories.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:

        total_sql_cost = sql_dbu_cost + sql_ec2_cost
        # 0. Create a Summary Sheet
        summary_data = {
            'Category': ['Databricks & Compute', 'S3 Storage', 'SQL Warehouses', 'Development Cost', 'Total'],
            'Monthly Cost ($)': [databricks_total_cost, s3_cost, total_sql_cost, dev_cost, total_monthly_summarized_cost],
            'Quartterly Cost ($)': [databricks_total_cost * 3, s3_cost * 3, total_sql_cost * 3, dev_cost * 3, total_quarterly_cost],
            'Half-Yearly Cost ($)': [databricks_total_cost * 6, s3_cost * 6, total_sql_cost * 6, dev_cost * 6, half_yearly_total_cost],
            'Yearly Cost ($)': [databricks_total_cost * 12, s3_cost * 12, total_sql_cost * 12, dev_cost * 12, total_yearly_cost_summarized]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summaries", index=False)
        # 1. Databricks Jobs Sheet (All Tiers Combined)
        all_dbx_dfs = []
        for tier, data in calculated_dbx_data.items():
            df_to_export = data['df'].copy()
            df_to_export['Tier'] = tier
            all_dbx_dfs.append(df_to_export)

        if all_dbx_dfs:
            combined_dbx_df = pd.concat(all_dbx_dfs, ignore_index=True)
            combined_dbx_df = combined_dbx_df.rename(columns={
                'Job Name': 'Name',
                'Runtime (hrs)': 'Runtime Hours',
                'Runs/Month': 'Runs per Month',
                'Compute type': 'Compute Type',
                'Instance Type': 'Instance',
                'Nodes': 'worker_Nodes',
                'DBU': 'Calculated DBU',
                'DBX': 'Calculated DBX Cost ($)',
                'EC2': 'Calculated EC2 Cost ($)'
            })
            ordered_cols_dbx = ['Tier', 'Name', 'Runtime Hours', 'Runs per Month', 'Compute Type', 'Instance', 'worker_Nodes', 'Calculated DBU', 'Calculated DBX Cost ($)', 'Calculated EC2 Cost ($)']
            present_cols = [col for col in ordered_cols_dbx if col in combined_dbx_df.columns]
            combined_dbx_df = combined_dbx_df[present_cols]
            combined_dbx_df.to_excel(writer, sheet_name="Databricks_Jobs", index=False)
        else:
            empty_dbx_df = pd.DataFrame(columns=['Tier', 'Name', 'Runtime Hours', 'Runs per Month', 'Compute Type', 'Instance', 'worker_Nodes', 'Calculated DBU', 'Calculated DBX Cost ($)', 'Calculated EC2 Cost ($)'])
            empty_dbx_df.to_excel(writer, sheet_name="Databricks_Jobs", index=False)

        # 2. S3 Storage Sheets (based on active method)
        if s3_calc_method == "Direct Storage[Recommended]":
            direct_data = []
            for zone, config in s3_direct_config.items():
                direct_data.append({
                    "Zone": zone,
                    "Storage Class": config["class"],
                    "Storage Amount": config["amount"],
                    "Unit": config["unit"],
                    "Monthly Growth %": config["monthly_growth_percent"]
                })
            if direct_data:
                df_direct = pd.DataFrame(direct_data)
                df_direct.to_excel(writer, sheet_name='S3_Direct_Storage', index=False)
            else:
                empty_s3_direct_df = pd.DataFrame(columns=["Zone", "Storage Class", "Storage Amount", "Unit", "Monthly Growth %"])
                empty_s3_direct_df.to_excel(writer, sheet_name='S3_Direct_Storage', index=False)

        else: # Table-Based
            consolidated_table_data_for_export = []
            for zone, list_of_table_configs in s3_table_based_config.items():
                for table_config in list_of_table_configs:
                    row = {
                        "Zone": zone,
                        "Table Name": table_config.get("Table Name", ""),
                        "Records": table_config.get("Records", 0),
                        "Columns": table_config.get("Columns", 0),
                        "Number of Tables": table_config.get("Table", 0),
                        "Avg_Column_length": table_config.get("Avg_Column_length", 0)
                    }
                    consolidated_table_data_for_export.append(row)

            if consolidated_table_data_for_export:
                df_table = pd.DataFrame(consolidated_table_data_for_export)
                ordered_cols_s3_table = ["Zone", "Table Name", "Records", "Columns", "Number of Tables", "Avg_Column_length"]
                df_table = df_table[ordered_cols_s3_table]
                df_table.to_excel(writer, sheet_name='S3_Table_Based_Storage', index=False)
            else:
                empty_s3_table_df = pd.DataFrame(columns=["Zone", "Table Name", "Records", "Columns", "Number of Tables", "Avg_Column_length"])
                empty_s3_table_df.to_excel(writer, sheet_name='S3_Table_Based_Storage', index=False)

        # 3. SQL Warehouses Sheet
        if sql_warehouses_config:
            warehouse_data = []
            for wh in sql_warehouses_config:
                warehouse_data.append({
                    "Name": wh.get("name", ""),
                    "Type": wh.get("type", ""),
                    "Size": wh.get("size", "N/A"),
                    "Nodes": wh.get("SQL_nodes", 1),
                    "Hours per Day": wh.get("hours_per_day", 0),
                    "Days per Month": wh.get("days_per_month", 0)
                })
            df_sql = pd.DataFrame(warehouse_data)
            ordered_cols_sql = ["Name", "Type", "Size", "Nodes", "Hours per Day", "Days per Month"]
            df_sql = df_sql[ordered_cols_sql]
            df_sql.to_excel(writer, sheet_name='SQL_Warehouses', index=False)
        else:
            empty_sql_df = pd.DataFrame(columns=["Name", "Type", "Size", "Nodes", "Hours per Day", "Days per Month"])
            empty_sql_df.to_excel(writer, sheet_name='SQL_Warehouses', index=False)

        # 4. Development Cost Sheet
        if not dev_costs_config.empty:
            df_dev = dev_costs_config.copy()
            df_dev = df_dev.rename(columns={
                'Compute_type': 'Compute Type',
                'Driver type': 'Driver Instance',
                'Worker Type': 'Worker Instance',
                'Nodes': 'Worker Nodes',
                'hr_per_month': 'Hours per Month',
                'no_of_Month': 'Number of Months',
                'DBX': 'Calculated DBX Cost ($)',
                'EC2': 'Calculated EC2 Cost ($)',
                'Total': 'Total Cost ($)'
            })
            ordered_cols_dev = ['Compute Type', 'Driver Instance', 'Worker Instance', 'Worker Nodes', 'Hours per Month', 'Number of Months', 'Calculated DBX Cost ($)', 'Calculated EC2 Cost ($)', 'Total Cost ($)']
            present_cols_dev = [col for col in ordered_cols_dev if col in df_dev.columns]
            df_dev = df_dev[present_cols_dev]
            df_dev.to_excel(writer, sheet_name='Development_Cost', index=False)
        else:
            empty_dev_df = pd.DataFrame(columns=['Compute Type', 'Driver Instance', 'Worker Instance', 'Worker Nodes', 'Hours per Month', 'Number of Months', 'Calculated DBX Cost ($)', 'Calculated EC2 Cost ($)', 'Total Cost ($)'])
            empty_dev_df.to_excel(writer, sheet_name='Development_Cost', index=False)

    output.seek(0)
    return output.getvalue()