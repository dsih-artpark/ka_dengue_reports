import pandas as pd
import numpy as np
import os
import geopandas as gpd
import matplotlib.pyplot as plt
from tabulate import tabulate
import datetime


# master_dists
regions=pd.read_csv("regionids.csv")
regions=regions[regions["parentID"]=="state_29"]

D={}
for index, row in regions.iterrows():
    D[row["regionID"]]=row["regionName"]


# D={'district_524': 'Bagalkote', 'district_528': 'Ballari', 'district_527': 'Belagavi', 'district_526': 'Bengaluru Rural', 'district_525': 'Bengaluru Urban', 'district_529': 'Bidar', 'district_531': 'Chamarajanagara', 'district_630': 'Chikkaballapura', 'district_532': 'Chikkamagaluru', 'district_533': 'Chitradurga', 'district_534': 'Dakshina Kannada', 'district_535': 'Davangere', 'district_536': 'Dharwad', 'district_537': 'Gadag', 'district_539': 'Hassan', 'district_540': 'Haveri', 'district_538': 'Kalaburagi', 'district_541': 'Kodagu', 'district_542': 'Kolar', 'district_543': 'Koppal', 'district_544': 'Mandya', 'district_545': 'Mysuru', 'district_546': 'Raichur', 'district_631': 'Ramanagara', 'district_547': 'Shivamogga', 'district_548': 'Tumakuru', 'district_549': 'Udupi', 'district_550': 'Uttara Kannada', 'district_738': 'Vijayanagar', 'district_530': 'Vijayapura', 'district_635': 'Yadgir'}

# Read data from the CSV file
df = pd.read_csv('src/BBMP_Eda/Data/ka-line-list-ihip.csv')

df = pd.read_csv('ka-line-list-ihip.csv')

df.columns
# Filter the dataset for cols we need

df = df[['metadata.recordID', 'location.admin2.ID',
       'location.admin2.name', 'location.admin3.ID', 'location.admin3.name',
       'location.admin4.ID', 'location.admin4.name', 'location.admin5.ID',
       'location.admin5.name', 'location.admin.coarseness',
       'location.geometry.latitude.provided',
       'location.geometry.longitude.provided', 'event.test',
       'event.test.resultDate']]

# Define the date range as 14 days (result date - 14 days)
df['event.test.resultDate'] = pd.to_datetime(df['event.test.resultDate'], format="%Y-%m-%dT%H:%M:%SZ")
max_result_date = df["event.test.resultDate"].max()
min_date = max_result_date - datetime.timedelta(days=14)

# Filter the DataFrame for cases within 14 days of the max result date
df = df[df['event.test.resultDate'] >= min_date]

# Headers 
print(f"Report Date: {datetime.datetime.today()}")
print(f"Report Period: {min_date} - {max_result_date}")


#### DATA AVAILABILITY ####
df["location.admin.coarseness"].value_counts()

# Filter out villages/ward without a valid village/ward ID - i.e., coarseness = village/ward
hotspots_df = df[df["location.admin5.ID"]!="admin_0"]
hotspots_df = hotspots_df.groupby(by = ['location.admin2.ID', 'location.admin2.name', 'location.admin3.ID', 'location.admin3.name', 'location.admin5.ID', 'location.admin5.name'])['event.test'].sum().reset_index().rename(columns={"event.test":"number_of_cases"})

# Identifying hotspots based on case count >=2
hotspots_df['whether_hotspot'] = hotspots_df['number_of_cases'].apply(lambda x: x >= 2)

# District table
## total cases by dist
district_table = df.groupby(by="location.admin2.ID")["event.test"].sum().reset_index().rename(columns={"event.test":"total_cases_by_district"})

# cases with info 
district_table = district_table.merge(hotspots_df.groupby(by=["location.admin2.ID", "location.admin2.name"])["location.admin3.ID"].count().reset_index().rename(columns={"location.admin3.ID":"cases_with_info"}), on="location.admin2.ID", how="left")

# number of hotspots
district_table = district_table.merge(hotspots_df.groupby(by="location.admin2.ID")["whether_hotspot"].sum().reset_index().rename(columns={"whether_hotspot":"number_of_hotspots"}), on="location.admin2.ID", how="left")

district_table
# Extracting codes for district, subdistrict, village, ward
district_table["district_code"] = district_table["location.admin2.ID"].str.split("_").str.get(1)
district_table["subdistrict_ulb_code"] = district_table["location.admin3.ID"].str.split("_").str.get(1)
district_table["village_ward_code"] = district_table["location.admin5.ID"].str.split("_").str.get(1)

hotspots_df.rename(columns={"location.admin2.name":"district_name", 
                            "location.admin3.name":"subdistrict_ulb name",
                            "location.admin5.name":"village_ward_name"}, inplace=True)


## TO WORK ON:
# # # Save the district summary to a CSV file
# district_summary.to_csv('State_29.csv', index=False)
# print(district_summary)

# state_markdown=tabulate(district_summary, tablefmt="pipe", headers="keys", showindex=False)

# state_report_content = f"""
# # Dengue Cases Report between {f"{pd.to_datetime(start_date).day} {pd.to_datetime(start_date).strftime('%B %Y')}"} and {f"{pd.to_datetime(end_date).day} {pd.to_datetime(end_date).strftime('%B %Y')}"}.

# ### Total number of cases reported = 3364 
# ### Number of cases that has village level address/info = {district_summary['total_cases'].sum()} 
# ### The hotspots generated are based on the available village level information  

# ### This report is generated on 16-07-2024

# ## Cases and Hotspot analysis

# {state_markdown}

# Total cases = {district_summary['total_cases'].sum()} and Total Hotspots = {district_summary['total_hotspots'].sum()}

# """

# # Write to a Markdown file
# with open('State_Report.md', 'w') as file:
#     file.write(state_report_content)














# # # Generate taluk summaries for each district
# # for district in village_cases_df['District'].unique():
# #     district_data = village_cases_df[village_cases_df['District'] == district]
# #     taluk_data_summary = district_data.groupby('Taluk/ULB').agg(
# #         total_cases=('positive_case', 'sum'),
# #         total_hotspots=('hotspot', lambda x: (x == 'Yes').sum())
# #     ).reset_index()
# #     filename = f"{district.replace(' ', '_').replace(',', '')}.csv"
# #     taluk_data_summary.to_csv(filename, index=False)

# #     # Sort by 'positive_case' in descending order and take the top 20
# #     top_villages = district_data.sort_values(by='positive_case', ascending=False).head(20)

# #     # Drop the 'District' column from the top villages data
# #     top_villages = top_villages[['village', 'Taluk/ULB', 'positive_case', 'hotspot']]  

# #     # Generate a filename for top villages report
# #     filename_top_villages = f"{district.replace(' ', '_').replace(',', '')}_top_villages.csv"
    
# #     # Save the top villages data to a CSV file
# #     # top_villages.to_csv(filename_top_villages, index=False)

# #     taluk_markdown=tabulate(taluk_data_summary, tablefmt="pipe", headers="keys", showindex=False)
# #     top_village_markdown=tabulate(top_villages, tablefmt="pipe", headers="keys", showindex=False)

# #     district_report_content=f"""
# #     # Dengue Cases Report of {district} district.

# #     # {taluk_data_summary['total_cases'].sum()} cases were reported between {start_date} and {end_date}.

# #     # There are total of {taluk_data_summary['toal_hotspots'].sum()} hotspots with this district.

# #     # This report is generated on 16-07-2024

# #     ## Cases and Hotspot analysis

# #     {taluk_markdown}

# #     Total Cases = {taluk_data_summary['total_cases'].sum()} and Total Hotspots = {taluk_data_summary['total_hotspots'].sum()}

# #     ## Top vilages with most hotspots

# #     {top_village_markdown}

# #     """

# #     # Write to a Markdown file
# #     with open(f'{district}_report.md', 'w') as file:
# #         file.write(district_report_content)
