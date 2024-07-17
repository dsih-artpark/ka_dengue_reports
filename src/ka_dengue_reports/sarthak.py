import pandas as pd
import numpy as np
import os
import geopandas as gpd
import matplotlib.pyplot as plt
from tabulate import tabulate


# Read data from the CSV file
df = pd.read_csv('src/BBMP_Eda/Data/ihip-data-final.csv')




# Convert 'date' column to datetime format
df['event.test.resultDate'] = pd.to_datetime(df['event.test.resultDate'])

# Define the start and end dates for the date range you want to analyze
start_date = pd.Timestamp('2024-07-14', tz='UTC')
end_date = pd.Timestamp('2024-07-15', tz='UTC')

# Filter the DataFrame based on the defined date range
df = df[(df['event.test.resultDate'] >= start_date) & (df['event.test.resultDate'] <= end_date)]


# Create a new column 'positive_case' that is True if any of the tests are positive
df['positive_case'] = True

df_district = df.groupby(['location.admin2.ID', 'location.admin2.name'])['positive_case'].sum() 

# Convert the Series to a DataFrame and rename columns
df_district = df_district.reset_index(name='total_cases')

df_district.rename(columns={'location.admin2.ID': 'district_id', 'location.admin2.name': 'District'}, inplace=True)

# print (df_district)


df_taluk = df.groupby(['location.admin3.name'])['positive_case'].sum()

df_taluk = df_taluk.reset_index(name='total_cases')

# Replace 'admin_0' with NaN in 'village_ID'
df_taluk['location.admin3.ID'] = df['location.admin3.ID'].replace('admin_0', np.nan)

df_taluk.rename(columns={'location.admin3.name': 'Taluk/ULB'}, inplace=True)


print(df_taluk)

# Replace 'admin_0' with NaN in 'village_ID'
df['location.admin5.ID'] = df['location.admin5.ID'].replace('admin_0', np.nan)

# print(df["location.admin5.ID"].isna().sum())


# Group the data by village
positive_cases_by_village = df.groupby(['location.admin2.ID', 'location.admin2.name', 'location.admin3.ID', 'location.admin3.name', 'location.admin5.ID', 'location.admin5.name'])['positive_case'].sum()

# Convert the grouped data back to a DataFrame
village_cases_df = positive_cases_by_village.reset_index()



# Rename 'location.admin5.name' to 'village'
village_cases_df.rename(columns={'location.admin5.name': 'village'}, inplace=True)

# Add a new column 'hotspot' where 'Yes' if cases >= 2, otherwise 'No'
village_cases_df['hotspot'] = village_cases_df['positive_case'].apply(lambda x: 'Yes' if x > 1 else 'No')



# Create a combined 'District' column in the format Name_IDnumber
village_cases_df['District'] = village_cases_df['location.admin2.name']
village_cases_df['Taluk/ULB'] = village_cases_df['location.admin3.name']

# Drop the old district ID and name columns
village_cases_df.drop(['location.admin2.ID', 'location.admin2.name', 'location.admin3.ID', 'location.admin3.name'], axis=1, inplace=True)

print(village_cases_df)









# Group by the new 'District' column and calculate the sum of 'total_cases' and count of hotspots
district_summary = village_cases_df.groupby('District').agg(
    total_cases_with_info=('positive_case', 'sum'),
    total_hotspots=('hotspot', lambda x: (x == 'Yes').sum())
).reset_index()




district_final_df = pd.merge(df_district, district_summary, on='District', how='inner')
print("Inner Join:\n", district_final_df)




html_table = tabulate(district_final_df, tablefmt="html", headers="keys", showindex=False)

# Adding basic CSS to style the HTML table
style = """
<style>
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}
th, td {
  padding: 10px;
}
</style>
"""

state_report_content = f"""
{style}
# Dengue Cases Report

### {district_final_df['total_cases'].sum()} cases were reported between {pd.to_datetime(start_date).day} {pd.to_datetime(start_date).strftime('%B %Y')} and {pd.to_datetime(end_date).day} {pd.to_datetime(end_date).strftime('%B %Y')}
### {district_summary['total_cases_with_info'].sum()} cases has village level information

### The hotspots generated are based on the available village level information  

### This report is generated on 16-07-2024

## Cases and Hotspot analysis

{html_table}

Total cases = {district_final_df['total_cases'].sum()} and Total Hotspots = {district_final_df['total_hotspots'].sum()}
"""



# Write to a Markdown file which supports HTML
with open('State_Report.md', 'w') as file:
    file.write(state_report_content)














# Generate taluk summaries for each district
for district in village_cases_df['District'].unique():
    district_data = village_cases_df[village_cases_df['District'] == district]
    taluk_data_summary = district_data.groupby('Taluk/ULB').agg(
        total_cases=('positive_case', 'sum'),
        total_hotspots=('hotspot', lambda x: (x == 'Yes').sum())
    ).reset_index()

    taluk_final_df = pd.merge(df_taluk, taluk_data_summary, on='Taluk/ULB', how='inner' )
    filename = f"{district.replace(' ', '_').replace(',', '')}.csv"
    taluk_final_df.to_csv(filename, index=False)

    # # Sort by 'positive_case' in descending order and take the top 20
    # top_villages = district_data.sort_values(by='positive_case', ascending=False).head(20)

    # # Drop the 'District' column from the top villages data
    # top_villages = top_villages[['village', 'Taluk/ULB', 'positive_case', 'hotspot']]  

    # # Generate a filename for top villages report
    # filename_top_villages = f"{district.replace(' ', '_').replace(',', '')}_top_villages.csv"
    
    # # Save the top villages data to a CSV file
    # # top_villages.to_csv(filename_top_villages, index=False)

    # taluk_markdown=tabulate(taluk_data_summary, tablefmt="pipe", headers="keys", showindex=False)
    # top_village_markdown=tabulate(top_villages, tablefmt="pipe", headers="keys", showindex=False)

    # district_report_content=f"""
    # # Dengue Cases Report of {district} district.

    # # {taluk_data_summary['total_cases'].sum()} cases were reported between {start_date} and {end_date}.

    # # There are total of {taluk_data_summary['toal_hotspots'].sum()} hotspots with this district.

    # # This report is generated on 16-07-2024

    # ## Cases and Hotspot analysis

    # {taluk_markdown}

    # Total Cases = {taluk_data_summary['total_cases'].sum()} and Total Hotspots = {taluk_data_summary['total_hotspots'].sum()}

    # ## Top vilages with most hotspots

    # {top_village_markdown}

    # """

    # # Write to a Markdown file
    # with open(f'{district}_report.md', 'w') as file:
    #     file.write(district_report_content)
