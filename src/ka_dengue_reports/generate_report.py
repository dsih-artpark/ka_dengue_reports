import datetime

import pandas as pd
from dataio.download import download_dataset_v2

# from ka_dengue_reports import get_regionIDs

def get_regionIDs(*, regionIDs_path="data/GS0015DS0034-LGD_Region_IDs_and_Names/regionids.csv"):

    regionIDs_df = pd.read_csv(regionIDs_path)

    regionIDs_dict = {}
    for _, row in regionIDs_df.iterrows():
        regionIDs_dict[row["regionID"]] = {"regionName": row["regionName"],
                                           "parentID": row["parentID"]
                                           }
    return regionIDs_df, regionIDs_dict



analysis_window = 6
report_file_name = report_file_name = f"reports/Karnataka-{datetime.datetime.today().strftime('%Y-%m-%d')}.md"


regionIDs_df, regionIDs_dict= get_regionIDs()

## changing regionIDs structure for BBMP, where admin2 = bbmp, admin3 = bbmp and admin5 = ward
regionIDs_df.loc[regionIDs_df["parentID"]=="ulb_276600", "regionID"]="ulb_276600"
regionIDs_df.loc[regionIDs_df["parentID"]=="ulb_276600", "regionName"]="BBMP"
regionIDs_df.loc[regionIDs_df["regionID"].str.startswith("ward_276600"), "parentID"]="ulb_276600"

ka_districts = regionIDs_df[(regionIDs_df["parentID"]=="state_29")]

# download_dataset_v2(dsid="EP0005DS0014", contains_any = "ihip")

df = pd.read_csv("data/EP0005DS0014-KA_Dengue_LL/ihip/ka-line-list-ihip.csv")

# changing reporting for BBMP 

df.loc[df["location.admin3.ID"]=="ulb_276600", "location.admin2.ID"]="ulb_276600"
df.loc[df["location.admin3.ID"]=="ulb_276600", "location.admin2.name"]="BBMP"

df = df[['location.admin2.ID',
       'location.admin2.name', 'location.admin3.ID', 'location.admin3.name',
       'location.admin4.ID', 'location.admin4.name', 'location.admin5.ID',
       'location.admin5.name', 'location.admin.coarseness',
       'location.geometry.latitude.provided',
       'location.geometry.longitude.provided', 'event.test',
       'event.test.resultDate']]

df['event.test.resultDate'] = pd.to_datetime(df['event.test.resultDate'], format="%Y-%m-%dT%H:%M:%SZ")
max_result_date = df["event.test.resultDate"].max()
min_date = max_result_date - datetime.timedelta(days=analysis_window)

df = df[df['event.test.resultDate'] >= min_date]

# Filtering dataframe for all cases with valid village/ward IDs
hotspots = df[df["location.admin5.ID"]!="admin_0"]
hotspots = hotspots.groupby(by = ['location.admin2.ID', 'location.admin3.ID',
                                   'location.admin5.ID', ])['event.test'].sum().reset_index().rename(columns={"event.test":"number_of_cases"})  # noqa: E501

hotspots = hotspots[hotspots['number_of_cases']>=2]

# District Table
district_table = df.groupby(by="location.admin2.ID")["event.test"].sum().reset_index().rename(columns={"event.test":"reported_cases"})  # noqa: E501

hotspots_by_district = hotspots.groupby(by=["location.admin2.ID"])["number_of_cases"].count().reset_index().rename(columns={"number_of_cases":"number_of_hotspots"})

# merge left with subdistrict table
district_table = district_table.merge(hotspots_by_district, on=["location.admin2.ID"], how="left")

district_table["number_of_hotspots"] = district_table["number_of_hotspots"].fillna(0)

# add master list of districts
# do an outer merge to retain admin_0s in the dataset

district_table=district_table.merge(ka_districts, left_on = ["location.admin2.ID"],
                                      right_on = ["regionID"], how = "outer")

district_table.loc[district_table["location.admin2.ID"].isna(), "location.admin2.ID"] = district_table["regionID"]
district_table = district_table.drop(columns=["regionID", "regionName", "parentID"])
district_table.fillna(0, inplace=True)

# Get district names
district_table["district_name"] = district_table["location.admin2.ID"].apply(lambda x: regionIDs_dict[x]["regionName"] if x!="admin_0" else "-")

district_table["district_code"] = district_table["location.admin2.ID"].str.split("_").str.get(1)

district_table = district_table.drop(columns = ["location.admin2.ID"])
district_table = district_table.sort_values(by="number_of_hotspots", ascending=False)
district_table = district_table.drop_duplicates().reset_index(drop=True)
district_table = district_table[["district_code", "district_name", "reported_cases",
                                 "number_of_hotspots"]]
district_table.loc[district_table["district_code"]=="0", "district_code"]="-"

district_table.loc[len(district_table.index)] = ["", "Total", district_table["reported_cases"].sum(),
                                                    district_table["number_of_hotspots"].sum()]

district_table = district_table.rename(columns = {"district_code":"District Code",
                                                  "district_name":"District Name", 
                                                  "reported_cases":"Reported Cases",
                                                  "number_of_hotspots":"Number of Hotspots"})

district_md = district_table.to_markdown(index=False)

cases_with_village_ward_info = round((len(df[(df["location.admin.coarseness"] == "village") | (df["location.admin.coarseness"] == "ward")])/len(df))*100, 1)
cases_with_subdistrict_ulb_info = round((len(df[(df["location.admin.coarseness"] == "subdistrict") | (df["location.admin.coarseness"] == "ulb")])/len(df))*100, 1)
cases_with_district_info = round((len(df[df["location.admin.coarseness"]=="district"])/len(df))*100,1)

header = f"""### Karnataka Dengue Report, dt. {datetime.datetime.today().strftime('%B %d %Y')}


#### Summary
* **Report Period**: {min_date.strftime('%B %d %Y')} - {max_result_date.strftime('%B %d %Y')}
* **Analysis Window**: {analysis_window+1} days
* **Reported Cases**: {len(df)} cases were reported in this time period.
* **Hotspots**: {len(hotspots)} villages/wards were identified as hotspots (with 2 or more cases).
"""


footer = f"""
<sup>[^1]</sup> Out of {len(df)} cases, {cases_with_village_ward_info}% cases have upto village/ward level information, {cases_with_subdistrict_ulb_info}% cases have upto subdistrict/ULB level information, and {cases_with_district_info}% cases have upto district information.
"""

header2 = f"""#### Number of Hotspots"""

# Write header, markdown table, and footer to a markdown file
with open(report_file_name, "w") as f:
    f.write(header)
    f.write("\n\n")
    f.write(header2)
    f.write("\n\n")
    f.write(district_md)
    f.write(footer)

