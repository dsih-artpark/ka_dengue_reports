import datetime
import pandas as pd
from dataio.download import download_dataset_v2
from importlib.metadata import version


def get_regionIDs(*, regionIDs_path="data/GS0015DS0034-LGD_Region_IDs_and_Names/regionids.csv"):

    regionIDs_df = pd.read_csv(regionIDs_path)

    regionIDs_dict = {}
    for _, row in regionIDs_df.iterrows():
        regionIDs_dict[row["regionID"]] = {"regionName": row["regionName"],
                                           "parentID": row["parentID"]
                                           }
    return regionIDs_df, regionIDs_dict


analysis_window = 6
# report_file_name = "reports/KA Den Report 17 July.md"

# download_dataset_v2(dsid="GS0015DS0034")

regionIDs_df, _ = get_regionIDs()


## changing regionIDs structure for BBMP, where admin2 = bbmp, admin3 = bbmp and admin5 = ward
regionIDs_df.loc[regionIDs_df["parentID"]=="ulb_276600", "regionID"]="ulb_276600"
regionIDs_df.loc[regionIDs_df["parentID"]=="ulb_276600", "regionName"]="BBMP"
regionIDs_df.loc[regionIDs_df["regionID"].str.startswith("ward_276600"), "parentID"]="ulb_276600"

ka_districts = regionIDs_df[regionIDs_df["parentID"]=="state_29"]["regionID"].to_list()
ka_districts+=["ulb_276600"]
ka_subdistricts = regionIDs_df[(regionIDs_df["parentID"].isin(ka_districts)) | (regionIDs_df["regionID"].str.startswith("ward_276600"))]

regionIDs_dict = {}
for index, row in regionIDs_df.iterrows():
    regionIDs_dict[row["regionID"]] = {"regionName": row["regionName"],
                                           "parentID": row["parentID"]}



# download_dataset_v2(dsid="EP0005DS0014", contains_any = "ihip")

df = pd.read_csv("data/EP0005DS0014-KA_Dengue_LL/ihip/ka-line-list-ihip.csv")

# changing reporting for BBMP 

df.loc[df["location.admin3.ID"]=="ulb_276600", "location.admin2.ID"]=df["location.admin3.ID"]
df.loc[df["location.admin3.ID"]=="ulb_276600", "location.admin2.name"]=df["location.admin3.name"]

df = df[['location.admin2.ID',
       'location.admin2.name', 'location.admin3.ID', 'location.admin3.name',
       'location.admin5.ID','location.admin5.name', 'location.admin.coarseness', 'event.test',
       'event.test.resultDate']]

df['event.test.resultDate'] = pd.to_datetime(df['event.test.resultDate'], format="%Y-%m-%dT%H:%M:%SZ")
max_result_date = df["event.test.resultDate"].max()
min_date = max_result_date - datetime.timedelta(days=analysis_window)

df = df[df['event.test.resultDate'] >= min_date]


print(f"Number of missing districts: {len(df[df['location.admin2.ID']=='admin_0'])}")
print(f"Number of missing sub-districts: {len(df[df['location.admin3.ID']=='admin_0'])}")
print(f"Number of missing villages: {len(df[df['location.admin5.ID']=='admin_0'])}")
print(f"Number of districts: {df['location.admin2.name'].nunique()}")
print(f"List of districts:{df['location.admin2.name'].unique()}")


# Filtering dataframe for all cases with valid village/ward IDs
hotspots = df[df["location.admin5.ID"]!="admin_0"]
print(f"Number of missing subdist in hotspots: {len(hotspots[hotspots['location.admin3.ID']=='admin_0'])}")
# should be 0

hotspots = hotspots.groupby(by = ['location.admin2.ID', 'location.admin3.ID',
                                   'location.admin5.ID'])['event.test'].sum().reset_index().rename(columns={"event.test":"number_of_cases"})  # noqa: E501

hotspots = hotspots[hotspots['number_of_cases']>=2]

print(f"Number of hotspots: {len(hotspots)}")

# get names of subdistricts and villages
hotspots["subdistrict_name"] = hotspots["location.admin3.ID"].apply(lambda x: regionIDs_dict[x]["regionName"] if x!="admin_0" else "-")
hotspots["village_name"] = hotspots["location.admin5.ID"].apply(lambda x: regionIDs_dict[x]["regionName"] if x!="admin_0" else "-")

# Subdistrict Table 1 - Number of cases, hotspots by district, sub-district

# number of reported cases by subdistrict - don't use names, as they have nan values
subdistrict_table = df.groupby(by=["location.admin2.ID", "location.admin3.ID"])["event.test"].sum().reset_index().rename(columns={"event.test":"reported_cases"})  # noqa: E501

# number of hotspots by subdistrict
hotspots_by_subdistrict = hotspots.groupby(by=["location.admin2.ID", "location.admin3.ID"])["number_of_cases"].count().reset_index().rename(columns={"number_of_cases":"number_of_hotspots"})

# merge left with subdistrict table
subdistrict_table = subdistrict_table.merge(hotspots_by_subdistrict, on=["location.admin2.ID", "location.admin3.ID"], how="left")

subdistrict_table["number_of_hotspots"] = subdistrict_table["number_of_hotspots"].fillna(0)

# add master list of subdistricts, names

# do an outer merge to retain admin_0s in the dataset
subdistrict_table = subdistrict_table.merge(ka_subdistricts, left_on = ["location.admin2.ID", "location.admin3.ID"],
                                      right_on = ["parentID", "regionID"], how = "outer")

# add new subdistricts to subdistrict_table
subdistrict_table.loc[subdistrict_table["location.admin2.ID"].isna(), "location.admin2.ID"] = subdistrict_table["parentID"]
subdistrict_table.loc[subdistrict_table["location.admin3.ID"].isna(), "location.admin3.ID"] = subdistrict_table["regionID"]
subdistrict_table = subdistrict_table.drop(columns=["regionID", "regionName", "parentID"])
subdistrict_table.fillna(0, inplace=True)

# Get subdistrict names
subdistrict_table["subdistrict_name"] = subdistrict_table["location.admin3.ID"].apply(lambda x: regionIDs_dict[x]["regionName"] if x!="admin_0" else "-")
subdistrict_table.rename(columns={"location.admin3.ID":"subdistrict_code"}, inplace=True)

subdistrict_table = subdistrict_table[["location.admin2.ID", "subdistrict_code", "subdistrict_name", "reported_cases", "number_of_hotspots"]]
                                       
# Iterate through districts

# for district_code in ka_districts:
for district_code in ka_districts:
    district_name = regionIDs_dict[district_code]["regionName"]
    report_file_name = f"reports/{district_name}-{datetime.datetime.today().strftime('%Y-%m-%d')}.md"

    # Table 1
    table = subdistrict_table[subdistrict_table["location.admin2.ID"] == district_code]
    table = table.sort_values(by="number_of_hotspots", ascending=False).head(15)
    table = table.drop_duplicates().reset_index(drop=True)
    table = table[['subdistrict_code', 'subdistrict_name', 'reported_cases', 'number_of_hotspots']]
    table.loc[len(table)] = ["", "Total", table["reported_cases"].sum(), table["number_of_hotspots"].sum()]
    table = table.rename(columns={"subdistrict_code": "**Sub-District/ULB Code**",
                                  "subdistrict_name": "**Sub-District/ULB Name**",
                                  "reported_cases": "**Reported Cases**",
                                  "number_of_hotspots": "**Number of Hotspots**"})
    table_md = table.to_markdown(index=False)

    # Table 2
    table2 = hotspots[hotspots["location.admin2.ID"] == district_code]
    table2 = table2.sort_values(by="number_of_cases", ascending=False).reset_index(drop=True)
    table2 = table2[['location.admin3.ID', 'subdistrict_name', 'location.admin5.ID', 'village_name', 'number_of_cases']]
    table2 = table2.rename(columns={"location.admin3.ID": "**Sub-District/ULB Code**",
                                    "subdistrict_name": "**Sub-District/ULB Name**",
                                    "location.admin5.ID": "**Village/Ward Code**",
                                    "village_name": "**Village/Ward Name**",
                                    "number_of_cases": "**Reported Cases**"})
    table2_md = table2.to_markdown(index=False)

    cases_with_village_ward_info = len(df[(df["location.admin2.ID"] == district_code) & (df["location.admin.coarseness"].isin(["village", "ward"]))])
    cases_with_subdistrict_ulb_info = len(df[(df["location.admin2.ID"] == district_code) & (df["location.admin.coarseness"].isin(["subdistrict", "ulb"]))])

    header = f"""## Karnataka Dengue Report - {district_name} dt. {datetime.datetime.today().strftime('%B %d %Y')}

#### Summary
* **Report Period**: {min_date.strftime('%B %d %Y')} - {max_result_date.strftime('%B %d %Y')}
* **Analysis Window**: {analysis_window} days
* **Reported Cases**: {len(df[df["location.admin2.ID"] == district_code])} cases were reported in this time period.
* **Hotspots**: {len(hotspots[(hotspots["location.admin2.ID"] == district_code)])} villages/wards were identified as hotspots (with 2 or more cases).
"""

    footer = f"""
<sup>[^1]</sup> Out of {len(df[df["location.admin2.ID"] == district_code])} cases, {cases_with_village_ward_info} cases have village/ward level information and {cases_with_subdistrict_ulb_info} cases have subdistrict/ULB level information.
"""
    
    header2 = f"""#### Top 15 Sub-Districts/ULBs by Hotspots"""

    header3 = f"""#### List of Hotspots"""

    # Write header, markdown table, and footer to a markdown file
    with open(report_file_name, "w") as f:
        f.write(header)
        f.write("\n\n")
        f.write(header2)
        f.write("\n\n")
        f.write(table_md)
        f.write(footer)
        f.write("\n\n")
        f.write(header3)
        f.write("\n\n")
        f.write(table2_md)

## The End!