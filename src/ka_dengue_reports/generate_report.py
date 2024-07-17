import datetime

import pandas as pd
from dataio.download import download_dataset_v2

from ka_dengue_reports import get_regionIDs

analysis_window = 7
report_file_name = "reports/KA Den Report 17 July.md"

download_dataset_v2(dsid="GS0015DS0034")
regionIDs_df, regionIDs_dict = get_regionIDs()
ka_districts = regionIDs_df[regionIDs_df["parentID"]=="state_29"].reset_index(drop=True)

download_dataset_v2(dsid="EP0005DS0014", contains_any = "ihip")

df = pd.read_csv("data/EP0005DS0014-KA_Dengue_LL/ihip/ka-line-list-ihip.csv")

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
hotspots = hotspots.groupby(by = ['location.admin2.ID', 'location.admin2.name', 'location.admin3.ID',
                                  'location.admin3.name', 'location.admin5.ID', 'location.admin5.name'])['event.test'].sum().reset_index().rename(columns={"event.test":"number_of_cases"})  # noqa: E501

hotspots['whether_hotspot'] = hotspots['number_of_cases'].apply(lambda x: x >= 2)

# District Table

district_table = df.groupby(by="location.admin2.ID")["event.test"].sum().reset_index().rename(columns={"event.test":"total_cases_by_district"})  # noqa: E501
district_table = district_table.merge(hotspots.groupby(by=["location.admin2.ID", "location.admin2.name"])["location.admin3.ID"].count().reset_index().rename(columns={"location.admin3.ID":"cases_with_info"}), on="location.admin2.ID", how="left")  # noqa: E501
district_table = district_table.merge(hotspots.groupby(by="location.admin2.ID")["whether_hotspot"].sum().reset_index().rename(columns={"whether_hotspot":"number_of_hotspots"}), on="location.admin2.ID", how="left") # noqa: E501
district_table = district_table.merge(ka_districts, left_on = "location.admin2.ID",
                                      right_on = "regionID", how = "right").drop(columns = ["location.admin2.ID",
                                                                                            "regionName", "parentID"]).fillna(0)
district_table["district_code"] = district_table["regionID"].str.split("_").str.get(1)

district_table = district_table.drop(columns = ["regionID"])
district_table = district_table[["district_code", "location.admin2.name", "total_cases_by_district",
                                 "cases_with_info","number_of_hotspots"]]
district_table = district_table.rename(columns = {"location.admin2.name":"district_name"})

district_table.loc[len(district_table.index)] = [pd.NA, "Total", district_table["total_cases_by_district"].sum(),
                                                 district_table["cases_with_info"].sum(), district_table["number_of_hotspots"].sum()]

district_table = district_table.rename(columns = {"district_code":"District Code",
                                                  "district_name":"District Name", 
                                                  "total_cases_by_district":"Total Cases",
                                                  "cases_with_info":"Cases with Village/Ward Info",
                                                  "number_of_hotspots":"Number of Hotspots"})
district_dict = district_table.fillna("").to_dict(orient='records')

datadict_df = pd.json_normalize(district_dict)


cases_with_village_ward_info = len(df[(df["location.admin.coarseness"] == "village") | (df["location.admin.coarseness"] == "ward")])
cases_with_subdistrict_ulb_info = len(df[(df["location.admin.coarseness"] == "subdistrict") | (df["location.admin.coarseness"] == "ulb")])

header = f"""### Karnataka Dengue Report, dtd. {datetime.datetime.today().strftime('%B %d %Y')}

#### Summary
* **Report Period**: {min_date.strftime('%B %d %Y')} - {max_result_date.strftime('%B %d %Y')}
* **Analysis Window**: {analysis_window} days
* **Total Cases**: {len(df)} cases were reported in this time period.
* **Hotspots**: {len(hotspots[hotspots["whether_hotspot"]])} villages/wards were identified as hotspots (with 2 or more cases).
* **Location Information Availability**: Out of {len(df)} cases, {cases_with_village_ward_info} cases have village/ward level information and {cases_with_subdistrict_ulb_info} cases have subdistrict/ULB level information.

"""

with open(report_file_name, "w") as f:
    f.write(header)

datadict_df.to_markdown(buf=report_file_name, index=False, mode = "ab")

