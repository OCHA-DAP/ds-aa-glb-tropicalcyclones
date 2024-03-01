import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
OLD_DATA_DIR = Path(os.getenv("AA_DATA_DIR"))
SLB_PROC_EC_DIR = (
    DATA_DIR / "public" / "processed" / "slb" / "ecmwf" / "cyclone_hindcasts"
)
ECMWF_RAW = (
    DATA_DIR / "public" / "exploration" / "glb" / "ecmwf" / "cyclone_hindcasts"
)


def xml2csv(filename, valid_names):
    try:
        tree = ET.parse(filename)
    except ET.ParseError:
        print("Error with file, skipping")
        return
    root = tree.getroot()

    prod_center = root.find("header/productionCenter").text
    baseTime = root.find("header/baseTime").text

    # Create one dictonary for each time point, and append it to a list
    for members in root.findall("data"):
        mtype = members.get("type")
        if mtype not in ["forecast", "ensembleForecast"]:
            continue
        for members2 in members.findall("disturbance"):
            cyclone_name = [
                name.text.lower().strip()
                for name in members2.findall("cycloneName")
            ]
            if not cyclone_name:
                continue
            cyclone_name = cyclone_name[0].lower()
            basin = members2.find("basin").text
            print(basin)
            # if cyclone_name not in list(df_typhoons["international"]):
            #     continue
            # print(f"Found typhoon {cyclone_name}")
            for members3 in members2.findall("fix"):
                tem_dic = {}
                tem_dic["mtype"] = [mtype]
                tem_dic["product"] = [
                    re.sub("\\s+", " ", prod_center).strip().lower()
                ]
                tem_dic["cyc_number"] = [
                    name.text for name in members2.findall("cycloneNumber")
                ]
                tem_dic["ensemble"] = [members.get("member")]
                tem_dic["speed"] = [
                    name.text
                    for name in members3.findall(
                        "cycloneData/maximumWind/speed"
                    )
                ]
                tem_dic["pressure"] = [
                    name.text
                    for name in members3.findall(
                        "cycloneData/minimumPressure/pressure"
                    )
                ]
                time = [name.text for name in members3.findall("validTime")]
                tem_dic["time"] = [
                    "/".join(time[0].split("T")[0].split("-"))
                    + ", "
                    + time[0].split("T")[1][:-1]
                ]
                # set sign of lat/lon based on N/S/E/W
                tem_dic["lat"] = [
                    "-" + name.text
                    if name.attrib.get("units") == "deg S"
                    else name.text
                    for name in members3.findall("latitude")
                ]
                tem_dic["lon"] = [
                    "-" + name.text
                    if name.attrib.get("units") == "deg W"
                    else name.text
                    for name in members3.findall("longitude")
                ]
                tem_dic["lead_time"] = [members3.get("hour")]
                tem_dic["forecast_time"] = [
                    "/".join(baseTime.split("T")[0].split("-"))
                    + ", "
                    + baseTime.split("T")[1][:-1]
                ]
                # tem_dic1 = dict(
                #     [
                #         (k, "".join(str(e).lower().strip() for e in v))
                #         for k, v in tem_dic.items()
                #     ]
                # )
                # Save to CSV
                # if not dry_run:
                #     outfile = (
                #         ECMWF_PROCESSED / f"csv/{cyclone_name}_all.csv"
                #     )
                #     pd.DataFrame(tem_dic1, index=[0]).to_csv(
                #         outfile,
                #         mode="a",
                #         header=not os.path.exists(outfile),
                #         index=False,
                #     )
