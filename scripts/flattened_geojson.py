import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os

def create_site_geojson(lte_csv, nr_csv, output_path, week_num):
    # Step 1: Load CSVs
    df_lte = pd.read_csv(lte_csv)
    df_nr = pd.read_csv(nr_csv)

    # Step 2: Combine and extract relevant columns
    df_combine = pd.concat([df_lte, df_nr], ignore_index=True)
    df_1 = df_combine[["Latitude", "Longitude", "Site ID", "Site Type", "System"]].copy()

    # Step 3: Group by Site ID and aggregate
    df = df_1.groupby("Site ID").agg({
        "Latitude": "first",
        "Longitude": "first",
        "Site Type": lambda x: ", ".join(sorted(set(x))),
        "System": lambda x: ", ".join(sorted(set(x)))
    }).reset_index()

    # Step 4: Create geometry column
    df["geometry"] = df.apply(lambda row: Point(row["Longitude"], row["Latitude"]), axis=1)

    # Step 5: Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # Step 6: Save to GeoJSON
    filename = f"site_flattened_{week_num}.geojson"
    output_file = os.path.join(output_path, filename)
    gdf.to_file(output_file, driver="GeoJSON")

    print(f" GeoJSON file created: {output_file}")
    return output_file



    

    