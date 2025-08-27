import streamlit as st

st.set_page_config(page_title="Vessel Transit Counts App", layout="wide")
st.title("📱 Vessel Transit Counts App")
st.markdown("🎯 Goal: Because a vessel can transit an area multiple times, this app helps you extract all such transits.")
st.markdown("Step 1: Define a 'BIG_POLYGON' and download the data in '.parquet' format from MSI SEASCAPE")
st.markdown("Step 2: Define a 'SMALL_POLYGON', and the app will count the transits within this area.")

# --- init session flags/state ---
if "run" not in st.session_state:
    st.session_state.run = False
if "poly_ready" not in st.session_state:
    st.session_state.poly_ready = False

######################################## Part 1 Visualise Polygons ########################################
with st.form("poly_form"):

    st.markdown("### Define Small and Big Polygons")
    

    small_poly_text = st.text_area(
        "Enter SMALL_POLYGON coordinates (lat, lon) — you'll count transits inside this area.",
        value="9.310730, -80.011146\n9.312522, -79.810156\n9.107972, -79.714580\n9.028758, -80.063607",
        height=200,
        help="The default area is Gatun Lake in the Panama Canal."
    )

    big_poly_text = st.text_area(
        "Enter BIG_POLYGON coordinates (lat, lon) - same area as your MSI SEASCAPE download.",
        value="9.575993, -80.388079\n9.753340, -79.601293\n8.886929, -79.016116\n8.459045, -79.862967",
        height=200,
        help="The default area is the Panama Canal."
    )

    poly_submit = st.form_submit_button("Visualise Polygons")

# if "poly_ready" not in st.session_state:
#     st.session_state.poly_ready = False
if poly_submit:
    st.session_state.poly_ready = True

# ---- Parse user input polygons ----
def parse_coords(text):
    coords = []
    for line in text.strip().splitlines():
        try:
            lat, lon = [float(x.strip()) for x in line.split(",")]
            coords.append((lat, lon))
        except Exception:
            st.warning(f"Could not parse line: '{line}' (expected 'lat, lon')")
    return coords

BIG_POLYGON = parse_coords(big_poly_text)
SMALL_POLYGON = parse_coords(small_poly_text)

# step 1. convert from (lat, lon) to (lon, lat)
# step 2. apply Polygon()
from shapely.geometry import Polygon
big_poly = Polygon([(lon, lat) for lat, lon in BIG_POLYGON])
small_poly = Polygon([(lon, lat) for lat, lon in SMALL_POLYGON])

# plot
import folium
import numpy as np
from shapely.geometry import mapping
def plot_two_polygons(big_poly, small_poly, zoom_start=8):
    # Center map between both polygons
    all_coords = list(big_poly.exterior.coords) + list(small_poly.exterior.coords)
    lons, lats = zip(*all_coords)
    center = [float(np.mean(lats)), float(np.mean(lons))]

    m = folium.Map(location=center, zoom_start=zoom_start)

    folium.TileLayer(
        tiles='https://{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
        attr='Google',
        name='Google Satellite',
        max_zoom=20,
        subdomains=['mt0', 'mt1', 'mt2', 'mt3']
    ).add_to(m)

    # Add big polygon (blue)
    folium.GeoJson(
        data=mapping(big_poly),
        name="Big Polygon",
        style_function=lambda x: {
            "fillColor": "#569FD3",
            "color": "black",
            "weight": 2,
            "fillOpacity": 0.3,
        },
    ).add_to(m)

    # Add small polygon (red)
    folium.GeoJson(
        data=mapping(small_poly),
        name="Small Polygon",
        style_function=lambda x: {
            "fillColor": "#DB1926",
            "color": "black",
            "weight": 2,
            "fillOpacity": 0.3,
        },
    ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    return m

from streamlit_folium import st_folium
if st.session_state.poly_ready:
    st.subheader("Area Preview:")
    st.caption("• Ensure the MSI Seascape data fully covers the Big Polygon.\n• The Small Polygon should not include berths/ports.")
    st_folium(plot_two_polygons(big_poly, small_poly, zoom_start=8), width=700, height=500)

######################################## Part 2 Calculate ########################################

with st.form("run_form"):

    # Folder Paths
    st.markdown("### Define input and output paths")

    INPUT_DATA_FOLDER = st.text_input(
        "Input folder containing .parquet files",
        value="I:/MSI Library/Staff Folders/Jingzhou Zhao/Monthly AIS Data/Containership",
        help="Provide a full folder path with .parquet files",
    )
    OUTPUT_DATA_FOLDER = st.text_input(
        "Output Excel file path",
        value="C:/Users/jingzhou.zhao/Downloads",
        help="Provide a full folder path",
    )
    run_submit  = st.form_submit_button("Start running")

if run_submit:
    st.session_state.run = True

if st.session_state.run:

    ##### Append Data #####
    import pandas as pd
    pd.set_option("display.max_rows", 500)

    import os
    import pyarrow.parquet as pq

    import geopandas as gpd

    st.write("⏳ I am reading the input files!")
    parquet_files = [f for f in os.listdir(INPUT_DATA_FOLDER) if f.endswith('.parquet')]

    dfs = []
    for file in parquet_files:
        columns = ["imo", "timestamp", "latitude", "longitude", "destination"]
        df = pq.read_table(os.path.join(INPUT_DATA_FOLDER, file), columns=columns).to_pandas()        
        st.write(f"Reading {file} — min: {df['timestamp'].min()} | max: {df['timestamp'].max()} | rows: {len(df):,}")
        dfs.append(df)

    st.write("⏳ I am appending!")
    df_all = pd.concat(dfs, ignore_index=True)
    st.write(f"✅ Appending is done! With {len(df_all)} rows\n")

    st.write("⏳ I am filtering the appended data with your Big Polygon.")

    df_all = df_all.sort_values(["imo", "timestamp"]).reset_index(drop=True)
    del dfs

    gdf = gpd.GeoDataFrame(
        df_all,
        geometry=gpd.points_from_xy(df_all["longitude"], df_all["latitude"]),
        crs="EPSG:4326")
    gdf = gdf[gdf.within(big_poly)]
    st.write(f"✅ {len(gdf)} rows were filtered within your Big Polygon.")

    ##### Process Data #####
    import pandas as pd
    import geopandas as gpd

    # --- Prep data ---
    gdf_clean = gdf.copy()
    gdf_clean = gdf_clean.sort_values(by=["imo", "timestamp"])

    # --- Replace the rectangle filter with polygon membership ---
    gdf_clean["in_box"] = gdf_clean.geometry.apply(small_poly.covers)

    # 🔴 Make first observation for each imo False or you may have error
    first_idx = gdf_clean.groupby("imo").head(1).index
    gdf_clean.loc[first_idx, "in_box"] = False

    # --- Transitions & voyage pairing (unchanged) ---
    gdf_clean["in_box_shift"] = gdf_clean.groupby("imo")["in_box"].shift(1)
    gdf_clean["entry"] = (gdf_clean["in_box"] == True) & (gdf_clean["in_box_shift"] == False)
    gdf_clean["exit"]  = (gdf_clean["in_box"] == False) & (gdf_clean["in_box_shift"] == True)
    gdf_clean[["entry", "exit"]] = gdf_clean[["entry", "exit"]].fillna(False)

    gdf_clean["entry_id"] = gdf_clean.groupby("imo")["entry"].cumsum()
    gdf_clean["exit_id"]  = gdf_clean.groupby("imo")["exit"].cumsum()

    entries = gdf_clean[gdf_clean["entry"]][["imo", "timestamp", "entry_id"]].rename(columns={"timestamp": "entry_time"})
    exits   = gdf_clean[gdf_clean["exit"]][["imo", "timestamp", "exit_id"]].rename(columns={"timestamp": "exit_time"})

    entries["pair_id"] = entries["entry_id"]
    exits["pair_id"]   = exits["exit_id"]

    voyages = pd.merge(entries, exits, on=["imo", "pair_id"], how="inner")
    voyages["duration"] = voyages["exit_time"] - voyages["entry_time"]

    st.write(f"\n🧭 Total complete transits: {len(voyages)}")
    st.write(f"⛵ Unique ships with complete transits: {voyages['imo'].nunique()}")

    from datetime import datetime
    file_name = f"transit_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"
    voyages.to_excel(os.path.join(OUTPUT_DATA_FOLDER, file_name), index=False)
    st.write("🥳 All Done. Thanks for using this app.")