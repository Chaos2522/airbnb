import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


class DataTransformer:
    def __init__(self):
        pass

    def transform(self, calendar_df: pd.DataFrame, listings_df: pd.DataFrame,
                  listings_details_df: pd.DataFrame, neighbourhoods_df: pd.DataFrame,
                  geojson_gdf: gpd.GeoDataFrame):
        """
        Transforms raw source data into a star schema.

        Data Sources:
          - calendar_df: Contains daily data (listing_id, date, available, price).
          - listings_df: Contains core listing information (with default price, etc.).
          - listings_details_df: Contains additional listing details.
          - neighbourhoods_df: A CSV mapping with two columns:
                neighbourhood_group, neighbourhood
              This file assigns each neighborhood (granular) to a higher-level group (which we use as borough).
          - geojson_gdf: GeoDataFrame read from neighbourhoods.geojson.
                (In this design we use the CSV mapping for borough assignment.)

        Transformation Steps:
          1. Clean calendar data (convert dates, clean price, compute occupancy flag).
          2. Merge listings and listings_details to create an enriched dataset.
          3. Build DIM_LISTING:
             - Select listing-specific attributes.
             - Generate a new surrogate key (an auto-increment integer).
             - Preserve the original listing id in a backup column (ListingID_BK).
          4. Build DIM_LOCATION:
             - Extract location attributes (neighborhood, city, latitude, longitude).
             - Merge with DIM_LISTING to convert the original listing id into the surrogate key.
             - Normalize the neighborhood strings (trim and lowercase) and merge with the CSV mapping
               (neighbourhoods_df) to assign a borough value.
          5. Build DIM_DATE:
             - Derive date components (day, month, quarter, year, week, season) from calendar_df.
          6. Build FACT_DAILY_REVENUE:
             - Use calendar data to capture daily measures (daily_price, occupied_flag).
             - Merge in the default price from listings_df.
             - Convert the original listing id into the surrogate key from DIM_LISTING.
        """
        # ----- Step 1: Clean Calendar Data -----
        calendar_df['date'] = pd.to_datetime(calendar_df['date'])
        # Create an occupancy flag: 0 if available ('t'), else 1.
        calendar_df['occupied_flag'] = calendar_df['available'].apply(lambda x: 0 if x == 't' else 1)
        # Remove currency symbols and convert price to float.
        calendar_df['price'] = calendar_df['price'].replace(r"[\$,]", '', regex=True).astype(float)

        # ----- Step 2: Clean Listings Data -----
        # Rename 'id' to 'listing_id' in both listings CSVs for consistency.
        listings_df.rename(columns={'id': 'listing_id'}, inplace=True)
        listings_details_df.rename(columns={'id': 'listing_id'}, inplace=True)
        # Merge listings and listings_details on 'listing_id' to enrich listing attributes.
        merged_listings = pd.merge(
            listings_df,
            listings_details_df,
            on='listing_id',
            how='left',
            suffixes=('', '_det')
        )

        # ----- Step 3: Build DIM_LISTING -----
        # Select columns that describe the listing.
        dim_listing = merged_listings[
            ['listing_id', 'property_type', 'room_type', 'host_name', 'description']].drop_duplicates().copy()
        dim_listing.rename(columns={'host_name': 'host'}, inplace=True)
        # Remove any duplicate listings based on the original listing_id.
        dim_listing = dim_listing.drop_duplicates(subset=['listing_id']).reset_index(drop=True)
        # Create a new surrogate key.
        dim_listing['listing_id_surrogate'] = dim_listing.index + 1
        # Preserve the original listing id in a backup column.
        dim_listing.rename(columns={'listing_id': 'ListingID_BK'}, inplace=True)
        # Reorder columns: surrogate key first, then backup and other attributes.
        dim_listing = dim_listing[
            ['listing_id_surrogate', 'ListingID_BK', 'property_type', 'room_type', 'host', 'description']]
        # (Optionally, you could rename 'listing_id_surrogate' to 'listing_id' if that is your primary key.)

        # ----- Step 4: Build DIM_LOCATION -----
        # Extract location-specific fields.
        dim_location = merged_listings[
            ['listing_id', 'neighbourhood_cleansed', 'city', 'latitude', 'longitude']].drop_duplicates().copy()
        # Rename 'neighbourhood_cleansed' to 'neighborhood'.
        dim_location.rename(columns={'neighbourhood_cleansed': 'neighborhood'}, inplace=True)
        # Initialize borough as None (to be filled in next step).
        dim_location['borough'] = None
        # Remove duplicate records based on listing_id.
        dim_location = dim_location.drop_duplicates(subset=['listing_id']).reset_index(drop=True)
        # Merge with DIM_LISTING to convert the original listing id into the surrogate key.
        # In DIM_LISTING, the original id is stored as 'ListingID_BK'.
        dim_location = dim_location.merge(
            dim_listing[['listing_id_surrogate', 'ListingID_BK']],
            left_on='listing_id',
            right_on='ListingID_BK',
            how='left'
        )
        # Drop the original listing id columns.
        dim_location.drop(columns=['listing_id', 'ListingID_BK'], inplace=True)
        # Rename the surrogate key column to 'listing_id' (this will be used as a foreign key).
        dim_location.rename(columns={'listing_id_surrogate': 'listing_id'}, inplace=True)

        # --- NEW: Assign Borough Using CSV Mapping (neighbourhoods.csv) ---
        # Normalize the neighborhood values in DIM_LOCATION (remove spaces, convert to lowercase).
        dim_location['neighborhood'] = dim_location['neighborhood'].str.strip().str.lower()
        # Normalize the 'neighbourhood' column in the CSV mapping similarly.
        neighbourhoods_df['neighborhood'] = neighbourhoods_df['neighbourhood'].str.strip().str.lower()
        # Merge DIM_LOCATION with the neighbourhoods CSV mapping on the 'neighborhood' column.
        # The CSV mapping has two columns: 'neighbourhood_group' (the borough) and 'neighbourhood' (the neighborhood).
        dim_location = dim_location.merge(
            neighbourhoods_df[['neighbourhood_group', 'neighborhood']],
            on='neighborhood',
            how='left'
        )
        # Set the borough in DIM_LOCATION to the value from 'neighbourhood_group'.
        dim_location['borough'] = dim_location['neighbourhood_group']
        # Drop the extra column from the mapping.
        dim_location.drop(columns=['neighbourhood_group'], inplace=True)

        # ----- Step 5: Build DIM_DATE -----
        # Derive components from the calendar date.
        calendar_df['day'] = calendar_df['date'].dt.day
        calendar_df['month'] = calendar_df['date'].dt.month
        calendar_df['year'] = calendar_df['date'].dt.year
        calendar_df['quarter'] = calendar_df['date'].dt.quarter
        calendar_df['week'] = calendar_df['date'].dt.isocalendar().week

        # Function to compute season.
        def get_season(month):
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            else:
                return 'Fall'

        calendar_df['season'] = calendar_df['month'].apply(get_season)
        # Create DIM_DATE using unique dates.
        dim_date = calendar_df[['date', 'day', 'month', 'quarter', 'year', 'week', 'season']].drop_duplicates().copy()
        dim_date.rename(columns={'date': 'full_date'}, inplace=True)

        # ----- Step 6: Build FACT_DAILY_REVENUE -----
        # Start with calendar data to capture daily metrics.
        fact_daily_revenue = calendar_df[['listing_id', 'date', 'price', 'occupied_flag']].copy()
        fact_daily_revenue.rename(columns={'price': 'daily_price'}, inplace=True)
        # Merge in the default price from listings_df (the fixed price).
        fact_daily_revenue = fact_daily_revenue.merge(
            listings_df[['listing_id', 'price']],
            on='listing_id',
            how='left',
            suffixes=('', '_default')
        )
        fact_daily_revenue.rename(columns={'price_default': 'default_price'}, inplace=True)
        # Convert the original listing id in the fact table to the surrogate key from DIM_LISTING.
        fact_daily_revenue = fact_daily_revenue.merge(
            dim_listing[['listing_id_surrogate', 'ListingID_BK']],
            left_on='listing_id',
            right_on='ListingID_BK',
            how='left'
        )
        # Drop the original listing id columns.
        fact_daily_revenue.drop(columns=['listing_id', 'ListingID_BK'], inplace=True)
        # Rename the surrogate key column to 'listing_id' for consistency.
        fact_daily_revenue.rename(columns={'listing_id_surrogate': 'listing_id'}, inplace=True)
        # Note: We intentionally do not merge any location descriptive attributes (e.g., borough)
        # into the fact table to keep it lean and normalized.

        return dim_listing, dim_location, dim_date, fact_daily_revenue
