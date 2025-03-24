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
        Transforms raw source data into a star schema with dimensions and a fact table,
        with the following modifications for location: (Feedback)
          - DIM_LISTING uses a surrogate key (listing_id_surrogate) for listings.
          - DIM_LOCATION is built using the natural key (latitude, longitude) and contains attributes such as
            neighborhood, city, and borough.
          - A mapping from the original listing_id to (latitude, longitude) is created.
          - FACT_DAILY_REVENUE is built using the surrogate listing key and enriched with latitude and longitude.

        Transformation Steps:
          1. Clean calendar data.
          2. Clean and merge listings data.
          3. Build DIM_LISTING.
          4. Build DIM_LOCATION and a mapping from original listing_id to location (lat/long).
          5. Build DIM_DATE.
          6. Build FACT_DAILY_REVENUE:
             - Convert the original listing_id to the surrogate key from DIM_LISTING.
             - Merge in latitude and longitude from the mapping.
             - Merge in date_id from DIM_DATE.
        """
        # ----- Step 1: Clean Calendar Data -----
        calendar_df['date'] = pd.to_datetime(calendar_df['date'])
        # Occupancy flag: 0 if available ('t'), else 1.
        calendar_df['occupied_flag'] = calendar_df['available'].apply(lambda x: 0 if x == 't' else 1)
        # Clean price: remove '$' and convert to float.
        calendar_df['price'] = calendar_df['price'].replace(r"[\$,]", '', regex=True).astype(float)

        # ----- Step 2: Clean and Merge Listings Data -----
        # Rename 'id' to 'listing_id' for consistency.
        listings_df.rename(columns={'id': 'listing_id'}, inplace=True)
        listings_details_df.rename(columns={'id': 'listing_id'}, inplace=True)
        # Merge listings with listings_details to enrich listing attributes.
        merged_listings = pd.merge(
            listings_df,
            listings_details_df,
            on='listing_id',
            how='left',
            suffixes=('', '_det')
        )

        # ----- Step 3: Build DIM_LISTING -----
        # Select listing-specific attributes, including the listing name.
        dim_listing = merged_listings[
            ['listing_id', 'name', 'property_type', 'room_type', 'host_name', 'description']].drop_duplicates().copy()
        # Rename columns: 'name' to 'listing_name', 'host_name' to 'host'.
        dim_listing.rename(columns={'name': 'listing_name', 'host_name': 'host'}, inplace=True)
        # Remove duplicates based on the original listing_id.
        dim_listing = dim_listing.drop_duplicates(subset=['listing_id']).reset_index(drop=True)
        # Create a surrogate key for DIM_LISTING.
        dim_listing['listing_id_surrogate'] = dim_listing.index + 1
        # Preserve the original listing id in a backup column.
        dim_listing.rename(columns={'listing_id': 'ListingID_BK'}, inplace=True)
        # Reorder columns so that the surrogate key is first.
        dim_listing = dim_listing[
            ['listing_id_surrogate', 'ListingID_BK', 'listing_name', 'property_type', 'room_type', 'host',
             'description']]

        # ----- Step 4: Build DIM_LOCATION and Mapping -----
        # Extract location-related columns.
        temp_location = merged_listings[
            ['listing_id', 'neighbourhood_cleansed', 'city', 'latitude', 'longitude']].copy()
        temp_location.rename(columns={'neighbourhood_cleansed': 'neighborhood'}, inplace=True)
        # Normalize neighborhood text.
        temp_location['neighborhood'] = temp_location['neighborhood'].str.strip().str.lower()
        # Merge with the CSV mapping (neighbourhoods.csv) to assign a borough.
        # Normalize CSV mapping: convert column names to lowercase and normalize neighborhood values.
        neighbourhoods_df.columns = [col.strip().lower() for col in neighbourhoods_df.columns]
        neighbourhoods_df['neighborhood'] = neighbourhoods_df['neighbourhood'].str.strip().str.lower()
        temp_location = temp_location.merge(
            neighbourhoods_df[['neighbourhood_group', 'neighborhood']],
            on='neighborhood',
            how='left'
        )
        temp_location['borough'] = temp_location['neighbourhood_group']
        temp_location.drop(columns=['neighbourhood_group'], inplace=True)
        # Build DIM_LOCATION: Unique combinations of (neighborhood, city, latitude, longitude).
        # The natural key will be (latitude, longitude).
        dim_location = temp_location.drop_duplicates(subset=['latitude', 'longitude']).reset_index(drop=True)
        # Retain relevant location attributes.
        dim_location = dim_location[['neighborhood', 'city', 'latitude', 'longitude', 'borough']]

        # Build a mapping from the original listing_id to its location attributes (latitude and longitude).
        # Drop duplicates on listing_id to ensure uniqueness.
        listing_location_mapping = temp_location[['listing_id', 'latitude', 'longitude']].drop_duplicates(
            subset=['listing_id']).copy()
        # To avoid column name collisions during merge, rename the key column.
        listing_location_mapping.rename(columns={'listing_id': 'orig_listing_id_map'}, inplace=True)

        # ----- Step 5: Build DIM_DATE -----
        # Extract date components.
        calendar_df['day'] = calendar_df['date'].dt.day
        calendar_df['month'] = calendar_df['date'].dt.month
        calendar_df['year'] = calendar_df['date'].dt.year
        calendar_df['quarter'] = calendar_df['date'].dt.quarter
        calendar_df['week'] = calendar_df['date'].dt.isocalendar().week

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
        # Build DIM_DATE from unique dates.
        dim_date = calendar_df[['date', 'day', 'month', 'quarter', 'year', 'week', 'season']].drop_duplicates().copy()
        dim_date.rename(columns={'date': 'full_date'}, inplace=True)
        dim_date = dim_date.sort_values('full_date').reset_index(drop=True)
        dim_date['date_id'] = dim_date.index + 1
        dim_date = dim_date[['date_id', 'full_date', 'day', 'month', 'quarter', 'year', 'week', 'season']]

        # ----- Step 6: Build FACT_DAILY_REVENUE with Latitude and Longitude -----
        # Start with the calendar data as the base for the fact table.
        fact_daily_revenue = calendar_df[['listing_id', 'date', 'price', 'occupied_flag']].copy()
        fact_daily_revenue.rename(columns={'price': 'daily_price'}, inplace=True)
        # Preserve the original listing_id for later use.
        fact_daily_revenue['orig_listing_id'] = fact_daily_revenue['listing_id']
        # Merge in the default price from listings_df.
        fact_daily_revenue = fact_daily_revenue.merge(
            listings_df[['listing_id', 'price']],
            on='listing_id',
            how='left',
            suffixes=('', '_default')
        )
        fact_daily_revenue.rename(columns={'price_default': 'default_price'}, inplace=True)
        # Convert the fact table's listing_id to the surrogate key from DIM_LISTING.
        fact_daily_revenue = fact_daily_revenue.merge(
            dim_listing[['listing_id_surrogate', 'ListingID_BK']],
            left_on='listing_id',
            right_on='ListingID_BK',
            how='left'
        )
        # Drop the original listing_id columns.
        fact_daily_revenue.drop(columns=['listing_id', 'ListingID_BK'], inplace=True)
        # Rename the surrogate key column to 'listing_id'.
        fact_daily_revenue.rename(columns={'listing_id_surrogate': 'listing_id'}, inplace=True)
        # Merge in latitude and longitude from the mapping.
        # Use the preserved original listing id ('orig_listing_id') to match with the mapping.
        fact_daily_revenue = fact_daily_revenue.merge(
            listing_location_mapping,
            left_on='orig_listing_id',
            right_on='orig_listing_id_map',
            how='left',
            suffixes=('', '_loc')
        )
        # Drop the extra mapping key and the preserved column.
        fact_daily_revenue.drop(columns=['orig_listing_id', 'orig_listing_id_map'], inplace=True)
        # Merge with DIM_DATE to get the surrogate date key.
        fact_daily_revenue = fact_daily_revenue.merge(
            dim_date[['date_id', 'full_date']],
            left_on='date',
            right_on='full_date',
            how='left'
        )
        # Drop raw date columns; FACT_DAILY_REVENUE now references DIM_DATE via date_id.
        fact_daily_revenue.drop(columns=['date', 'full_date'], inplace=True)

        # Now, FACT_DAILY_REVENUE contains:
        # - listing_id (the surrogate key from DIM_LISTING),
        # - daily_price, default_price, occupied_flag,
        # - latitude and longitude (from the mapping),
        # - date_id (from DIM_DATE).

        return dim_listing, dim_location, dim_date, fact_daily_revenue
