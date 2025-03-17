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
        Transforms raw source data into a star schema with dimensions and a fact table.

        Data Sources:
          - calendar_df: Contains daily calendar data (listing_id, date, availability, price).
          - listings_df: Contains main listing information (id, name, host, price, etc.).
          - listings_details_df: Contains additional listing details such as description.
          - neighbourhoods_df: A CSV mapping neighborhoods to a higher-level borough (neighbourhood_group).
          - geojson_gdf: A GeoDataFrame from neighbourhoods.geojson (not used in this transformation).

        Transformation Steps:
          1. Clean Calendar Data:
             - Convert 'date' to datetime, clean 'price', and compute an occupancy flag.
          2. Clean and Merge Listings Data:
             - Rename the 'id' column to 'listing_id' and merge listings with listing details.
          3. Build DIM_LISTING:
             - Select core listing attributes.
             - Create a surrogate key for listings (listing_id_surrogate) and preserve the original id as ListingID_BK.
          4. Build DIM_LOCATION:
             - Extract unique location attributes.
             - Normalize neighborhood names and merge with the CSV mapping to assign a borough.
             - Create a surrogate key for locations (location_id) and build a mapping from the original listing_id to location_id.
          5. Build DIM_DATE:
             - Derive date components (day, month, quarter, etc.) from the calendar.
             - Create a surrogate key (date_id) for the unique dates.
          6. Build FACT_DAILY_REVENUE:
             - Start from calendar data, merge in default price from listings, and convert the original listing_id to the surrogate key.
             - Merge in location_id from the listing-to-location mapping.
             - Merge in date_id from DIM_DATE.
             - Drop raw date columns so that the fact table references only surrogate keys and measures.
        """
        # ----- Step 1: Clean Calendar Data -----
        # Convert the 'date' column to datetime format.
        calendar_df['date'] = pd.to_datetime(calendar_df['date'])
        # Create an occupancy flag: 0 if available ('t'), else 1.
        calendar_df['occupied_flag'] = calendar_df['available'].apply(lambda x: 0 if x == 't' else 1)
        # Clean the 'price' field by removing '$' and commas, then convert to float.
        calendar_df['price'] = calendar_df['price'].replace(r"[\$,]", '', regex=True).astype(float)

        # ----- Step 2: Clean and Merge Listings Data -----
        # Standardize the listing ID by renaming the 'id' column to 'listing_id'.
        listings_df.rename(columns={'id': 'listing_id'}, inplace=True)
        listings_details_df.rename(columns={'id': 'listing_id'}, inplace=True)
        # Merge listings with additional details to enrich the listing attributes.
        merged_listings = pd.merge(
            listings_df,
            listings_details_df,
            on='listing_id',
            how='left',
            suffixes=('', '_det')
        )

        # ----- Step 3: Build DIM_LISTING -----
        # Select key listing attributes to build the listing dimension.
        dim_listing = merged_listings[
            ['listing_id', 'name', 'property_type', 'room_type', 'host_name', 'description']
        ].drop_duplicates().copy()
        # Rename columns for clarity.
        dim_listing.rename(columns={'name': 'listing_name', 'host_name': 'host'}, inplace=True)
        # Remove duplicates based on the original listing id.
        dim_listing = dim_listing.drop_duplicates(subset=['listing_id']).reset_index(drop=True)
        # Create a surrogate key for listings.
        dim_listing['listing_id_surrogate'] = dim_listing.index + 1
        # Preserve the original listing id as a backup.
        dim_listing.rename(columns={'listing_id': 'ListingID_BK'}, inplace=True)
        # Reorder columns: surrogate key first, then the backup id and attributes.
        dim_listing = dim_listing[
            ['listing_id_surrogate', 'ListingID_BK', 'listing_name', 'property_type', 'room_type', 'host',
             'description']
        ]

        # ----- Step 4: Build DIM_LOCATION with its own surrogate key -----
        # Extract location-specific columns from the merged listings.
        temp_location = merged_listings[
            ['listing_id', 'neighbourhood_cleansed', 'city', 'latitude', 'longitude']
        ].copy()
        # Rename 'neighbourhood_cleansed' to 'neighborhood' for consistency.
        temp_location.rename(columns={'neighbourhood_cleansed': 'neighborhood'}, inplace=True)
        # Normalize neighborhood names by stripping whitespace and converting to lowercase.
        temp_location['neighborhood'] = temp_location['neighborhood'].str.strip().str.lower()

        # Create DIM_LOCATION from unique combinations of location attributes.
        dim_location = temp_location.drop_duplicates(
            subset=['neighborhood', 'city', 'latitude', 'longitude']
        ).reset_index(drop=True)
        # Normalize the neighbourhoods CSV mapping: lower-case columns and neighborhood names.
        neighbourhoods_df.columns = [col.strip().lower() for col in neighbourhoods_df.columns]
        neighbourhoods_df['neighborhood'] = neighbourhoods_df['neighbourhood'].str.strip().str.lower()
        # Merge to assign borough information based on the neighborhood.
        dim_location = dim_location.merge(
            neighbourhoods_df[['neighbourhood_group', 'neighborhood']],
            on='neighborhood',
            how='left'
        )
        # Set the 'borough' column.
        dim_location['borough'] = dim_location['neighbourhood_group']
        # Drop the extra column from the mapping.
        dim_location.drop(columns=['neighbourhood_group'], inplace=True)
        # Create a surrogate key for DIM_LOCATION.
        dim_location = dim_location.reset_index(drop=True)
        dim_location['location_id'] = dim_location.index + 1
        # Reorder columns to place the surrogate key first.
        dim_location = dim_location[['location_id', 'neighborhood', 'city', 'latitude', 'longitude', 'borough']]

        # Build a mapping from the original listing id to the new location_id.
        listing_location_mapping = temp_location.merge(
            dim_location,
            on=['neighborhood', 'city', 'latitude', 'longitude'],
            how='left'
        )[["listing_id", "location_id"]].drop_duplicates(subset=["listing_id"])

        # ----- Step 5: Build DIM_DATE -----
        # Derive date components (day, month, year, quarter, week) from the calendar data.
        calendar_df['day'] = calendar_df['date'].dt.day
        calendar_df['month'] = calendar_df['date'].dt.month
        calendar_df['year'] = calendar_df['date'].dt.year
        calendar_df['quarter'] = calendar_df['date'].dt.quarter
        calendar_df['week'] = calendar_df['date'].dt.isocalendar().week

        # Function to determine the season based on the month.
        def get_season(month):
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            else:
                return 'Fall'

        # Apply the season function to the month column.
        calendar_df['season'] = calendar_df['month'].apply(get_season)
        # Build the date dimension table with unique dates.
        dim_date = calendar_df[['date', 'day', 'month', 'quarter', 'year', 'week', 'season']].drop_duplicates().copy()
        # Rename 'date' to 'full_date' for clarity.
        dim_date.rename(columns={'date': 'full_date'}, inplace=True)
        # Sort the dates and reset index.
        dim_date = dim_date.sort_values('full_date').reset_index(drop=True)
        # Create a surrogate key for DIM_DATE.
        dim_date['date_id'] = dim_date.index + 1
        # Reorder columns so that the surrogate key appears first.
        dim_date = dim_date[['date_id', 'full_date', 'day', 'month', 'quarter', 'year', 'week', 'season']]

        # ----- Step 6: Build FACT_DAILY_REVENUE with location_id and date_id -----
        # Start with the calendar data as the base for the fact table.
        fact_daily_revenue = calendar_df[['listing_id', 'date', 'price', 'occupied_flag']].copy()
        # Preserve the original listing_id for joining with the location mapping.
        fact_daily_revenue['orig_listing_id'] = fact_daily_revenue['listing_id']
        # Rename 'price' to 'daily_price' to differentiate from default price.
        fact_daily_revenue.rename(columns={'price': 'daily_price'}, inplace=True)
        # Merge in the default listing price from the listings data.
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
        # Drop the original listing_id columns after merging.
        fact_daily_revenue.drop(columns=['listing_id', 'ListingID_BK'], inplace=True)
        # Rename the surrogate key column to 'listing_id' to serve as a foreign key.
        fact_daily_revenue.rename(columns={'listing_id_surrogate': 'listing_id'}, inplace=True)

        # Merge in the location_id using the preserved original listing id.
        fact_daily_revenue = fact_daily_revenue.merge(
            listing_location_mapping,
            left_on='orig_listing_id',
            right_on='listing_id',
            how='left'
        )
        # Clean up extra columns from the join.
        fact_daily_revenue.drop(columns=['listing_id_y'], inplace=True)
        fact_daily_revenue.rename(columns={'listing_id_x': 'listing_id'}, inplace=True)
        fact_daily_revenue.drop(columns=['orig_listing_id'], inplace=True)

        # Merge with DIM_DATE to incorporate the date_id.
        fact_daily_revenue = fact_daily_revenue.merge(
            dim_date[['date_id', 'full_date']],
            left_on='date',
            right_on='full_date',
            how='left'
        )
        # Drop the raw date columns; now the fact table references DIM_DATE via date_id.
        fact_daily_revenue.drop(columns=['date', 'full_date'], inplace=True)

        return dim_listing, dim_location, dim_date, fact_daily_revenue
