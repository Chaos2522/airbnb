import pandas as pd


class DataTransformer:
    def __init__(self):
        pass

    def transform(self, calendar_df: pd.DataFrame, listings_df: pd.DataFrame,
                  listings_details_df: pd.DataFrame, neighbourhoods_df: pd.DataFrame):
        # --- Clean calendar data ---
        calendar_df['date'] = pd.to_datetime(calendar_df['date'])
        calendar_df['occupied_flag'] = calendar_df['available'].apply(lambda x: 0 if x == 't' else 1)
        calendar_df['price'] = calendar_df['price'].replace(r"[\$,]", '', regex=True).astype(float)

        # --- Clean listings data ---
        listings_df.rename(columns={'id': 'listing_id'}, inplace=True)
        listings_details_df.rename(columns={'id': 'listing_id'}, inplace=True)

        # Merge listings and details for enriched dimension info
        merged_listings = pd.merge(listings_df, listings_details_df, on='listing_id', how='left', suffixes=('', '_det'))

        # --- Build DIM_LISTING ---
        dim_listing = merged_listings[
            ['listing_id', 'property_type', 'room_type', 'host_name', 'description']
        ].drop_duplicates().copy()
        dim_listing.rename(columns={'host_name': 'host'}, inplace=True)

        # --- Build DIM_LOCATION ---
        dim_location = merged_listings[
            ['listing_id', 'neighbourhood_cleansed', 'city', 'latitude', 'longitude']
        ].drop_duplicates().copy()
        dim_location.rename(columns={'neighbourhood_cleansed': 'neighborhood'}, inplace=True)
        dim_location['borough'] = None  # Set or compute borough if applicable

        # *** NEW: Assign surrogate keys for locations ***
        # Ensure one record per listing_id, then create a surrogate key column.
        dim_location = dim_location.drop_duplicates(subset=['listing_id']).reset_index(drop=True)
        dim_location['location_id'] = dim_location.index + 1

        # --- Build DIM_DATE ---
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
        dim_date = calendar_df[['date', 'day', 'month', 'quarter', 'year', 'week', 'season']].drop_duplicates().copy()
        dim_date.rename(columns={'date': 'full_date'}, inplace=True)

        # --- Build FACT_DAILY_REVENUE ---
        # Create fact table from calendar data – this column represents the day-specific price (if available)
        fact_daily_revenue = calendar_df[['listing_id', 'date', 'price', 'occupied_flag']].copy()
        fact_daily_revenue.rename(columns={'price': 'daily_price'}, inplace=True)

        # Merge in location_id using listing_id (as before)
        fact_daily_revenue = fact_daily_revenue.merge(
            dim_location[['listing_id', 'location_id']],
            on='listing_id',
            how='left'
        )

        # *** NEW: Add default price from listings (the fixed price in listings.csv)
        # listings_df contains the default price in the "price" column.
        fact_daily_revenue = fact_daily_revenue.merge(
            listings_df[['listing_id', 'price']],
            on='listing_id',
            how='left',
            suffixes=('', '_default')
        )
        # Rename the default price column so that we have both "daily_price" and "default_price"
        fact_daily_revenue.rename(columns={'price_default': 'default_price'}, inplace=True)

        return dim_listing, dim_location, dim_date, fact_daily_revenue
