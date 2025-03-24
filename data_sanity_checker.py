import pandas as pd


class DataSanityChecker:
    def __init__(self, raw_listings_df: pd.DataFrame, raw_calendar_df: pd.DataFrame,
                 dim_listing: pd.DataFrame, dim_location: pd.DataFrame,
                 fact_daily_revenue: pd.DataFrame):
        """
        Initialize the checker with both source and transformed data.

        Parameters:
          - raw_listings_df: Source listings data (must include 'listing_id', 'latitude', 'longitude').
          - raw_calendar_df: Source calendar data.
          - dim_listing: Transformed DIM_LISTING (with surrogate key and ListingID_BK).
          - dim_location: Transformed DIM_LOCATION (with natural key from latitude/longitude).
          - fact_daily_revenue: Transformed FACT_DAILY_REVENUE (which includes surrogate listing_id,
            latitude and longitude, and date_id).
        """
        self.raw_listings_df = raw_listings_df.copy()
        self.raw_calendar_df = raw_calendar_df.copy()
        self.dim_listing = dim_listing.copy()
        self.dim_location = dim_location.copy()
        self.fact_daily_revenue = fact_daily_revenue.copy()

    def check_dim_listing_ids(self):
        """
        Check that every unique listing_id from raw_listings_df appears in DIM_LISTING as ListingID_BK.
        """
        source_ids = set(self.raw_listings_df['listing_id'].unique())
        transformed_ids = set(self.dim_listing['ListingID_BK'].unique())
        missing = source_ids - transformed_ids
        assert not missing, f"Missing listing IDs in DIM_LISTING: {missing}"
        print("DIM_LISTING sanity check: All source listing IDs are present in ListingID_BK.")

    def check_dim_location_latlong(self):
        """
        Check that DIM_LOCATION contains the correct latitude and longitude values.
        Since DIM_LOCATION is built as the unique set of (latitude, longitude) from the source,
        we verify that each (lat, long) pair in DIM_LOCATION exists in the raw listings.
        """
        # Build a set of unique (lat, long) pairs from raw_listings_df.
        raw_pairs = set(
            tuple(x) for x in self.raw_listings_df[['latitude', 'longitude']].drop_duplicates().values
        )
        # Build a set of unique (lat, long) pairs from DIM_LOCATION.
        dim_pairs = set(
            tuple(x) for x in self.dim_location[['latitude', 'longitude']].drop_duplicates().values
        )
        missing = raw_pairs - dim_pairs
        assert not missing, f"Missing latitude/longitude pairs in DIM_LOCATION: {missing}"
        print("DIM_LOCATION sanity check: All latitude/longitude pairs from source are present.")

    def check_fact_listing_latlong(self):
        """
        Check that the latitude and longitude values in FACT_DAILY_REVENUE match the source data.
        The process is:
          1. Build a mapping from raw_listings_df: original listing_id -> (latitude, longitude).
          2. Join DIM_LISTING with this mapping using ListingID_BK to get a mapping from the surrogate key to (lat, long).
          3. Join FACT_DAILY_REVENUE with this mapping on surrogate listing_id.
          4. Compare the lat/long values.
        """
        # Build raw mapping: for each listing_id in raw_listings_df, take the first occurrence of (latitude, longitude)
        raw_mapping = self.raw_listings_df.groupby('listing_id')[['latitude', 'longitude']].first().reset_index()
        # In DIM_LISTING, ListingID_BK holds the original id and listing_id_surrogate is the surrogate key.
        mapping = self.dim_listing[['listing_id_surrogate', 'ListingID_BK']].copy()
        # Merge mapping with raw_mapping on ListingID_BK == raw listing_id.
        mapping = pd.merge(mapping, raw_mapping, left_on='ListingID_BK', right_on='listing_id', how='left')
        # Drop the redundant raw listing_id column.
        mapping.drop(columns=['listing_id'], inplace=True)
        mapping.rename(columns={'listing_id_surrogate': 'listing_id'}, inplace=True)
        # Now join FACT_DAILY_REVENUE (which should include latitude and longitude from the mapping during transformation)
        # with our mapping on surrogate listing_id.
        fact_check = pd.merge(self.fact_daily_revenue, mapping, on='listing_id', how='left', suffixes=('', '_raw'))
        # Compare the latitude and longitude from FACT_DAILY_REVENUE with those in mapping.
        mismatches = fact_check[
            (fact_check['latitude'] != fact_check['latitude_raw']) |
            (fact_check['longitude'] != fact_check['longitude_raw'])
            ]
        assert mismatches.empty, (
            f"FACT_DAILY_REVENUE lat/long mismatches:\n{mismatches[['listing_id', 'latitude', 'latitude_raw', 'longitude', 'longitude_raw']]}"
        )
        print("FACT_DAILY_REVENUE sanity check: Latitude and longitude values match source data.")

    def check_fact_row_count(self):
        """
        Check that the number of rows in FACT_DAILY_REVENUE equals the number of rows in the raw calendar data.
        (Assuming one fact row per calendar record.)
        """
        expected = len(self.raw_calendar_df)
        actual = len(self.fact_daily_revenue)
        assert expected == actual, f"FACT_DAILY_REVENUE row count mismatch: expected {expected}, got {actual}"
        print("FACT_DAILY_REVENUE sanity check: Row count is as expected.")

    def run_all_checks(self):
        """
        Run all sanity checks.
        """
        self.check_dim_listing_ids()
        self.check_dim_location_latlong()
        self.check_fact_listing_latlong()
        self.check_fact_row_count()
        print("All data sanity checks passed successfully.")
