import pandas as pd


class DataExtractor:
    def __init__(self, paths: dict):
        """
        paths: dictionary of file paths, for example:
          {
            'calendar': 'resources/calendar.csv',
            'listings': 'resources/listings.csv',
            'listings_details': 'resources/listings_details.csv',
            'neighbourhoods': 'resources/neighbourhoods.csv',
            'geojson': 'resources/neighbourhoods.geojson'
          }
        """
        self.paths = paths

    def extract(self):
        """
        Reads the CSV files (and optionally GeoJSON) into Pandas DataFrames.
        Returns:
          calendar_df, listings_df, listings_details_df, neighbourhoods_df
        """
        # Use low_memory=False to avoid chunk-based type inference.
        calendar_df = pd.read_csv(self.paths['calendar'], low_memory=False)
        listings_df = pd.read_csv(self.paths['listings'], low_memory=False)
        listings_details_df = pd.read_csv(self.paths['listings_details'], low_memory=False)
        neighbourhoods_df = pd.read_csv(self.paths['neighbourhoods'], low_memory=False)

        # If you need the GeoJSON data:
        # import geopandas as gpd
        # geojson_gdf = gpd.read_file(self.paths['geojson'])
        # (then return geojson_gdf if needed)

        return calendar_df, listings_df, listings_details_df, neighbourhoods_df
