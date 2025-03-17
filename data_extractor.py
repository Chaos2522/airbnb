import pandas as pd
import geopandas as gpd


class DataExtractor:
    def __init__(self, paths: dict):
        """
        Initialize the extractor with a dictionary of file paths.
        Example of paths:
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
        Reads the source files:
          - CSV files (calendar, listings, listings_details, neighbourhoods) using Pandas.
          - The geojson file using GeoPandas.
        Returns:
          calendar_df, listings_df, listings_details_df, neighbourhoods_df, geojson_gdf
        """
        calendar_df = pd.read_csv(self.paths['calendar'], low_memory=False)
        listings_df = pd.read_csv(self.paths['listings'], low_memory=False)
        listings_details_df = pd.read_csv(self.paths['listings_details'], low_memory=False)
        neighbourhoods_df = pd.read_csv(self.paths['neighbourhoods'], low_memory=False)
        geojson_gdf = gpd.read_file(self.paths['geojson'])
        return calendar_df, listings_df, listings_details_df, neighbourhoods_df, geojson_gdf
