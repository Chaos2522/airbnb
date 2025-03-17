from db_connection import DBConnection
from data_extractor import DataExtractor
from data_transformer import DataTransformer
from data_loader import DataLoader


def main():
    # Database connection settings.
    user = "postgres"
    password = "airbnb123d"
    host = "localhost"
    port = "5432"
    dbname = "airbnb_dwh"

    # Initialize the database connection.
    db = DBConnection(user, password, host, port, dbname)
    db.create_database_if_not_exists()
    engine = db.connect()

    # Define file paths for all source files.
    paths = {
        'calendar': 'resources/calendar.csv',
        'listings': 'resources/listings.csv',
        'listings_details': 'resources/listings_details.csv',
        'neighbourhoods': 'resources/neighbourhoods.csv',
        'geojson': 'resources/neighbourhoods.geojson'
    }

    # Extraction: Read all source files.
    extractor = DataExtractor(paths)
    calendar_df, listings_df, listings_details_df, neighbourhoods_df, geojson_gdf = extractor.extract()

    # Transformation: Build dimensions and fact table.
    transformer = DataTransformer()
    dim_listing, dim_location, dim_date, fact_daily_revenue = transformer.transform(
        calendar_df, listings_df, listings_details_df, neighbourhoods_df, geojson_gdf
    )

    # Loading: Write transformed tables into the PostgreSQL database.
    loader = DataLoader(engine)
    loader.load_dimension(dim_listing, "dim_listing", chunk_size=50000)
    loader.load_dimension(dim_location, "dim_location", chunk_size=50000)
    loader.load_dimension(dim_date, "dim_date", chunk_size=50000)
    # Option: Load fact table using the COPY command for performance.
    loader.load_fact_using_copy(fact_daily_revenue, "fact_daily_revenue")

    print("[ETL] Process completed successfully!")


if __name__ == "__main__":
    main()
