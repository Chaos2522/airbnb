from db_connection import DBConnection
from data_extractor import DataExtractor
from data_transformer import DataTransformer
from data_loader import DataLoader


def main():
    # Database connection settings
    user = "postgres"
    password = "airbnb123d"
    host = "localhost"  # or "db" if running in the same Docker network as the Python container
    port = "5432"
    dbname = "airbnb_dwh"

    # Initialize DB connection and create database if it doesn't exist
    db = DBConnection(user, password, host, port, dbname)
    db.create_database_if_not_exists()  # Create the database if needed
    engine = db.connect()

    # Define file paths
    paths = {
        'calendar': 'resources/calendar.csv',
        'listings': 'resources/listings.csv',
        'listings_details': 'resources/listings_details.csv',
        'neighbourhoods': 'resources/neighbourhoods.csv',
        'geojson': 'resources/neighbourhoods.geojson'
    }

    # Extraction step
    extractor = DataExtractor(paths)
    calendar_df, listings_df, listings_details_df, neighbourhoods_df = extractor.extract()

    # Transformation step
    transformer = DataTransformer()
    dim_listing, dim_location, dim_date, fact_daily_revenue = transformer.transform(
        calendar_df, listings_df, listings_details_df, neighbourhoods_df
    )

    # Loading step
    loader = DataLoader(engine)
    loader.load_dimension(dim_listing, "dim_listing", chunk_size=50000)
    loader.load_dimension(dim_location.drop(columns=["listing_id"]), "dim_location", chunk_size=50000)
    loader.load_dimension(dim_date, "dim_date", chunk_size=50000)

    # Option 1: Use chunked load (to_sql)
    # loader.load_fact(fact_daily_revenue, "fact_daily_revenue", chunk_size=50000)

    # Option 2: Use the COPY command for fact data
    loader.load_fact_using_copy(fact_daily_revenue, "fact_daily_revenue")

    print("[ETL] Process completed successfully!")


if __name__ == "__main__":
    main()
