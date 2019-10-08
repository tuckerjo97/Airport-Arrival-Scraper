import pandas as pd
from DataBases.aws_mysql_database import downloading
from DataBases.aws_mysql_database import uploading
from DataBases.s3_transfer import s3_transfer
from AirportBot import AirportBot
import time


def download_zips():
    BUCKET_NAME = "loftyai-credentials"
    CREDENTIAL_NAME = "Lofty_Credentials"

    credentials = s3_transfer(
        bucket_name=BUCKET_NAME,
        file_name=CREDENTIAL_NAME,
        direction='download',
        file_type='json')

    # print(credentials)

    query = 'select zipcode, city, state from geocoded_zips;'
    zipcode_data = downloading(
        query,
        user=credentials['databases']['mysql']['aws']['user'],
        host=credentials['databases']['mysql']['aws']['host'],
        password=credentials['databases']['mysql']['aws']['password'],
        db='zipcode_geocoded_data')
    return zipcode_data

def main():
    """
    Main controller for the scraper. Airport codes downloaded from https://datahub.io/core/airport-codes
    """
    start = time.time()

    ### Download zipcode and city data from AWS and format airport codes for ones that appear in AWS data
    zipcode_data = download_zips()
    airport_data = pd.read_csv("formatted_airport.csv", index_col=0)
    airport_data = airport_data[airport_data['city'].isin(zipcode_data['city'])]


    ### Scraper methods
    scraper = AirportBot(airport_data.iloc[:5])
    final_results = scraper.scrape()

    end = time.time()
    print("total runtime: {}".format(end-start))

    BUCKET_NAME = "loftyai-credentials"
    CREDENTIAL_NAME = "Lofty_Credentials"

    CREDENTIALS = s3_transfer(
        bucket_name=BUCKET_NAME,
        file_name=CREDENTIAL_NAME,
        direction='download',
        file_type='json')

    if not CREDENTIALS:
        print('LOFTY AI credentials not downloaded from S3 bucket properly...')

    uploading(
        final_results,
        host=CREDENTIALS['databases']['mysql']['aws']['host'],
        user=CREDENTIALS['databases']['mysql']['aws']['user'],
        password=CREDENTIALS['databases']['mysql']['aws']['password'],
        db='airport_data',
        table='airport_data')


if __name__ == "__main__":
    main()
