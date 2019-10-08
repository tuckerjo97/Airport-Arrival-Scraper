import pandas as pd
from requests_html import HTMLSession
from datetime import datetime
import time
import random


class AirportBot(object):
    """
    This scraper class contains all the methods needed to scrape data of all incoming flights
    to cities by the day. The scraper uses the public API of FlightAware.com
    """

    def __init__(self, cities):
        """
        Input is a dataframe in the form of [ICAO airport code, city name]
        """
        self.cities = cities
        self.days = '(Mon|Tue|Wed|Thu|Fri|Sat|Sun)'
        self.session = HTMLSession()

    def scrape(self):
        """
        Main driver for scraper. If there are no arrivals on that day, then the total number of arrivals + enroute will be
        zero as it won't be possible to determine if the enroute is on another day
        """
        masterDF = pd.DataFrame()
        for _, code, city in self.cities.itertuples():
            url = ("https://flightaware.com/live/airport/" + code)
            try:
                site = self.session.get(url)
                arrivals = site.html.find('[data-type = arrivals]')
                enroute = site.html.find('[data-type = enroute]')

                arrivals_df = pd.read_html(arrivals[0].html, header=1)
                arrivals_df = arrivals_df[0][["Ident", "Type", "From", "Depart", "Arrive"]].dropna()

                enroute_df = pd.read_html(enroute[0].html, header=1)
                enroute_df = enroute_df[0][["Ident", "Type", "From", "Depart", "Arrive"]].dropna()

                enroute_count = 0
                if not arrivals_df.empty:
                    arrivals_df = self.scrape_arrivals(code)
                if not enroute_df.empty:
                    enroute_df = self.scrape_enroute(code)

            except Exception as e:
                print("an error occured with message: " + str(e))
                continue

            arrivals_count = len(arrivals_df)

            if not arrivals_df.empty and not enroute_df.empty:
                if arrivals_df['day'].iloc[0] == enroute_df['day'].iloc[0]:
                    enroute_count = len(enroute_df)
            elif arrivals_df.empty and not enroute_df.empty:
                enroute_count = len(enroute_df)



            total = arrivals_count + enroute_count
            df_format = pd.DataFrame({'date': [datetime.now()], 'city': [city], 'ICAO': [code], 'numArrivals': [total]})
            masterDF = masterDF.append(df_format)
            print("scraped {} with ICAO code: {} and arrival count: {}".format(city, code, total))

            time.sleep(random.randint(2, 5))

        return masterDF

    def scrape_arrivals(self, code):
        """
        Scrapes all arivals for a specific airport using the date of the first arrival as a guideline for what day to
        look for. The number of arrivals can change depending on what time of the day the scraper is run
        """
        same_date = True
        offset = 0
        masterDF = pd.DataFrame()
        day = None
        first_try = True
        while same_date:
            url = ("https://flightaware.com/live/airport/"
                    + code
                    + "/arrivals?;offset="
                    + str(offset)
                    + ";order=actualarrivaltime;sort=DESC")
            site = self.session.get(url)
            table_html = site.html.find('table table')
            airportdata = pd.read_html(table_html[0].html, header=1)[0]
            airportdata = airportdata.loc[:, ['Arrival']]

            airportdata['day'] = airportdata['Arrival'].str.extract(self.days)
            airportdata = airportdata[~airportdata['day'].isna()]
            airportdata = airportdata.reset_index()

            masterDF = masterDF.append(airportdata)

            if airportdata.empty:
                same_date = False
            elif day and airportdata['day'].iloc[len(airportdata) - 1] != day:
                same_date = False
            elif airportdata['day'].iloc[0] != airportdata['day'].iloc[len(airportdata)-1] or len(airportdata) == 1:
                same_date = False
            else:
                offset += 20
            if same_date:
                day = airportdata['day'].iloc[0]

            if not first_try:
                time.sleep(random.randint(2, 5))
            first_try = False

        masterDF = masterDF[~masterDF['day'].isna()]
        if not masterDF.empty:
            day = masterDF['day'].iloc[0]
            masterDF = masterDF[masterDF['day'] == day]

        return masterDF.reset_index()


    def scrape_enroute(self, code):
        """
        Scrapes all enroute flights for a specific airport using the date of the first arrival as a guideline for what
        day to look for. Will add the number of enroute to arrivals to get total arrivals for the day
        """
        same_date = True
        offset = 0
        masterDF = pd.DataFrame()
        day = None
        first_try = True
        while same_date:
            url = ("https://flightaware.com/live/airport/"
                   + code
                   +"/enroute?;offset="
                   + str(offset)
                   +";order=estimatedarrivaltime;sort=ASC")
            site = self.session.get(url)
            table_html = site.html.find('table table')
            airportdata = pd.read_html(table_html[0].html, header=1)[0]
            airportdata = airportdata.loc[:, ['EstimatedArrival Time']]
            airportdata.columns = ['EstimatedArrivalTime']

            airportdata['day'] = airportdata['EstimatedArrivalTime'].str.extract(self.days)
            airportdata = airportdata[~airportdata['day'].isna()]
            airportdata = airportdata.reset_index()

            masterDF = masterDF.append(airportdata)

            if airportdata.empty:
                same_date = False
            elif day and airportdata['day'].iloc[len(airportdata) - 1] != day:
                same_date = False
            elif airportdata['day'].iloc[0] != airportdata['day'].iloc[len(airportdata) - 1] or len(airportdata) == 1:
                same_date = False
            else:
                offset += 20
            if same_date:
                day = airportdata['day'].iloc[0]

            if not first_try:
                time.sleep(random.randint(2, 5))
            first_try = False


        masterDF = masterDF[~masterDF['day'].isna()]
        if not masterDF.empty:
            day = masterDF['day'].iloc[0]
            masterDF = masterDF[masterDF['day'] == day]

        return masterDF.reset_index()
