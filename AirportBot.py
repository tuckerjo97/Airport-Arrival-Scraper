import pandas as pd
from requests_html import HTMLSession
from datetime import datetime
import time
import random
import re

class AirportBot(object):
    """
    This scraper class contains all the methods needed to scrape data of all incoming flights
    to cities by the day. The scraper uses the public API of FlightAware.com
    """

    def __init__(self, cities):
        """
        Input is a dataframe in the form of [airport name, city name, ICAO code, lat, lon]
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
        for _, name, city, code, lat, lon in self.cities.itertuples():
            time.sleep(random.randint(2, 5))

            try:
                arrivals = self.scrape_arrivals(code)
                if type(arrivals) == int:
                    print("Invalid Code: {}".format(code))
                    continue
                arrivals = arrivals.dropna()

                if not arrivals.empty:
                    arrival_day = arrivals.loc[0, "day"]
                    arrivals = arrivals[arrivals['day'] == arrival_day]

                enroute = self.scrape_enroute(code)
                enroute = enroute.dropna()

                if not arrivals.empty and not enroute.empty:
                    arrival_day = arrivals.loc[0, "day"]
                    enroute = enroute[enroute['day'] == arrival_day]
                elif not enroute.empty:
                    enroute_day = enroute.loc[0, "day"]
                    enroute = enroute[enroute['day'] == enroute_day]

                total = len(arrivals) + len(enroute)
                df_format = pd.DataFrame(
                    {'date': [datetime.now()], 'name': [name], 'city': [city],
                     'ICAO': [code], 'numArrivals': [total], 'lat': [lat], 'lon': [lon]})
                masterDF = masterDF.append(df_format)
                print("scraped {} with ICAO code: {} and arrival count: {}".format(city, code, total))
            except Exception as e:
                print("An error occured with message: " + str(e))
                continue

        return masterDF


    def scrape_arrivals(self, code):
        """
        Scrapes all arivals for a specific airport using the date of the first arrival as a guideline for what day to
        look for. The number of arrivals can change depending on what time of the day the scraper is run
        """

        ### Confirms that the code for url is actually the correct one used on flightaware
        url = ("https://flightaware.com/live/airport/"
               + code
               + "/arrivals?;offset=0"
               + ";order=actualarrivaltime;sort=DESC")
        site = self.session.get(url)
        url_confirm = site.url
        code = self.confirm_code(url_confirm, code)
        if code == "INVALID":
            return 0

        same_date = True
        offset = 0
        masterDF = pd.DataFrame()
        first_try = True
        day = None
        while same_date:
            if not first_try:
                url = ("https://flightaware.com/live/airport/"
                        + code
                        + "/arrivals?;offset="
                        + str(offset)
                        + ";order=actualarrivaltime;sort=DESC")
                site = self.session.get(url)

            table_html = site.html.find('table table')
            airportdata = pd.read_html(table_html[0].html, header=1)[0]
            airportdata = airportdata.loc[:, ['Ident', 'Type', 'Origin', 'Departure', 'Arrival']]

            ### Checks to make sure there are any results
            if airportdata.empty or airportdata.loc[0, 'Ident'] == "Sorry. No matching flights found; try again later.":
                return masterDF

            else:
                airportdata['day'] = airportdata['Arrival'].str.extract(self.days)
                airportdata = airportdata.reset_index()

            airportdata = airportdata.loc[:, ["Arrival", "day"]]
            masterDF = masterDF.append(airportdata)

            if day != None and airportdata.loc[len(airportdata)-1, "day"] != day:
                same_date = False

            if airportdata['day'].iloc[0] != airportdata['day'].iloc[len(airportdata)-1] or len(airportdata) == 1:
                same_date = False
            else:
                offset += 20

            if not first_try:
                time.sleep(random.randint(2, 5))
            first_try = False

            if not airportdata.empty:
                day = airportdata.loc[0, "day"]

        masterDF = masterDF[~masterDF['day'].isna()]
        if masterDF.empty:
            return pd.DataFrame()
        return masterDF.reset_index()


    def scrape_enroute(self, code):
        """
        Scrapes all enroute flights for a specific airport using the date of the first arrival as a guideline for what
        day to look for. Will add the number of enroute to arrivals to get total arrivals for the day
        """

        url = ("https://flightaware.com/live/airport/"
               + code
               + "/enroute?;offset=0"
               + ";order=actualarrivaltime;sort=ASC")
        site = self.session.get(url)
        code = self.confirm_code(site.url, code)

        same_date = True
        offset = 0
        masterDF = pd.DataFrame()
        first_try = True
        day = None
        while same_date:
            if not first_try:
                url = ("https://flightaware.com/live/airport/"
                        + code
                        + "/enroute?;offset="
                        + str(offset)
                        + ";order=actualarrivaltime;sort=ASC")
                site = self.session.get(url)

            table_html = site.html.find('table table')
            airportdata = pd.read_html(table_html[0].html, header=1)[0]
            airportdata = airportdata.loc[:, ['Ident', 'Type', 'Origin', 'ScheduledDeparture Time',
                                              'Departure', 'EstimatedArrival Time']]
            airportdata.columns = ['Ident', 'Type', 'Origin', 'ScheduledDepartureTime',
                                              'Departure', 'EstimatedArrivalTime']

            ### Checks to make sure there are any results
            if airportdata.empty or airportdata.loc[0, 'Ident'] == "Sorry. No matching flights found; try again later.":
                return masterDF
            else:
                airportdata['day'] = airportdata['EstimatedArrivalTime'].str.extract(self.days)
                airportdata = airportdata.reset_index()

            airportdata = airportdata.loc[:, ["EstimatedArrivalTime", "day"]]

            masterDF = masterDF.append(airportdata)

            if day != None and airportdata.loc[len(airportdata)-1, "day"] != day:
                same_date = False

            if airportdata['day'].iloc[0] != airportdata['day'].iloc[len(airportdata) - 1] or len(airportdata) == 1:
                same_date = False
            else:
                offset += 20

            if not first_try:
                time.sleep(random.randint(2, 5))
            first_try = False

            if not airportdata.empty:
                day = airportdata.loc[0, "day"]

        masterDF = masterDF[~masterDF['day'].isna()]
        if masterDF.empty:
            return pd.DataFrame()
        return masterDF.reset_index()

    def confirm_code(self, url, code):
        new_code = re.search("(?<=airport\/)[a-zA-Z]*", url).group(0)
        if new_code == code:
            return code
        else:
            return new_code
