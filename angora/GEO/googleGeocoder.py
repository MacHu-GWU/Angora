##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    
    
Module description
------------------
    This is a utility tools to do mass address geocoding using GoogleAPI. The idea is to
    automatically store geocoded data in a sqlite database. Schema is:
        address_key: an address string or a coordinate
        geocoded_json_data: json data includes every details.
    
    I use address_key to avoid to do unnecessary API request. And the API manager can
    wisely choose usable API key to use. In addition, you can use check_usable() method
    to check if all API keys are usable. (or you may need to get more or update)
    
    
Keyword
-------
    geocoding, API
    
    
Compatibility
-------------
    Python2: No
    Python3: Yes


Prerequisites
-------------
    angora: https://github.com/MacHu-GWU/Angora
    geopy: https://pypi.python.org/pypi/geopy
    

Import Command
--------------
    from .googleGeocoder import GoogleGeocoder
"""

from __future__ import print_function
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderQuotaExceeded
from angora.SQLITE import *
from angora.DATA import *
import time

metadata = MetaData()
engine = Sqlite3Engine("geocoding.sqlite3", autocommit=False)
datatype = DataType()
results = Table("results", metadata,
    Column("address", datatype.text, primary_key=True),
    Column("json", datatype.pickletype),
    )
metadata.create_all(engine)
ins = results.insert()

class APIManager():
    def __init__(self):
        self.api_keys = [
                    "AIzaSyAuzs8xdbysdYZO1wNV3vVw1AdzbL_Dnpk", # sanhe
                    "AIzaSyBfgV3y5z_od63NdoTSgu9wgEdg5D_sjnk", # rich
                    "AIzaSyDsaepgzV7qoczqTW7P2fMmvigxnzg-ZdE", # meng yan
                    "AIzaSyBqgiVid6V2xPZoADmv7dobIfvbhvGhEZA", # zhang tao
                    "AIzaSyBtbvGbyAwiywSdsk8-okThcN3q515GDZQ", # jack
                    "AIzaSyC5XmaneaaRYLr4H0x7HMRoFPgjW9xcu2w", # fenhan
                    "AIzaSyDgM5xmKIjS_nooN_TBRLxrFDypVyON9bU", # Amina
                    "AIzaSyCl95-wDqhxM1CtUzXjvirsAXCU_c1ihu8", # Ryan    
                    ]
        self.keychain = self.api_keys[::-1] # create a copy of reversed api_keys
        
    def take_one(self):
        """take one API key
        """
        try:
            return self.keychain[-1]
        except:
            raise Exception("There's no usable api_keys. Get more quota or more API keys.")
        
    def remove_one(self):
        """remove one API key running out of quota
        """
        self.keychain.pop()
        
class GoogleGeocoder():
    def __init__(self):
        self.api = APIManager()
        self.sleeptime = 0
        
    def check_usable(self):
        """exam if all API keys are usable. it cost 1 quota for each API keys to perform this check.
        """
        bad_keys = list()
        for key in self.api.keychain:
            try:
                geocoder = GoogleV3(key)
                location = geocoder.geocode("1600 Pennsylvania Ave NW, Washington, DC 20500")
                if location.raw["formatted_address"] != "1600 Pennsylvania Avenue Northwest, Washington, DC 20500, USA":
                    raise Exception("API key = '%s' not working." % key)
            except:
                bad_keys.append(key)
        if len(bad_keys) == 0:
            print("All API keys are usable")
        else:
            print("%s are not usable" % bad_keys)

    def set_sleeptime(self, sleeptime):
        """set the sleeping interval time between each API requests
        """
        self.sleeptime = sleeptime
        
    def geocode(self, address):
        """return geocoded json data by address string
        """
        key = self.api.take_one()
        geocoder = GoogleV3(key)
        try:
            location = geocoder.geocode(address)
            return location.raw
        except GeocoderQuotaExceeded: # reach the maximum quota
            self.api.remove_one()
            return self.geocode(address) # try again with new key
        except: # other error
            return None

    def batch_geocode(self, list_of_address):
        """a stable batch geocode method to automatically handle API usage, data storage,
        avoid repeating.
        """
        # put the list of query to database as primary key
        engine.insert_many_records(ins, [(address, None) for address in list_of_address])
        engine.commit()
        
        for address in list_of_address:
            # only make api requests when it is not been geocoded before
            if not list(engine.select(Select([results.json]).\
                                      where(results.address == address)))[0][0]:
                time.sleep(self.sleeptime)
                print("trying to geocode '%s'..." % address)
                raw = self.geocode(address)
                if raw: # if successfully geocoded, update
                    engine.update(results.update().\
                                  values(json = raw).\
                                  where(results.address == address))
                    engine.commit()
                    print("\tSuccess!")
                else:
                    print("\tFailed!")
            else:
                print("'%s' is already successfully geocoded." % address)
                
    def reverse(self, lat, lng):
        """do reverse lookup by latitude and longitude, return json data
        """
        key = self.api.take_one()
        geocoder = GoogleV3(key)
        try:
            locations = geocoder.reverse((lat, lng))
            return locations[0].raw # use first matching by default
        except GeocoderQuotaExceeded: # reach the maximum quota
            self.api.remove_one()
            return self.reverse((lat, lng)) # try again with new key
        except: # other error
            return None

    def batch_reverse(self, list_of_coordinates):
        """a stable batch reverse geocode method to automatically handle API usage, data storage,
        avoid repeating.
        """
        # put the list of query to database as primary key
        engine.insert_many_records(ins, 
            [(str(hash(coordinate)), None) for coordinate in list_of_coordinates])
        engine.commit()
        
        for coordinate in list_of_coordinates:
            hashkey = str(hash(coordinate))
            # only make api requests when it is not been geocoded before
            if not list(engine.select(Select([results.json]).\
                                      where(results.address == hashkey)))[0][0]:
                time.sleep(self.sleeptime)
                print("trying to geocode '%s'..." % coordinate)
                raw = self.reverse(*coordinate)
                if raw: # if successfully geocoded, update
                    engine.update(results.update().\
                                  values(json = raw).\
                                  where(results.address == hashkey))
                    engine.commit()
                    print("\tSuccess!")
                else:
                    print("\tFailed!")
            else:
                print("(%s, %s) is already successfully geocoded." % coordinate)
                    
if __name__ == "__main__":
    from pprint import pprint as ppt
    """usage example
    """
    list_of_address = [
        "675 15th St NW Washington, DC 20005",
        "2317 Morgan Ln Dunn Loring, VA 22027",
        "1201 Rockville Pike Rockville, MD 20852",
        ]
    list_of_coordinates = [
        (39.085801, -77.084513),
        (38.872719, -77.306417),
        (38.902027, -77.053536),
        ]
    
    google = GoogleGeocoder()
    google.batch_geocode(list_of_address)
    google.batch_reverse(list_of_coordinates)
    engine.prt_all(results)

    # engine.check_usable()
    