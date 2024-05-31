import math
from typing import NamedTuple

class Coord(NamedTuple):
    """
    Represents a geo coordinate (lat/lon in degrees)
    """
    lat: float
    lon: float

# These formulas have been found "on the internet". One source using the same formula and coefficients: http://www.csgnetwork.com/degreelenllavcalc.html
# They assume the earth to be a sphere which is only approximately true, therefore precision is limited (+-0.5% according to some sources)
# Results have been roughly validated using the Google Maps distance measuring tool.

def m_per_deg_lat(lat: float) -> float:
    """
    Calculate the length of a degree of latitude in meters at a certain latitude.
    """
    lat_rad = math.radians(lat)
    return 111132.92 - 559.82 * math.cos(2 * lat_rad) + 1.175 * math.cos(4 * lat_rad) - 0.0023 * math.cos(6 * lat_rad)

def m_per_deg_lon(lat: float) -> float:
    """
    Calculate the length of a degree of longitude in meters at a certain latitude.
    """
    lat_rad = math.radians(lat)
    return 111412.84 * math.cos(lat_rad) - 93.5 * math.cos(3 * lat_rad) + 0.118 * math.cos(5 * lat_rad)

def distance_m(coord1: Coord, coord2: Coord) -> float:
    """
    Calculates the euclidean distance in meters between coord1 and coord2. 
    Due to the length per degree longitude strongly depending on latitude, 
    the accuracy decreases with increasing absolute latitude and difference in latitude.
    For small differences in latitude accuracy should be fine (e.g. <1° delta lat)
    """
    abs_lat_delta = abs(coord1.lat - coord2.lat) * m_per_deg_lat(coord1.lat)
    abs_lon_delta = abs(coord1.lon - coord2.lon) * m_per_deg_lon(coord1.lat)
    return math.sqrt(pow(abs_lat_delta, 2) + pow(abs_lon_delta, 2))
