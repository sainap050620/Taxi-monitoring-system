from math import asin, cos, radians, sin, sqrt


def haversine(lat1, lon1, lat2, lon2):
    radius_earth = 6372.8

    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))

    return radius_earth * c
