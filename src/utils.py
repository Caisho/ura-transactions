import math


def get_tenure_type(tenure):
    """We want to group the tenures into [60, 99, 999, Freehold, Unknown]
       - Case 1: 60 years lease
            - Only 60 years
       - Case 2: 99 years lease
            - Range 70 to 150 years
       - Case 3: 999 years lease
            - Range 150 to 999 years
       - Case 4: Freehold
            - Range > 999 years
            - FH
       - Default: Unknown
            - NA and others
    Args:
        tenure (str): lease length (commencing from year if applicable)
    """
    tokens = tenure.split(' ')
    tenure_type = 'Unknown'
    try:
        leasehold_years = int(tokens[0])
        if leasehold_years == 60:
            tenure_type = '60 years'
        elif leasehold_years > 60 and leasehold_years <= 110:
            tenure_type = '99 years'
        elif leasehold_years > 110:
            tenure_type = '999 years'
    except ValueError:
        if tokens[0] == 'Freehold':
            tenure_type = 'Freehold'
    finally:
        return tenure_type


def get_coordinates_center(data):
    """Calculate the centre of a list of latitude and logitude coordinates using degrees.

    Args:
        data ([list]): list of list(latitude, logitude)
    """
    x, y, z = 0, 0, 0
    for coord in data:
        lat = coord[1] * math.pi / 180
        long = coord[0] * math.pi / 180

        x += math.cos(lat) * math.cos(long)
        y += math.cos(lat) * math.sin(long)
        z += math.sin(lat)

    x = x / len(data)
    y = y / len(data)
    z = z / len(data)

    center_long = math.atan2(y, x)
    center_sqrt = math.sqrt(x*x + y*y)
    center_lat = math.atan2(z, center_sqrt)

    return [center_lat * 180 / math.pi, center_long * 180 / math.pi]


def get_coordinates_distance(coord1, coord2):
    """Uses haversine formula to calculate the shortest dist between 2 points

    Args:
        coord1 (tuple or list): longitude and latitude coordinates in that order
        coord2 (tuple or list): longitude and latitude coordinates in that order
    """
    R = 6371e3
    long1 = coord1[0]
    long2 = coord2[0]
    lat1 = coord1[1]
    lat2 = coord2[1]
    lat1_rad = lat1 * math.pi / 180
    lat2_rad = lat2 * math.pi / 180
    chg_lat = (lat2 - lat1) * math.pi / 180
    chg_long = (long2 - long1) * math.pi / 180

    a = (math.sin(chg_lat/2) * math.sin(chg_lat/2) +
         math.cos(lat1_rad) * math.cos(lat2_rad) *
         math.sin(chg_long/2) * math.sin(chg_long/2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    dist = R * c
    return dist
