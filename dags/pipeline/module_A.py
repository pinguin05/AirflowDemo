import os
import gpxpy
import pandas as pd
import numpy as np
from geopy.exc import  GeocoderUnavailable
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

from storage import save_df_to_postgres, load_table_from_postgres

GPX_PATH = "/data/input"

def parse_gpx(gpx_string, track_id):
    """
    Функция для парсинга gpx треков с помощью gpxpy
    """
    try:
        gpx = gpxpy.parse(gpx_string)
    except Exception as e:
        print(e)
        return None
    points = []
    for track in gpx.tracks:
        for seg in track.segments:
            for point in seg.points:
                points.append({
                    "track_id": track_id,
                    "name": track.name,
                    "time": point.time,
                    "latitude": point.latitude,
                    "longitude": point.longitude,
                    "elevation": point.elevation
                })
    return points

def load_gpx():
    """
    Загрузка маршрутов в базу данных
    """
    for i, file in enumerate(os.listdir(GPX_PATH)):
        track = parse_gpx(open(f'{GPX_PATH}/{file}'), i)
        if track:
            if i == 0:
                data = pd.DataFrame(track)
            else:
                data = pd.concat([data, pd.DataFrame(track)], axis=0, ignore_index=True)

    save_df_to_postgres(data, 'tracks_raw')

def length(parsed_gpx):
    """
    расчёт расстояний между соседними точками используя geopy
    """
    length_3d = []
    for track in parsed_gpx.groupby('track_id'):
        track = track[1]
        for idx in track.index[:-1]:
            p1 = (track['latitude'][idx], track['longitude'][idx])
            p2 = (track['latitude'][idx + 1], track['longitude'][idx + 1])
            length_2d = geodesic(p1, p2).meters
            if pd.notna(track['elevation'][idx]):
                length_track = np.sqrt((length_2d ** 2) + (track['elevation'][idx] - track['elevation'][idx + 1]) ** 2)
            else:
                length_track = length_2d
            length_3d.append(length_track)
        length_3d.append(None)
    return length_3d

def around_type(df):
    """
    Определение типа местности на основе информации о количестве обьектов вокруг точки
    """
    if pd.notna(df[['water', 'forest', 'buildings']]).sum() and df[['water', 'forest', 'buildings' ]].sum():
        if df["buildings"] > 400:
            return 'city'
        elif df["forest"] > 5:
            return "forest"
        elif df["water"] > 0:
            return "next to the water"
    return None

def step_frequency(parsed_gpx):
    """
    Функция для подсчёта частоты шагов между двумя точками
    """
    steps = []
    for track in parsed_gpx.groupby('track_id'):
        track = track[1]
        if pd.notna(track['time']).sum():
            for idx in track.index[:-1]:
                delta = (parsed_gpx["time"][idx + 1] - parsed_gpx["time"][idx]).total_seconds()
                if delta > 0.5:
                    steps.append((parsed_gpx["lenth3d"][idx] / 0.75) / delta)
                else:
                    steps.append(None)
            steps.append(None)
        else:
            for idx in track.index:
                steps.append(None)
    return steps

def get_season(date):
    """
    Фукнция для класификации сезона по дате
    """
    month = date.month
    if month in [12, 1, 2]:
        return 'Зима'
    elif month in [3, 4, 5]:
        return 'Весна'
    elif month in [6, 7, 8]:
        return 'Лето'
    else:
        return 'Осень'
    
def parse_country(parsed_gpx):
    """
    Функция для парсинга региона маршрута
    """
    locations = []
    for track in parsed_gpx.groupby('track_id'):
        track = track[1]
        try:
            geo_loc = Nominatim(user_agent="GetLoc")
            loc_name = geo_loc.reverse([track["latitude"].mean(), track["longitude"].mean()])
            locations.append(loc_name.address)
        except GeocoderUnavailable:
            locations.append('Unknown')
    return locations

def parse_simple_features():
    tracks = load_table_from_postgres('tracks_raw')

    # определение сезона
    tracks['season'] = tracks['time'].apply(get_season)
    # рассчёт расстояния между точками
    tracks["lenth3d"] = length(tracks)
    # рассчёт частоты шагов между точками
    tracks["step_frequency"] = step_frequency(tracks)
    # определение часа
    tracks['hour'] = tracks['time'].apply(lambda x: x.hour)
    # парсинг региона
    regions = parse_country(tracks)
    tracks["region"] = tracks["track_id"].map(lambda x: regions[x])

    save_df_to_postgres(tracks, 'tracks_raw')