from dataclasses import dataclass, field
import pandas as pd
import requests
from functools import reduce
from shapely import Polygon, MultiPolygon, Point
from shapely.wkb import loads

from .abstract import Extractor
from database.models import CountryBoudaries


@dataclass
class NASAPowerAPIExtractor(Extractor):
    buffer: int
    end_year: int
    parameters: list[str]

    regional_url: str = field(
        default_factory=lambda: "https://power.larc.nasa.gov/api/temporal/monthly/regional?"
    )
    point_url: str = field(
        default_factory=lambda: "https://power.larc.nasa.gov/api/temporal/monthly/point?"
    )
    community: str = field(default_factory=lambda: "AG")
    start_year: int = field(default_factory=lambda: 1981)
    format: str = field(default_factory=lambda: "JSON")

    def extract(self, country: CountryBoudaries) -> pd.DataFrame:
        polygon = loads(bytes(country.geometry.data))
        points = self.get_points_in_polygon(polygon)

        points_metric = []
        counter = 1
        for point in points[:10]:
            point_url = self.create_url_for_point_endpoint(point)
            response = requests.get(point_url)
            point_df = self._formate_api_reponse(response)
            point_df["point"] = f"point_{counter}"
            point_df["latitude"] = point.x
            point_df["longitude"] = point.y
            point_df["country_iso3_id"] = country.country_iso3_id
            points_metric.append(point_df)
            counter += 1
        if len(points_metric) == 0:
            return pd.DataFrame()
        return pd.concat(points_metric, axis=0)

    def get_points_in_polygon(self, polygon: Polygon | MultiPolygon):
        url = self.create_url_for_regional_endpoint(polygon)
        coordinates = self.call_api_to_get_points(url)
        points = self._convert_coordinates_to_points(coordinates)
        points_in_polygon = [
            point for point in points if self._is_point_in_polygon(point, polygon)
        ]
        return points_in_polygon

    def create_url_for_regional_endpoint(self, polygon: Polygon | MultiPolygon) -> str:
        latitude_min, longitude_min, latitude_max, longitude_max = polygon.bounds
        url = (
            f"{self.regional_url}"
            f"latitude-min={latitude_min}&"
            f"latitude-max={latitude_max}&"
            f"longitude-min={longitude_min}&"
            f"longitude-max={longitude_max}&"
            f"parameters={self.parameters[0]}&"
            f"community={self.community}&"
            f"start={self.start_year}&"
            f"end={self.end_year}&"
            f"format={self.format}"
        )
        return url

    def create_url_for_point_endpoint(self, point: Point) -> str:
        latitude = point.x
        longitude = point.y
        parameters = ",".join(self.parameters)
        url = (
            f"{self.point_url}"
            f"latitude={latitude}&"
            f"longitude={longitude}&"
            f"parameters={parameters}&"
            f"community={self.community}&"
            f"start={self.start_year}&"
            f"end={self.end_year}&"
            f"format={self.format}"
        )
        return url

    def call_api_to_get_points(self, url: str) -> list:
        points = []
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            features = data["features"]

            for feature in features:
                point = feature["geometry"]["coordinates"]
                points.append(tuple(point))
        else:
            return []
        return points

    def _convert_coordinates_to_points(self, coordinates: list) -> list[Point]:
        return [Point(coordinate) for coordinate in coordinates]

    def _is_point_in_polygon(self, point: Point, polygon: Polygon) -> bool:
        return polygon.buffer(self.buffer).contains(point)

    def _formate_api_reponse(self, response: requests.Response) -> pd.DataFrame:
        if response.status_code == 200:
            data = response.json()
            data = data["properties"]["parameter"]
            if len(data) == 0:
                return pd.DataFrame()

            point_values = []
            for metric, values in data.items():
                values_df = pd.DataFrame(list(values.items()), columns=["date", metric])
                point_values.append(values_df)
            df_merged = reduce(
                lambda left, right: pd.merge(left, right, on="date"), point_values
            )
            return df_merged
        return pd.DataFrame()
