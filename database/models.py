from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, String, Float
from geoalchemy2 import Geometry


class Base(DeclarativeBase): ...


class FAOCountryCropYield(Base):
    __tablename__ = "fao_country_crop_yield"

    id = Column(Integer, primary_key=True, index=True)
    country_name = Column(String, index=True)
    country_iso3_id = Column(String, index=True)
    crop_name = Column(String, index=True)
    year = Column(Integer, index=True)
    value_source = Column(String)
    area_harvested = Column(Float)
    yield_kg_per_ha = Column(Float)

    def __repr__(self):
        return f"<FAOCountryCropYield(country={self.country_name}, crop={self.crop_name}, year={self.year})>"


class CountryBoudaries(Base):
    __tablename__ = "natural_earth_country_boundaries"

    id = Column(Integer, primary_key=True, index=True)
    country_name = Column(String, index=True)
    country_iso3_id = Column(String, index=True)
    geometry = Column(Geometry(geometry_type="MULTIPOLYGON", srid=4326))
