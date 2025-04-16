from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Hotel(Base):
    __tablename__ = "hotels"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    description = Column(String)
    streetaddress = Column(String)

    country_id = Column(Integer, ForeignKey("Country.id"), index=True)
    state_id = Column(Integer, ForeignKey("State.id"), index=True)
    city_id = Column(Integer, ForeignKey("City.id"), index=True)
    area_id = Column(Integer, ForeignKey("Area.id"), index=True)

    # Relationships
    country = relationship("Country")
    state = relationship("State")
    city = relationship("City")
    area = relationship("Area")


class Country(Base):
    __tablename__ = "Country"

    id = Column(Integer, primary_key=True, index=True)
    country = Column(String, nullable=False)


class State(Base):
    __tablename__ = "State"

    id = Column(Integer, primary_key=True, index=True)
    state = Column(String, nullable=False)


class City(Base):
    __tablename__ = "City"

    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, nullable=False)


class Area(Base):
    __tablename__ = "Area"

    id = Column(Integer, primary_key=True, index=True)
    area = Column(String, nullable=False)  # fixed capitalization to match Python style
