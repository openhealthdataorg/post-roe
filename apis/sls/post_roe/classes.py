from dataclasses import dataclass

@dataclass
class Origin:
    zip3: str
    lat_lon: list

@dataclass
class Destination:
    zip3: str
    lat_lon: list