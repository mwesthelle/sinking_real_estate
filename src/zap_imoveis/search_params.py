from dataclasses import dataclass


@dataclass
class NeighborhoodSearchParams:
    name: str
    latitude: str
    longitude: str
    alt_name: str | None = None


def get_address_search_params(neighborhood_params: NeighborhoodSearchParams):
    name = neighborhood_params.name
    address_neighborhood = neighborhood_params.alt_name or name
    return {
        "addressCity": "Porto Alegre",
        "addressLocationId": f"BR>Rio Grande do Sul>NULL>Porto Alegre>Barrios>{name}",
        "addressState": "Rio Grande do Sul",
        "addressNeighborhood": address_neighborhood,
        "addressPointLat": neighborhood_params.latitude,
        "addressPointLon": neighborhood_params.longitude,
        "addressType": "neighborhood",
    }
