from xml.etree import ElementTree
from typing import NamedTuple
import sys

# Constants for floating-point precision handling
EPSILON = 0.00001
INFINITY = sys.float_info.max
TINY = sys.float_info.min


class Point(NamedTuple):
    x: float
    y: float


class Edge(NamedTuple):
    start: Point
    end: Point


class Polygon(NamedTuple):
    name: str | None
    edges: list[Edge]


def _parse_coordinates(coord_text: str | None) -> list[Point]:
    """Parse a KML coordinate string into a list of Points."""
    if not coord_text:
        return []

    coord_list = []
    for coord in coord_text.strip().split():
        if coord.strip():
            try:
                parts = coord.split(",")
                if len(parts) >= 2:
                    lon = float(parts[0])
                    lat = float(parts[1])
                    coord_list.append(Point(lon, lat))
            except (ValueError, IndexError):
                continue

    return coord_list


def _find_folder_by_name(
    root: ElementTree.Element, folder_name: str, namespace: str
) -> ElementTree.Element | None:
    """
    Recursively search for a folder with the specified name in the KML structure.

    Args:
        root: Current XML element to search from
        folder_name: Name of the folder to find
        namespace: KML namespace string

    Returns:
        The folder element if found, None otherwise
    """
    # Check all folders at current level
    for folder in root.findall(f".//{namespace}Folder"):
        name_elem = folder.find(f"./{namespace}name")
        if name_elem is not None and name_elem.text == folder_name:
            return folder

        # Recursively search in subfolders
        subfolder_result = _find_folder_by_name(folder, folder_name, namespace)
        if subfolder_result is not None:
            return subfolder_result

    return None


def extract_polygons_from_folder(file_path: str, folder_name: str) -> list[Polygon]:
    """
    Read KML file and extract polygons from a specific folder.

    Args:
        file_path: Path to the KML file
        folder_name: Name of the folder containing desired polygons

    Returns:
        DataFrame containing polygon data from the specified folder
    """
    # Parse the KML file
    tree = ElementTree.parse(file_path)
    root = tree.getroot()

    # Extract namespace from the root element
    namespace = root.tag.split("}")[0] + "}"

    # Find the specified folder
    target_folder = _find_folder_by_name(root, folder_name, namespace)
    if target_folder is None:
        print(f"Folder '{folder_name}' not found in the KML file")
        return []

    polygons = []

    # Process all placemarks within the target folder
    for placemark in target_folder.findall(f".//{namespace}Placemark"):

        name_elem = placemark.find(f"{namespace}name")
        polygon_name = name_elem.text if name_elem is not None else None

        # Find Polygon element
        polygon = placemark.find(f".//{namespace}Polygon")
        if polygon is not None:
            outer_boundary = polygon.find(f".//{namespace}outerBoundaryIs")
            if outer_boundary is not None:
                coordinates = outer_boundary.find(f".//{namespace}coordinates")
                coord_text = coordinates.text if coordinates is not None else None

                # Parse coordinates and create edges
                points = _parse_coordinates(coord_text)
                if points:
                    edges = [
                        Edge(points[i], points[(i + 1) % len(points)])
                        for i in range(len(points))
                    ]
                    polygon = Polygon(polygon_name, edges)
                    polygons.append(polygon)

    return polygons


def _ray_intersects_segment(point: Point, edge: Edge) -> bool:
    """
    Determines if a horizontal ray cast from a point intersects with a line segment.
    """
    start, end = edge

    if start.y > end.y:
        start, end = end, start

    if point.y == start.y or point.y == end.y:
        point = Point(point.x, point.y + EPSILON)

    if (point.y > end.y or point.y < start.y) or (point.x > max(start.x, end.x)):
        return False

    if point.x < min(start.x, end.x):
        return True

    if abs(start.x - end.x) > TINY:
        slope_edge = (end.y - start.y) / float(end.x - start.x)
    else:
        slope_edge = INFINITY

    if abs(start.x - point.x) > TINY:
        slope_point = (point.y - start.y) / float(point.x - start.x)
    else:
        slope_point = INFINITY

    return slope_point >= slope_edge


def _is_point_inside_polygon(point: Point, polygon: Polygon) -> bool:
    """Determines if a point lies inside a polygon using the ray casting algorithm."""
    intersection_count = sum(
        _ray_intersects_segment(point, edge) for edge in polygon.edges
    )
    return intersection_count % 2 == 1


def check_point_in_kml_polygon(
    longitude: float, latitude: float, polygon: Polygon
) -> bool:
    """Check if a coordinate pair is inside a KML polygon."""
    point = Point(longitude, latitude)
    return _is_point_inside_polygon(point, polygon)


if __name__ == "__main__":
    file_path = "cheias_em_porto_alegre.kml"
    polygons = extract_polygons_from_folder(
        file_path, "Inundação simulada com nível 500 cm (5.0 m)"
    )
