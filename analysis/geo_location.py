from xml.etree import ElementTree
from typing import NamedTuple
import sys

import polars as pl

# Constants for floating-point precision handling
EPSILON = 0.00001
INFINITY = sys.float_info.max
TINY = sys.float_info.min


class Point(NamedTuple):
    """A 2D point representation.

    Attributes:
        x: The x-coordinate (longitude in geographic context)
        y: The y-coordinate (latitude in geographic context)
    """

    x: float
    y: float


class Edge(NamedTuple):
    """A line segment between two points.

    Attributes:
        start: The starting point of the edge
        end: The ending point of the edge
    """

    start: Point
    end: Point


class Polygon(NamedTuple):
    """A polygon represented by its edges.

    Attributes:
        edges: List of edges forming the polygon
    """

    edges: list[Edge]


class PolygonWithBBox(NamedTuple):
    """A polygon with its precomputed bounding box.

    Attributes:
        polygon: The original polygon
        min_p: Point representing the minimum coordinates of the bounding box
        max_p: Point representing the maximum coordinates of the bounding box
    """

    polygon: Polygon
    min_p: Point
    max_p: Point


def extract_polygons_from_folder(file_path: str, folder_name: str) -> list[Polygon]:
    """Extracts polygons from a specific folder in a KML file.

    Args:
        file_path: Path to the KML file.
        folder_name: Name of the folder containing desired polygons.

    Returns:
        A list of Polygon objects extracted from the specified folder.
    """
    tree = ElementTree.parse(file_path)
    root = tree.getroot()
    namespace = root.tag.split("}")[0] + "}"

    target_folder = _find_folder_by_name(root, folder_name, namespace)
    if target_folder is None:
        print(f"Folder '{folder_name}' not found in the KML file")
        return []

    polygons = []
    for placemark in target_folder.findall(f".//{namespace}Placemark"):
        polygon = placemark.find(f".//{namespace}Polygon")
        if polygon is not None:
            outer_boundary = polygon.find(f".//{namespace}outerBoundaryIs")
            if outer_boundary is not None:
                coordinates = outer_boundary.find(f".//{namespace}coordinates")
                coord_text = coordinates.text if coordinates is not None else None

                points = _parse_coordinates(coord_text)
                if points:
                    edges = [
                        Edge(points[i], points[(i + 1) % len(points)])
                        for i in range(len(points))
                    ]
                    polygon = Polygon(edges)
                    polygons.append(polygon)

    return polygons


def mark_points_in_polygons(df: pl.DataFrame, polygons: list[Polygon]) -> pl.DataFrame:
    """Marks points in a DataFrame that fall within any of the given polygons.

    Args:
        df: Polars DataFrame containing point coordinates.
        polygons: List of polygons to check against.

    Returns:
        DataFrame with an additional 'flooded' boolean column indicating if each point
        falls within any polygon.
    """
    processed_polygons = _preprocess_polygons(polygons)

    return df.with_columns(
        [
            pl.struct(["lat", "lon", "approximateLat", "approximateLon"])
            .map_elements(
                lambda x: _check_point_in_any_polygon(
                    x["lat"],
                    x["lon"],
                    x["approximateLat"],
                    x["approximateLon"],
                    processed_polygons,
                )
            )
            .alias("flooded")
        ]
    )


def _check_point_in_any_polygon(
    lat: float | None,
    lon: float | None,
    approx_lat: float | None,
    approx_lon: float | None,
    processed_polygons: list[PolygonWithBBox],
) -> bool:
    """Checks if a point falls within any of the processed polygons.

    Args:
        lat: The latitude coordinate.
        lon: The longitude coordinate.
        approx_lat: Approximate latitude if exact is not available.
        approx_lon: Approximate longitude if exact is not available.
        processed_polygons: List of preprocessed polygons with bounding boxes.

    Returns:
        True if the point falls within any polygon, False otherwise.
    """
    final_lat = approx_lat if lat is None else lat
    final_lon = approx_lon if lon is None else lon

    if final_lat is None or final_lon is None:
        return False

    point = Point(final_lon, final_lat)

    for p in processed_polygons:
        if _is_point_in_bbox(point, p.min_p, p.max_p):
            if _is_point_inside_polygon(point, p.polygon):
                return True
    return False


def _find_folder_by_name(
    root: ElementTree.Element, folder_name: str, namespace: str
) -> ElementTree.Element | None:
    """Recursively searches for a folder with the specified name in the KML structure.

    Args:
        root: Current XML element to search from.
        folder_name: Name of the folder to find.
        namespace: KML namespace string.

    Returns:
        The folder element if found, None otherwise.
    """
    for folder in root.findall(f".//{namespace}Folder"):
        name_elem = folder.find(f"./{namespace}name")
        if name_elem is not None and name_elem.text == folder_name:
            return folder

        subfolder_result = _find_folder_by_name(folder, folder_name, namespace)
        if subfolder_result is not None:
            return subfolder_result

    return None


def _parse_coordinates(coord_text: str | None) -> list[Point]:
    """Parses a KML coordinate string into a list of Points.

    Args:
        coord_text: String containing comma-separated coordinate pairs.

    Returns:
        List of Point objects representing the parsed coordinates.
    """
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


def _ray_intersects_segment(point: Point, edge: Edge) -> bool:
    """Determines if a horizontal ray cast from a point intersects with a line segment.

    Args:
        point: The point from which to cast the ray.
        edge: The edge to test for intersection.

    Returns:
        True if the ray intersects the edge, False otherwise.
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
    """Determines if a point lies inside a polygon using the ray casting algorithm.

    Args:
        point: The point to test.
        polygon: The polygon to test against.

    Returns:
        True if the point is inside the polygon, False otherwise.
    """
    intersection_count = sum(
        _ray_intersects_segment(point, edge) for edge in polygon.edges
    )
    return intersection_count % 2 == 1


def _get_bounding_box(polygon: Polygon) -> tuple[Point, Point]:
    """Gets min/max points defining the bounding box of a polygon.

    Args:
        polygon: The polygon to compute the bounding box for.

    Returns:
        A tuple of Points representing the minimum and maximum coordinates of the
        bounding box.
    """
    xs = [edge.start.x for edge in polygon.edges] + [polygon.edges[-1].end.x]
    ys = [edge.start.y for edge in polygon.edges] + [polygon.edges[-1].end.y]
    return Point(min(xs), min(ys)), Point(max(xs), max(ys))


def _is_point_in_bbox(point: Point, min_p: Point, max_p: Point) -> bool:
    """Checks if point lies within bounding box.

    Args:
        point: The point to test.
        min_p: The minimum point of the bounding box.
        max_p: The maximum point of the bounding box.

    Returns:
        True if the point lies within the bounding box, False otherwise.
    """
    return min_p.x <= point.x <= max_p.x and min_p.y <= point.y <= max_p.y


def _preprocess_polygons(polygons: list[Polygon]) -> list[PolygonWithBBox]:
    """Precomputes bounding boxes for all polygons for efficient point-in-polygon
    testing.

    Args:
        polygons: List of polygons to preprocess.

    Returns:
        List of PolygonWithBBox objects containing the original polygons and their
        bounding boxes.
    """
    processed = []
    for polygon in polygons:
        min_p, max_p = _get_bounding_box(polygon)
        processed.append(PolygonWithBBox(polygon, min_p, max_p))
    return processed
