from zap_imoveis.listing_fields import LISTING_FIELDS
from zap_imoveis.account_fields import ACCOUNT_FIELDS


def convert_list_to_dict(fields):
    """Convert a list of fields to a dict with None values"""
    return {field: None for field in fields}


def build_search_query(listing_fields=None, account_fields=None):
    """
    Build the complete search query structure.

    Args:
        listing_fields: List of fields to include for listings
        account_fields: List of fields to include for account
    """
    listing_fields = listing_fields or LISTING_FIELDS
    account_fields = account_fields or ACCOUNT_FIELDS

    query_structure = {
        "search": {
            "result": {
                "listings": {
                    "listing": convert_list_to_dict(listing_fields),
                    "account": convert_list_to_dict(account_fields),
                    "medias": [],
                    "accountLink": [],
                    "link": [],
                }
            },
            "totalCount": [],
        },
        "page": [],
        "facets": [],
        "fullUriFragments": [],
    }

    return query_structure


def build_query_fields(fields_dict):
    """Build the final query string from the structure"""
    if isinstance(fields_dict, dict):
        parts = []
        for key, value in fields_dict.items():
            if isinstance(value, dict):
                nested_fields = build_query_fields(value)
                parts.append(f"{key}({nested_fields})")
            elif isinstance(value, list):
                parts.append(f"{key}")
            else:
                parts.append(key)
        return ",".join(parts)
    return ",".join(fields_dict)
