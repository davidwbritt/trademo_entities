from dataclasses import dataclass
from typing import List, TypedDict


@dataclass
class ShippingLocation:
    country: str
    shipping_volume: int
    nearby_jurisdictions: List[str]  # ISO codes
    notes: str


# Ordered by shipping volume
JURISDICTION_NEIGHBORHOODS: dict[str, ShippingLocation] = {
    # East Asia - Highest volume region
    "CN": ShippingLocation(
        country="China",
        shipping_volume=270560,
        nearby_jurisdictions=["HK", "TW", "KR", "JP", "VN", "SG"],
        notes="Major manufacturing hub, strong ties with HK/TW",
    ),
    "HK": ShippingLocation(
        country="Hong Kong",
        shipping_volume=38890,
        nearby_jurisdictions=["CN", "TW", "SG", "MO"],
        notes="Major trading hub, closely tied to mainland China",
    ),
    "KR": ShippingLocation(
        country="South Korea",
        shipping_volume=37723,
        nearby_jurisdictions=["JP", "CN", "TW", "HK"],
        notes="Major manufacturing and technology hub",
    ),
    "JP": ShippingLocation(
        country="Japan",
        shipping_volume=34481,
        nearby_jurisdictions=["KR", "CN", "TW", "HK"],
        notes="Major manufacturing and technology center",
    ),
    "TW": ShippingLocation(
        country="Taiwan",
        shipping_volume=24499,
        nearby_jurisdictions=["CN", "HK", "JP", "PH"],
        notes="Major technology manufacturing hub",
    ),
    # South Asia
    "IN": ShippingLocation(
        country="India",
        shipping_volume=215372,
        nearby_jurisdictions=["LK", "BD", "NP", "BT", "SG", "AE"],
        notes="Major manufacturing hub with strong UAE trading ties",
    ),
    "BD": ShippingLocation(
        country="Bangladesh",
        shipping_volume=9470,
        nearby_jurisdictions=["IN", "MM", "NP", "LK"],
        notes="Major textile manufacturing hub",
    ),
    "LK": ShippingLocation(
        country="Sri Lanka",
        shipping_volume=6701,
        nearby_jurisdictions=["IN", "SG", "AE"],
        notes="Strategic shipping location",
    ),
    "NP": ShippingLocation(
        country="Nepal",
        shipping_volume=14920,
        nearby_jurisdictions=["IN", "BD", "CN"],
        notes="Landlocked, heavily dependent on Indian ports",
    ),
    # Southeast Asia
    "VN": ShippingLocation(
        country="Vietnam",
        shipping_volume=130162,
        nearby_jurisdictions=["CN", "TH", "KH", "SG", "MY"],
        notes="Growing manufacturing hub",
    ),
    "PH": ShippingLocation(
        country="Philippines",
        shipping_volume=49219,
        nearby_jurisdictions=["HK", "TW", "JP", "SG", "MY"],
        notes="Major logistics hub",
    ),
    "SG": ShippingLocation(
        country="Singapore",
        shipping_volume=29379,
        nearby_jurisdictions=["MY", "ID", "TH", "HK", "AE"],
        notes="Major global trading hub",
    ),
    "TH": ShippingLocation(
        country="Thailand",
        shipping_volume=21280,
        nearby_jurisdictions=["MY", "SG", "VN", "KH", "MM"],
        notes="Major manufacturing base",
    ),
    "MY": ShippingLocation(
        country="Malaysia",
        shipping_volume=15752,
        nearby_jurisdictions=["SG", "ID", "TH", "BN"],
        notes="Strategic shipping location",
    ),
    "ID": ShippingLocation(
        country="Indonesia",
        shipping_volume=9474,
        nearby_jurisdictions=["SG", "MY", "TH", "PH"],
        notes="Major archipelagic shipping nation",
    ),
    # North America
    "US": ShippingLocation(
        country="United States",
        shipping_volume=118056,
        nearby_jurisdictions=["CA", "MX", "BM", "BS"],
        notes="Major global trading hub",
    ),
    "CA": ShippingLocation(
        country="Canada",
        shipping_volume=11230,
        nearby_jurisdictions=["US", "MX"],
        notes="Major trading partner with US",
    ),
    "MX": ShippingLocation(
        country="Mexico",
        shipping_volume=46072,
        nearby_jurisdictions=["US", "GT", "BZ", "CA"],
        notes="Major manufacturing hub",
    ),
    # Middle East
    "AE": ShippingLocation(
        country="United Arab Emirates",
        shipping_volume=27441,
        nearby_jurisdictions=["SA", "OM", "BH", "QA", "KW", "SG"],
        notes="Major global trading hub, strong ties with Asia",
    ),
    "SA": ShippingLocation(
        country="Saudi Arabia",
        shipping_volume=7060,
        nearby_jurisdictions=["AE", "BH", "KW", "QA", "OM"],
        notes="Major regional hub",
    ),
    # Europe
    "DE": ShippingLocation(
        country="Germany",
        shipping_volume=36887,
        nearby_jurisdictions=["NL", "BE", "FR", "CH", "AT", "PL", "CZ"],
        notes="Major European manufacturing hub",
    ),
    "IT": ShippingLocation(
        country="Italy",
        shipping_volume=26860,
        nearby_jurisdictions=["CH", "FR", "DE", "AT", "SI", "HR"],
        notes="Major manufacturing center",
    ),
    "GB": ShippingLocation(
        country="United Kingdom",
        shipping_volume=25575,
        nearby_jurisdictions=["IE", "FR", "NL", "BE"],
        notes="Major trading nation",
    ),
    "NL": ShippingLocation(
        country="Netherlands",
        shipping_volume=11209,
        nearby_jurisdictions=["BE", "DE", "FR", "GB", "LU"],
        notes="Major European port hub",
    ),
    "PL": ShippingLocation(
        country="Poland",
        shipping_volume=9575,
        nearby_jurisdictions=["DE", "CZ", "SK", "UA", "BY", "LT"],
        notes="Growing manufacturing hub",
    ),
    # Russia and CIS
    "RU": ShippingLocation(
        country="Russia",
        shipping_volume=74584,
        nearby_jurisdictions=["KZ", "BY", "UA", "GE", "AZ"],
        notes="Major regional hub",
    ),
    "KZ": ShippingLocation(
        country="Kazakhstan",
        shipping_volume=799,
        nearby_jurisdictions=["RU", "KG", "UZ", "CN"],
        notes="Major transit country",
    ),
    "UZ": ShippingLocation(
        country="Uzbekistan",
        shipping_volume=7989,
        nearby_jurisdictions=["KZ", "KG", "TJ", "AF", "TM"],
        notes="Growing manufacturing base",
    ),
}


def get_potential_jurisdictions(shipping_location: str) -> List[str]:
    """
    Returns a list of potential company jurisdictions based on a shipping location.
    """
    if shipping_location in JURISDICTION_NEIGHBORHOODS:
        location_info = JURISDICTION_NEIGHBORHOODS[shipping_location]
        # Always include the shipping location itself as a potential jurisdiction
        return [shipping_location] + location_info.nearby_jurisdictions
    return []


# Example usage
"""
shipping_location = "SG"
potential_jurisdictions = get_potential_jurisdictions(shipping_location)
print(f"Companies shipping through {shipping_location} might be registered in: {potential_jurisdictions}")
"""
