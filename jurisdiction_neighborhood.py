from dataclasses import dataclass
from typing import List, Dict
from functools import lru_cache
import threading



@dataclass
class ShippingLocation:
    country: str
    regional_jurisdictions: List[str]
    notes: str


# Jurisdiction neighborhoods organized by global region
JURISDICTION_NEIGHBORHOODS: dict[str, ShippingLocation] = {
    # East Asia
    "CN": ShippingLocation(
        country="China",
        regional_jurisdictions=["CN", "HK", "MO", "TW", "KR", "JP", "VN", "MN", "KZ", "KG", "SG", "MY", "TH", "PH"],
        notes="Major manufacturing hub, strong ties with HK/TW/MO",
    ),
    "HK": ShippingLocation(
        country="Hong Kong",
        regional_jurisdictions=["HK", "CN", "MO", "TW", "SG", "VN", "MY", "TH", "PH"],
        notes="Major trading hub, closely tied to mainland China",
    ),
    "MO": ShippingLocation(
        country="Macau",
        regional_jurisdictions=["MO", "HK", "CN", "TW", "PH"],
        notes="Special administrative region with strong CN/HK ties",
    ),
    "KR": ShippingLocation(
        country="South Korea",
        regional_jurisdictions=["KR", "JP", "CN", "TW", "HK", "VN", "SG"],
        notes="Major manufacturing and technology hub",
    ),
    "KP": ShippingLocation(
        country="North Korea",
        regional_jurisdictions=["KP", "CN", "RU", "KR"],
        notes="Limited international trade connections",
    ),
    "JP": ShippingLocation(
        country="Japan",
        regional_jurisdictions=["JP", "KR", "CN", "TW", "HK", "VN", "SG", "PH"],
        notes="Major manufacturing and technology center",
    ),
    "TW": ShippingLocation(
        country="Taiwan",
        regional_jurisdictions=["TW", "CN", "HK", "JP", "PH", "VN", "SG"],
        notes="Major technology manufacturing hub",
    ),
    "MN": ShippingLocation(
        country="Mongolia",
        regional_jurisdictions=["MN", "CN", "RU", "KZ"],
        notes="Landlocked nation with strong ties to China and Russia",
    ),

    # Southeast Asia
    "VN": ShippingLocation(
        country="Vietnam",
        regional_jurisdictions=["VN", "CN", "LA", "KH", "TH", "MY", "SG", "ID", "PH"],
        notes="Growing manufacturing hub",
    ),
    "LA": ShippingLocation(
        country="Laos",
        regional_jurisdictions=["LA", "VN", "KH", "TH", "CN", "MM"],
        notes="Landlocked country with growing trade links",
    ),
    "KH": ShippingLocation(
        country="Cambodia",
        regional_jurisdictions=["KH", "TH", "VN", "LA", "MY", "SG"],
        notes="Emerging manufacturing center",
    ),
    "TH": ShippingLocation(
        country="Thailand",
        regional_jurisdictions=["TH", "MY", "MM", "LA", "KH", "VN", "SG", "ID"],
        notes="Major manufacturing base and logistics hub",
    ),
    "MM": ShippingLocation(
        country="Myanmar",
        regional_jurisdictions=["MM", "TH", "LA", "CN", "BD", "IN"],
        notes="Strategic location between South and Southeast Asia",
    ),
    "PH": ShippingLocation(
        country="Philippines",
        regional_jurisdictions=["PH", "ID", "MY", "VN", "CN", "TW", "JP", "SG"],
        notes="Major logistics hub",
    ),
    "SG": ShippingLocation(
        country="Singapore",
        regional_jurisdictions=["SG", "MY", "ID", "TH", "VN", "PH", "CN", "IN", "AE"],
        notes="Major global trading hub",
    ),
    "MY": ShippingLocation(
        country="Malaysia",
        regional_jurisdictions=["MY", "SG", "ID", "TH", "BN", "PH", "VN"],
        notes="Strategic shipping location",
    ),
    "BN": ShippingLocation(
        country="Brunei",
        regional_jurisdictions=["BN", "MY", "SG", "ID", "PH"],
        notes="Oil and gas trading hub",
    ),
    "ID": ShippingLocation(
        country="Indonesia",
        regional_jurisdictions=["ID", "SG", "MY", "TH", "PH", "TL", "PG"],
        notes="Major archipelagic shipping nation",
    ),
    "TL": ShippingLocation(
        country="East Timor",
        regional_jurisdictions=["TL", "ID", "AU"],
        notes="Emerging economy with strong ties to Indonesia",
    ),

    # South Asia
    "IN": ShippingLocation(
        country="India",
        regional_jurisdictions=["IN", "LK", "BD", "NP", "BT", "MM", "AE", "SG", "MY"],
        notes="Major manufacturing hub with strong global trade",
    ),
    "BD": ShippingLocation(
        country="Bangladesh",
        regional_jurisdictions=["BD", "IN", "MM", "NP", "LK", "TH", "SG"],
        notes="Major textile manufacturing hub",
    ),
    "LK": ShippingLocation(
        country="Sri Lanka",
        regional_jurisdictions=["LK", "IN", "SG", "MY", "AE", "MV"],
        notes="Strategic shipping location",
    ),
    "MV": ShippingLocation(
        country="Maldives",
        regional_jurisdictions=["MV", "LK", "IN", "AE", "SG"],
        notes="Indian Ocean trading point",
    ),
    "NP": ShippingLocation(
        country="Nepal",
        regional_jurisdictions=["NP", "IN", "CN", "BD", "BT"],
        notes="Landlocked, dependent on Indian ports",
    ),
    "BT": ShippingLocation(
        country="Bhutan",
        regional_jurisdictions=["BT", "IN", "NP", "BD", "CN"],
        notes="Landlocked, closely tied to Indian economy",
    ),
    "PK": ShippingLocation(
        country="Pakistan",
        regional_jurisdictions=["PK", "CN", "IN", "AF", "IR", "AE"],
        notes="Strategic location between Middle East and South Asia",
    ),
    "AF": ShippingLocation(
        country="Afghanistan",
        regional_jurisdictions=["AF", "PK", "IR", "TM", "UZ", "TJ", "CN"],
        notes="Landlocked, regional trade hub",
    ),

    # Central Asia
    "KZ": ShippingLocation(
        country="Kazakhstan",
        regional_jurisdictions=["KZ", "RU", "CN", "KG", "UZ", "TM"],
        notes="Major transit country between Asia and Europe",
    ),
    "UZ": ShippingLocation(
        country="Uzbekistan",
        regional_jurisdictions=["UZ", "KZ", "KG", "TJ", "AF", "TM"],
        notes="Growing manufacturing base",
    ),
    "KG": ShippingLocation(
        country="Kyrgyzstan",
        regional_jurisdictions=["KG", "KZ", "CN", "TJ", "UZ"],
        notes="Key location on Belt and Road Initiative",
    ),
    "TJ": ShippingLocation(
        country="Tajikistan",
        regional_jurisdictions=["TJ", "UZ", "KG", "CN", "AF"],
        notes="Emerging transit country",
    ),
    "TM": ShippingLocation(
        country="Turkmenistan",
        regional_jurisdictions=["TM", "UZ", "KZ", "IR", "AF"],
        notes="Energy export hub",
    ),

    # Middle East
    "AE": ShippingLocation(
        country="United Arab Emirates",
        regional_jurisdictions=["AE", "SA", "OM", "BH", "QA", "KW", "IR", "PK", "IN"],
        notes="Major global trading hub, strong ties with Asia",
    ),
    "SA": ShippingLocation(
        country="Saudi Arabia",
        regional_jurisdictions=["SA", "AE", "BH", "KW", "QA", "OM", "YE", "JO", "IQ"],
        notes="Major regional hub",
    ),
    "IR": ShippingLocation(
        country="Iran",
        regional_jurisdictions=["IR", "TR", "IQ", "TM", "AF", "PK", "AE"],
        notes="Strategic location between Middle East and Asia",
    ),
    "IQ": ShippingLocation(
        country="Iraq",
        regional_jurisdictions=["IQ", "TR", "IR", "SY", "JO", "SA", "KW"],
        notes="Regional trade center",
    ),
    "JO": ShippingLocation(
        country="Jordan",
        regional_jurisdictions=["JO", "SA", "IQ", "SY", "IL", "PS", "EG"],
        notes="Strategic Middle East logistics hub",
    ),
    "KW": ShippingLocation(
        country="Kuwait",
        regional_jurisdictions=["KW", "SA", "IQ", "IR", "BH", "QA", "AE"],
        notes="Oil export hub",
    ),
    "BH": ShippingLocation(
        country="Bahrain",
        regional_jurisdictions=["BH", "SA", "QA", "KW", "AE", "OM"],
        notes="Financial and logistics center",
    ),
    "QA": ShippingLocation(
        country="Qatar",
        regional_jurisdictions=["QA", "SA", "BH", "AE", "KW", "OM"],
        notes="Major LNG export hub",
    ),
    "OM": ShippingLocation(
        country="Oman",
        regional_jurisdictions=["OM", "AE", "SA", "YE", "IR"],
        notes="Strategic shipping location",
    ),
    "YE": ShippingLocation(
        country="Yemen",
        regional_jurisdictions=["YE", "SA", "OM", "DJ", "ER"],
        notes="Strategic location near shipping lanes",
    ),
    "IL": ShippingLocation(
        country="Israel",
        regional_jurisdictions=["IL", "EG", "JO", "LB", "CY", "TR", "GR"],
        notes="Technology hub with Mediterranean trade",
    ),
    "PS": ShippingLocation(
        country="Palestine",
        regional_jurisdictions=["PS", "IL", "JO", "EG"],
        notes="Trade dependent on neighboring countries",
    ),
    "LB": ShippingLocation(
        country="Lebanon",
        regional_jurisdictions=["LB", "SY", "IL", "CY", "TR"],
        notes="Mediterranean trading center",
    ),
    "SY": ShippingLocation(
        country="Syria",
        regional_jurisdictions=["SY", "TR", "IQ", "JO", "LB"],
        notes="Regional trade connections",
    ),

    # North Africa
    "EG": ShippingLocation(
        country="Egypt",
        regional_jurisdictions=["EG", "LY", "SD", "IL", "SA", "JO", "GR", "IT"],
        notes="Major Suez Canal shipping hub",
    ),
    "LY": ShippingLocation(
        country="Libya",
        regional_jurisdictions=["LY", "EG", "TN", "DZ", "TD", "SD"],
        notes="North African trade center",
    ),
    "TN": ShippingLocation(
        country="Tunisia",
        regional_jurisdictions=["TN", "DZ", "LY", "IT", "MT"],
        notes="Mediterranean trading hub",
    ),
    "DZ": ShippingLocation(
        country="Algeria",
        regional_jurisdictions=["DZ", "TN", "LY", "MA", "MR", "ML", "NE"],
        notes="Major North African economy",
    ),
    "MA": ShippingLocation(
        country="Morocco",
        regional_jurisdictions=["MA", "DZ", "ES", "PT", "MR"],
        notes="Gateway between Europe and Africa",
    ),
    "SD": ShippingLocation(
        country="Sudan",
        regional_jurisdictions=["SD", "EG", "LY", "TD", "SS", "ET", "ER"],
        notes="Strategic location between North and East Africa",
    ),

    # East Africa
    "ET": ShippingLocation(
        country="Ethiopia",
        regional_jurisdictions=["ET", "SD", "SS", "KE", "SO", "DJ", "ER"],
        notes="Major East African economy",
    ),
    "DJ": ShippingLocation(
        country="Djibouti",
        regional_jurisdictions=["DJ", "ET", "ER", "SO", "YE"],
        notes="Strategic shipping hub",
    ),
    "ER": ShippingLocation(
        country="Eritrea",
        regional_jurisdictions=["ER", "ET", "SD", "DJ"],
        notes="Red Sea shipping access",
    ),
    "SO": ShippingLocation(
        country="Somalia",
        regional_jurisdictions=["SO", "ET", "DJ", "KE"],
        notes="Strategic location near shipping lanes",
    ),
    "KE": ShippingLocation(
        country="Kenya",
        regional_jurisdictions=["KE", "TZ", "UG", "SS", "ET", "SO"],
        notes="East African logistics hub",
    ),
    "UG": ShippingLocation(
        country="Uganda",
        regional_jurisdictions=["UG", "KE", "TZ", "RW", "SS", "CD"],
        notes="East African trade center",
    ),
    "TZ": ShippingLocation(
        country="Tanzania",
        regional_jurisdictions=["TZ", "KE", "UG", "RW", "BI", "CD", "ZM", "MW", "MZ"],
        notes="East African port hub",
    ),
    "RW": ShippingLocation(
        country="Rwanda",
        regional_jurisdictions=["RW", "UG", "TZ", "BI", "CD"],
        notes="Growing East African trade center",
    ),
    "BI": ShippingLocation(
        country="Burundi",
        regional_jurisdictions=["BI", "RW", "TZ", "CD"],
        notes="Emerging trade nation",
    ),

    # West Africa
    "NG": ShippingLocation(
        country="Nigeria",
        regional_jurisdictions=["NG", "BJ", "NE", "CM", "GH", "CI"],
        notes="Largest West African economy",
    ),
    "GH": ShippingLocation(
        country="Ghana",
        regional_jurisdictions=["GH", "CI", "BF", "TG", "NG"],
        notes="Major West African port hub",
    ),
    "CI": ShippingLocation(
        country="Ivory Coast",
        regional_jurisdictions=["CI", "GH", "BF", "ML", "GN", "LR"],
        notes="Major West African trade hub",
    ),
    "SN": ShippingLocation(
        country="Senegal",
        regional_jurisdictions=["SN", "MR", "ML", "GW", "GN", "GM"],
        notes="West African maritime hub",
    ),
    "ML": ShippingLocation(
        country="Mali",
        regional_jurisdictions=["ML", "DZ", "NE", "BF", "CI", "GN", "SN", "MR"],
        notes="Landlocked Sahel trade route",
    ),
    "BF": ShippingLocation(
        country="Burkina Faso",
        regional_jurisdictions=["BF", "ML", "NE", "BJ", "TG", "GH", "CI"],
        notes="Landlocked with regional trade links",
    ),
    "NE": ShippingLocation(
        country="Niger",
        regional_jurisdictions=["NE", "DZ", "ML", "BF", "NG", "TD", "LY"],
        notes="Sahel trade corridor",
    ),
    "BJ": ShippingLocation(
        country="Benin",
        regional_jurisdictions=["BJ", "NG", "NE", "BF", "TG"],
        notes="West African coastal hub",
    ),
    "TG": ShippingLocation(
        country="Togo",
        regional_jurisdictions=["TG", "GH", "BF", "BJ"],
        notes="Coastal trading nation",
    ),
    "LR": ShippingLocation(
        country="Liberia",
        regional_jurisdictions=["LR", "CI", "GN", "SL"],
        notes="Historic maritime nation",
    ),
    "SL": ShippingLocation(
        country="Sierra Leone",
        regional_jurisdictions=["SL", "GN", "LR"],
        notes="Coastal West African nation",
    ),
    "GN": ShippingLocation(
        country="Guinea",
        regional_jurisdictions=["GN", "SN", "ML", "CI", "LR", "SL", "GW"],
        notes="Strategic location in West Africa",
    ),
    "GW": ShippingLocation(
        country="Guinea-Bissau",
        regional_jurisdictions=["GW", "SN", "GN"],
        notes="Small coastal nation",
    ),
    "GM": ShippingLocation(
        country="Gambia",
        regional_jurisdictions=["GM", "SN"],
        notes="Small West African trade point",
    ),

    # Central Africa
    "CM": ShippingLocation(
        country="Cameroon",
        regional_jurisdictions=["CM", "NG", "TD", "CF", "CG", "GA", "GQ"],
        notes="Central African trade hub",
    ),
    "TD": ShippingLocation(
        country="Chad",
        regional_jurisdictions=["TD", "LY", "SD", "CF", "CM", "NG", "NE"],
        notes="Landlocked central African nation",
    ),
    "CF": ShippingLocation(
        country="Central African Republic",
        regional_jurisdictions=["CF", "TD", "SD", "SS", "CD", "CG", "CM"],
        notes="Landlocked with regional connections",
    ),
    "CG": ShippingLocation(
        country="Republic of the Congo",
        regional_jurisdictions=["CG", "CD", "CM", "GA", "AO", "CF"],
        notes="Central African oil exporter",
    ),
    "CD": ShippingLocation(
        country="Democratic Republic of the Congo",
        regional_jurisdictions=["CD", "CG", "CF", "SS", "UG", "RW", "BI", "TZ", "ZM", "AO"],
        notes="Large central African nation",
    ),
    "GA": ShippingLocation(
        country="Gabon",
        regional_jurisdictions=["GA", "CM", "GQ", "CG"],
        notes="Oil-exporting nation",
    ),
    "GQ": ShippingLocation(
        country="Equatorial Guinea",
        regional_jurisdictions=["GQ", "CM", "GA"],
        notes="Oil and gas exporter",
    ),

    # Southern Africa
    "ZA": ShippingLocation(
        country="South Africa",
        regional_jurisdictions=["ZA", "NA", "BW", "ZW", "MZ", "SZ", "LS"],
        notes="Major African economy and logistics hub",
    ),
    "NA": ShippingLocation(
        country="Namibia",
        regional_jurisdictions=["NA", "ZA", "BW", "ZM", "AO"],
        notes="Southern African maritime access",
    ),
    "BW": ShippingLocation(
        country="Botswana",
        regional_jurisdictions=["BW", "ZA", "NA", "ZW"],
        notes="Landlocked southern African nation",
    ),
    "ZW": ShippingLocation(
        country="Zimbabwe",
        regional_jurisdictions=["ZW", "ZA", "BW", "MZ", "ZM"],
        notes="Southern African trade route",
    ),
    "ZM": ShippingLocation(
        country="Zambia",
        regional_jurisdictions=["ZM", "CD", "TZ", "MW", "MZ", "ZW", "BW", "NA", "AO"],
        notes="Copper export hub",
    ),
    "MZ": ShippingLocation(
        country="Mozambique",
        regional_jurisdictions=["MZ", "TZ", "MW", "ZM", "ZW", "ZA", "SZ"],
        notes="Indian Ocean gateway",
    ),
    "AO": ShippingLocation(
        country="Angola",
        regional_jurisdictions=["AO", "CD", "CG", "ZM", "NA"],
        notes="Oil-exporting nation",
    ),
    "MW": ShippingLocation(
        country="Malawi",
        regional_jurisdictions=["MW", "TZ", "MZ", "ZM"],
        notes="Landlocked nation",
    ),
    "LS": ShippingLocation(
        country="Lesotho",
        regional_jurisdictions=["LS", "ZA"],
        notes="Landlocked within South Africa",
    ),
    "SZ": ShippingLocation(
        country="Eswatini",
        regional_jurisdictions=["SZ", "ZA", "MZ"],
        notes="Small landlocked kingdom",
    ),

    # North America
    "US": ShippingLocation(
        country="United States",
        regional_jurisdictions=["US", "CA", "MX", "BM", "BS", "CU", "DO", "JM", "PA"],
        notes="Major global trading hub",
    ),
    "CA": ShippingLocation(
        country="Canada",
        regional_jurisdictions=["CA", "US", "GL", "IS"],
        notes="Major trading partner with US",
    ),
    "MX": ShippingLocation(
        country="Mexico",
        regional_jurisdictions=["MX", "US", "GT", "BZ", "CU"],
        notes="Major manufacturing hub",
    ),

    # Central America
    "GT": ShippingLocation(
        country="Guatemala",
        regional_jurisdictions=["GT", "MX", "BZ", "SV", "HN"],
        notes="Central American trade center",
    ),
    "BZ": ShippingLocation(
        country="Belize",
        regional_jurisdictions=["BZ", "MX", "GT"],
        notes="Caribbean coast access",
    ),
    "SV": ShippingLocation(
        country="El Salvador",
        regional_jurisdictions=["SV", "GT", "HN"],
        notes="Pacific coast trade",
    ),
    "HN": ShippingLocation(
        country="Honduras",
        regional_jurisdictions=["HN", "GT", "SV", "NI"],
        notes="Central American logistics",
    ),
    "NI": ShippingLocation(
        country="Nicaragua",
        regional_jurisdictions=["NI", "HN", "CR"],
        notes="Central American shipping route",
    ),
    "CR": ShippingLocation(
        country="Costa Rica",
        regional_jurisdictions=["CR", "NI", "PA"],
        notes="Central American hub",
    ),
    "PA": ShippingLocation(
        country="Panama",
        regional_jurisdictions=["PA", "CR", "CO"],
        notes="Major global shipping hub",
    ),

    # Caribbean
    "CU": ShippingLocation(
        country="Cuba",
        regional_jurisdictions=["CU", "US", "MX", "BS", "JM", "HT"],
        notes="Caribbean's largest island",
    ),
    "JM": ShippingLocation(
        country="Jamaica",
        regional_jurisdictions=["JM", "CU", "HT", "DO", "TC", "KY"],
        notes="Caribbean logistics center",
    ),
    "HT": ShippingLocation(
        country="Haiti",
        regional_jurisdictions=["HT", "DO", "CU", "JM", "BS"],
        notes="Western Hispaniola",
    ),
    "DO": ShippingLocation(
        country="Dominican Republic",
        regional_jurisdictions=["DO", "HT", "PR", "TC"],
        notes="Eastern Hispaniola",
    ),
    "BS": ShippingLocation(
        country="Bahamas",
        regional_jurisdictions=["BS", "US", "CU", "TC"],
        notes="Atlantic maritime hub",
    ),
    "BB": ShippingLocation(
        country="Barbados",
        regional_jurisdictions=["BB", "VC", "LC", "TT", "GD"],
        notes="Eastern Caribbean hub",
    ),
    "TT": ShippingLocation(
        country="Trinidad and Tobago",
        regional_jurisdictions=["TT", "VE", "GY", "BB", "GD"],
        notes="Southern Caribbean energy hub",
    ),

    # South America
    "CO": ShippingLocation(
        country="Colombia",
        regional_jurisdictions=["CO", "PA", "VE", "BR", "PE", "EC"],
        notes="Major Pacific-Caribbean access",
    ),
    "VE": ShippingLocation(
        country="Venezuela",
        regional_jurisdictions=["VE", "CO", "BR", "GY", "TT"],
        notes="Caribbean coast nation",
    ),
    "GY": ShippingLocation(
        country="Guyana",
        regional_jurisdictions=["GY", "VE", "BR", "SR", "TT"],
        notes="Emerging energy exporter",
    ),
    "SR": ShippingLocation(
        country="Suriname",
        regional_jurisdictions=["SR", "GY", "BR", "GF"],
        notes="Northern South American coast",
    ),
    "BR": ShippingLocation(
        country="Brazil",
        regional_jurisdictions=["BR", "UY", "AR", "PY", "BO", "PE", "CO", "VE", "GY", "SR", "GF"],
        notes="Largest South American economy",
    ),
    "EC": ShippingLocation(
        country="Ecuador",
        regional_jurisdictions=["EC", "CO", "PE"],
        notes="Pacific coast exporter",
    ),
    "PE": ShippingLocation(
        country="Peru",
        regional_jurisdictions=["PE", "EC", "CO", "BR", "BO", "CL"],
        notes="Pacific trade hub",
    ),
    "BO": ShippingLocation(
        country="Bolivia",
        regional_jurisdictions=["BO", "PE", "BR", "PY", "AR", "CL"],
        notes="Landlocked with regional ties",
    ),
    "PY": ShippingLocation(
        country="Paraguay",
        regional_jurisdictions=["PY", "BO", "BR", "AR"],
        notes="Landlocked with river access",
    ),
    "UY": ShippingLocation(
        country="Uruguay",
        regional_jurisdictions=["UY", "BR", "AR"],
        notes="Southern cone trading nation",
    ),
    "AR": ShippingLocation(
        country="Argentina",
        regional_jurisdictions=["AR", "CL", "BO", "PY", "BR", "UY"],
        notes="Major South American economy",
    ),
    "CL": ShippingLocation(
        country="Chile",
        regional_jurisdictions=["CL", "PE", "BO", "AR"],
        notes="Pacific coast trading nation",
    ),

    # Europe (continued from original with additions)
    "GB": ShippingLocation(
        country="United Kingdom",
        regional_jurisdictions=["GB", "IE", "FR", "NL", "BE", "DE", "NO"],
        notes="Major trading nation",
    ),
    "IE": ShippingLocation(
        country="Ireland",
        regional_jurisdictions=["IE", "GB", "FR", "IS"],
        notes="European island nation",
    ),
    "FR": ShippingLocation(
        country="France",
        regional_jurisdictions=["FR", "GB", "BE", "LU", "DE", "CH", "IT", "ES", "MC", "AD"],
        notes="Major European economy",
    ),
    "ES": ShippingLocation(
        country="Spain",
        regional_jurisdictions=["ES", "FR", "PT", "AD", "MA", "DZ"],
        notes="Iberian trading hub",
    ),
    "PT": ShippingLocation(
        country="Portugal",
        regional_jurisdictions=["PT", "ES", "MA"],
        notes="Atlantic maritime nation",
    ),
    "DE": ShippingLocation(
        country="Germany",
        regional_jurisdictions=["DE", "NL", "BE", "LU", "FR", "CH", "AT", "CZ", "PL", "DK"],
        notes="Major European manufacturing hub",
    ),
    "IT": ShippingLocation(
        country="Italy",
        regional_jurisdictions=["IT", "FR", "CH", "AT", "SI", "HR", "ME", "AL", "GR", "MT", "RO"],
        notes="Mediterranean trading hub",
    ),
    "CH": ShippingLocation(
        country="Switzerland",
        regional_jurisdictions=["CH", "DE", "FR", "IT", "AT", "LI"],
        notes="Central European logistics",
    ),
    "AT": ShippingLocation(
        country="Austria",
        regional_jurisdictions=["AT", "DE", "CZ", "SK", "HU", "SI", "IT", "CH", "LI"],
        notes="Central European hub",
    ),
    "PL": ShippingLocation(
        country="Poland",
        regional_jurisdictions=["PL", "DE", "CZ", "SK", "UA", "BY", "LT"],
        notes="Central European logistics hub",
    ),
    "CZ": ShippingLocation(
        country="Czech Republic",
        regional_jurisdictions=["CZ", "DE", "PL", "SK", "AT"],
        notes="Central European manufacturing",
    ),
    "SK": ShippingLocation(
        country="Slovakia",
        regional_jurisdictions=["SK", "CZ", "PL", "UA", "HU", "AT"],
        notes="Central European trade route",
    ),
    "HU": ShippingLocation(
        country="Hungary",
        regional_jurisdictions=["HU", "SK", "UA", "RO", "RS", "HR", "SI", "AT"],
        notes="Central European logistics hub",
    ),
    "RO": ShippingLocation(
        country="Romania",
        regional_jurisdictions=["RO", "HU", "UA", "MD", "BG", "RS"],
        notes="Black Sea access",
    ),
    "BG": ShippingLocation(
        country="Bulgaria",
        regional_jurisdictions=["BG", "RO", "RS", "MK", "GR", "TR"],
        notes="Black Sea trading nation",
    ),
    "RS": ShippingLocation(
        country="Serbia",
        regional_jurisdictions=["RS", "HU", "RO", "BG", "MK", "XK", "ME", "BA", "HR"],
        notes="Southeastern European crossroads",
    ),
    "HR": ShippingLocation(
        country="Croatia",
        regional_jurisdictions=["HR", "SI", "HU", "RS", "BA", "ME", "IT"],
        notes="Adriatic shipping access",
    ),
    "SI": ShippingLocation(
        country="Slovenia",
        regional_jurisdictions=["SI", "IT", "AT", "HU", "HR"],
        notes="Central European transit",
    ),
    "BA": ShippingLocation(
        country="Bosnia and Herzegovina",
        regional_jurisdictions=["BA", "HR", "RS", "ME"],
        notes="Balkan trade route",
    ),
    "ME": ShippingLocation(
        country="Montenegro",
        regional_jurisdictions=["ME", "HR", "BA", "RS", "XK", "AL"],
        notes="Adriatic coast access",
    ),
    "XK": ShippingLocation(
        country="Kosovo",
        regional_jurisdictions=["XK", "RS", "ME", "AL", "MK"],
        notes="Balkan transit point",
    ),
    "AL": ShippingLocation(
        country="Albania",
        regional_jurisdictions=["AL", "ME", "XK", "MK", "GR"],
        notes="Adriatic and Ionian seas access",
    ),
    "MK": ShippingLocation(
        country="North Macedonia",
        regional_jurisdictions=["MK", "BG", "GR", "AL", "XK", "RS"],
        notes="Balkan crossroads",
    ),
    "GR": ShippingLocation(
        country="Greece",
        regional_jurisdictions=["GR", "AL", "MK", "BG", "TR", "IT", "CY"],
        notes="Mediterranean shipping hub",
    ),
    "CY": ShippingLocation(
        country="Cyprus",
        regional_jurisdictions=["CY", "GR", "TR", "IL", "LB", "EG"],
        notes="Mediterranean island hub",
    ),
    "TR": ShippingLocation(
        country="Turkey",
        regional_jurisdictions=["TR", "GR", "BG", "GE", "AM", "IR", "IQ", "SY"],
        notes="Eurasian crossroads",
    ),
    "AM": ShippingLocation(
        country="Armenia",
        regional_jurisdictions=["AM", "GE", "TR", "IR", "AZ"],
        notes="Caucasus trade route",
    ),
    "GE": ShippingLocation(
        country="Georgia",
        regional_jurisdictions=["GE", "RU", "TR", "AM", "AZ"],
        notes="Black Sea gateway",
    ),
    "AZ": ShippingLocation(
        country="Azerbaijan",
        regional_jurisdictions=["AZ", "GE", "RU", "IR", "AM"],
        notes="Caspian Sea energy hub",
    ),
    "BY": ShippingLocation(
        country="Belarus",
        regional_jurisdictions=["BY", "RU", "UA", "PL", "LT", "LV"],
        notes="Eastern European transit",
    ),
    "UA": ShippingLocation(
        country="Ukraine",
        regional_jurisdictions=["UA", "BY", "RU", "MD", "RO", "HU", "SK", "PL"],
        notes="Black Sea access",
    ),
    "MD": ShippingLocation(
        country="Moldova",
        regional_jurisdictions=["MD", "RO", "UA"],
        notes="Eastern European transit",
    ),
    "LT": ShippingLocation(
        country="Lithuania",
        regional_jurisdictions=["LT", "LV", "BY", "PL", "RU"],
        notes="Baltic region hub",
    ),
    "LV": ShippingLocation(
        country="Latvia",
        regional_jurisdictions=["LV", "EE", "LT", "BY", "RU"],
        notes="Baltic shipping access",
    ),
    "EE": ShippingLocation(
        country="Estonia",
        regional_jurisdictions=["EE", "LV", "RU", "FI"],
        notes="Baltic tech hub",
    ),
    "FI": ShippingLocation(
        country="Finland",
        regional_jurisdictions=["FI", "SE", "NO", "RU", "EE"],
        notes="Northern European logistics",
    ),
    "SE": ShippingLocation(
        country="Sweden",
        regional_jurisdictions=["SE", "NO", "FI", "DK"],
        notes="Scandinavian hub",
    ),
    "NO": ShippingLocation(
        country="Norway",
        regional_jurisdictions=["NO", "SE", "FI", "RU", "DK", "IS"],
        notes="North Sea shipping",
    ),
    "DK": ShippingLocation(
        country="Denmark",
        regional_jurisdictions=["DK", "DE", "SE", "NO"],
        notes="Baltic Sea gateway",
    ),
    "IS": ShippingLocation(
        country="Iceland",
        regional_jurisdictions=["IS", "NO", "GB", "IE"],
        notes="North Atlantic hub",
    ),
    
    # Oceania
    "AU": ShippingLocation(
        country="Australia",
        regional_jurisdictions=["AU", "ID", "PG", "NZ", "NC", "SB", "TL"],
        notes="Major Oceanian hub",
    ),
    "NZ": ShippingLocation(
        country="New Zealand",
        regional_jurisdictions=["NZ", "AU", "FJ", "NC"],
        notes="South Pacific hub",
    ),
    "PG": ShippingLocation(
        country="Papua New Guinea",
        regional_jurisdictions=["PG", "ID", "SB", "AU"],
        notes="Pacific island nation",
    ),
    "FJ": ShippingLocation(
        country="Fiji",
        regional_jurisdictions=["FJ", "VU", "NC", "SB", "NZ"],
        notes="South Pacific crossroads",
    ),
    "SB": ShippingLocation(
        country="Solomon Islands",
        regional_jurisdictions=["SB", "PG", "VU", "NC"],
        notes="Pacific trade route",
    ),
    "VU": ShippingLocation(
        country="Vanuatu",
        regional_jurisdictions=["VU", "NC", "SB", "FJ"],
        notes="Pacific shipping registry",
    ),
    "NC": ShippingLocation(
        country="New Caledonia",
        regional_jurisdictions=["NC", "VU", "SB", "AU", "NZ"],
        notes="French Pacific territory",
    ),
}


class JurisdictionCache:
    """Thread-safe singleton cache for jurisdiction lookups."""
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self):
        self.country_to_iso: Dict[str, str] = {}
        self.lowercase_country_to_iso: Dict[str, str] = {}
        self.iso_to_jurisdictions: Dict[str, List[str]] = {}
        self.lowercase_country_to_jurisdictions: Dict[str, List[str]] = {}
        self.initialized = False
    
    @classmethod
    def get_instance(cls) -> 'JurisdictionCache':
        """Get or create the singleton instance."""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = cls()
        return cls._instance
    
    def initialize(self) -> None:
        """Initialize the cache if not already done."""
        if self.initialized:
            return
            
        with self._lock:
            if self.initialized:
                return
                
            for iso_code, location in JURISDICTION_NEIGHBORHOODS.items():
                # Cache ISO lookups
                self.country_to_iso[location.country] = iso_code
                self.lowercase_country_to_iso[location.country.lower()] = iso_code
                
                # Cache jurisdiction lookups
                self.iso_to_jurisdictions[iso_code] = location.regional_jurisdictions
                self.lowercase_country_to_jurisdictions[location.country.lower()] = location.regional_jurisdictions
            
            self.initialized = True


@lru_cache(maxsize=1024)
def get_regional_jurisdictions(shipping_location: str) -> List[str]:
    """
    Returns a list of potential company jurisdictions based on a shipping location.
    Uses LRU cache for repeated lookups.
    
    Args:
        shipping_location (str): ISO code
    
    Returns:
        List[str]: List of related jurisdiction ISO codes
    """
    cache = JurisdictionCache.get_instance()
    cache.initialize()
    return cache.iso_to_jurisdictions.get(shipping_location, [])


@lru_cache(maxsize=1024)
def get_regional_jurisdictions_by_country(country: str) -> List[str]:
    """
    Returns a list of potential company jurisdictions based on a country name.
    Case-insensitive with LRU cache for repeated lookups.
    
    Args:
        country (str): Country name in any case
    
    Returns:
        List[str]: List of related jurisdiction ISO codes
    """
    cache = JurisdictionCache.get_instance()
    cache.initialize()
    return cache.lowercase_country_to_jurisdictions.get(country.lower(), [])


@lru_cache(maxsize=1024)
def get_iso_code_by_country(country: str) -> str:
    """
    Returns the ISO code for a given country name.
    Case-insensitive with LRU cache for repeated lookups.
    
    Args:
        country (str): Country name in any case
    
    Returns:
        str: ISO code if found, empty string if not found
    """
    cache = JurisdictionCache.get_instance()
    cache.initialize()
    return cache.lowercase_country_to_iso.get(country.lower(), "")


# Optional: Preload cache at module import
JurisdictionCache.get_instance().initialize()