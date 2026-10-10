"""District -> canonical city label. India Post only has districts; metros span several."""
from __future__ import annotations

# Keys are upper-case India Post district names (substring match, first hit wins).
CITY_ALIASES: list[tuple[str, str]] = [
    ("BENGALURU", "Bengaluru"), ("BANGALORE", "Bengaluru"),
    ("MUMBAI", "Mumbai"), ("THANE", "Mumbai"), ("RAIGAD", "Mumbai"),
    ("NEW DELHI", "Delhi"), ("DELHI", "Delhi"), ("GAUTAM BUDDHA", "Delhi"), ("GURGAON", "Delhi"),
    ("GURUGRAM", "Delhi"), ("GHAZIABAD", "Delhi"), ("FARIDABAD", "Delhi"),
    ("CHENNAI", "Chennai"), ("KANCHIPURAM", "Chennai"), ("CHENGALPATTU", "Chennai"),
    ("TIRUVALLUR", "Chennai"),
    ("HYDERABAD", "Hyderabad"), ("RANGA REDDY", "Hyderabad"), ("RANGAREDDY", "Hyderabad"), ("MEDCHAL", "Hyderabad"),
    ("KOLKATA", "Kolkata"), ("24 PARAGANAS", "Kolkata"), ("HOWRAH", "Kolkata"), ("NORTH 24 PARGANAS", "Kolkata"),
    ("SOUTH 24 PARGANAS", "Kolkata"),
    ("PUNE", "Pune"), ("AHMEDABAD", "Ahmedabad"), ("GANDHINAGAR", "Ahmedabad"),
    ("JAIPUR", "Jaipur"), ("LUCKNOW", "Lucknow"), ("INDORE", "Indore"), ("BHOPAL", "Bhopal"),
    ("SURAT", "Surat"), ("KOCHI", "Kochi"), ("ERNAKULAM", "Kochi"), ("COIMBATORE", "Coimbatore"),
    ("VADODARA", "Vadodara"), ("NAGPUR", "Nagpur"), ("KANPUR", "Kanpur"),
    ("DEHRADUN", "Dehradun"), ("MYSURU", "Mysuru"), ("MYSORE", "Mysuru"),
    ("LUDHIANA", "Ludhiana"), ("ALLAHABAD", "Prayagraj"), ("PRAYAGRAJ", "Prayagraj"),
    ("KOLHAPUR", "Kolhapur"), ("RANCHI", "Ranchi"), ("BHOPAL", "Bhopal"),
    ("VISAKHAPATNAM", "Visakhapatnam"), ("PATNA", "Patna"), ("CHANDIGARH", "Chandigarh"),
]


def city_for_district(district: str | None) -> str | None:
    if not district:
        return None
    d = district.strip().upper()
    for key, city in CITY_ALIASES:
        if key in d:
            return city
    return district.strip().title()
