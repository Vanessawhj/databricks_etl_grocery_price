import re
from typing import List, Optional, Tuple

SIZE_RE = re.compile(r'(\d+(?:\.\d+)?)\s*(kg|g|l|ml)', re.I)
PACK_RE = re.compile(r'(\d+)\s*(pack|pk|pkt|each)', re.I)


def normalize_name(name: str) -> str:
    name = re.sub(r"<br\s*/?>", " ", name, flags=re.I)  # strip HTML <br>
    name = re.sub(r"\.{2,}", " ", name)        # turn "..." into a space
    name = name.lower()
    name = re.sub(r'[®™]', '', name)                  # remove symbols
    name = re.sub(r'\s+', ' ', name)                  # collapse whitespace
    name = re.sub(r':+\.?:*', ' ', name)   # replace any mix of ':' or '.' with space
    name = re.sub(r'\s+', ' ', name).strip()  # collapse multiple spaces

    return name

def parse_pack_size_and_pack(name: str):

    m = SIZE_RE.search(name)
    n = PACK_RE.search(name)

    if m:
        qty = float(m.group(1))
        unit = m.group(2).lower()
        return qty, unit
    elif n:
        qty = float(n.group(1))
        unit = n.group(2).lower()
        return qty, unit
    
    else:
        return None, None
    

def parse_woolies_unit_price(text: str) -> Optional[Tuple[float, float, str]]:
    """Parse Woolworths unit price strings like '$1.50 / 100G' into numeric parts."""

    UNIT_PRICE_PATTERN = re.compile(
        r"""
        ^\s*\$?(?P<unit_price>\d+(?:\.\d+)?)   # amount (e.g. 1.35, 50.15)
        \s*/\s*
        (?P<unit_qty>\d+(?:\.\d+)?)?           # optional quantity before the unit (100, 1)
        (?P<unit_uom>[A-Za-z]+)                # unit letters (ML, G, EA, KG, L)
        \s*$
        """,
        re.VERBOSE,
    )

    if not text:
        return None
    match = UNIT_PRICE_PATTERN.search(text.strip())
    if not match:
        return None
    price = float(match.group("unit_price"))
    qty = float(match.group("unit_qty") or 1)
    unit = match.group("unit_uom").lower()
    return price, qty, unit


    