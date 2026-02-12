RARITY_CHOICES = ("common", "uncommon", "rare", "legendary", "exotic")

RARITY_COLORS = {
    "common": 0x95A5A6,
    "uncommon": 0x2ECC71,
    "rare": 0x3498DB,
    "legendary": 0x9B59B6,
    "exotic": 0xF39C12,
}


def normalize_rarity(value: str | None) -> str:
    text = (value or "common").strip().lower()
    if text not in RARITY_CHOICES:
        return "common"
    return text


def rarity_color(value: str | None) -> int:
    return RARITY_COLORS[normalize_rarity(value)]
