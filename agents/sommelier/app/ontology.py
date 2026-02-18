from __future__ import annotations

ALLERGEN_FAMILIES: dict[str, list[str]] = {
    "allium": ["onion", "shallot", "garlic", "leek", "chive", "scallion", "spring onion"],
    "nightshade": ["tomato", "potato", "bell pepper", "eggplant", "chili", "paprika", "cayenne"],
    "tree_nut": ["almond", "walnut", "pecan", "cashew", "pistachio", "macadamia", "hazelnut", "pine nut"],
    "peanut": ["peanut", "groundnut"],
    "dairy": ["milk", "cream", "butter", "cheese", "yogurt", "whey", "casein", "ghee", "parmesan", "mozzarella", "ricotta", "mascarpone", "brie", "gruyere"],
    "gluten": ["wheat", "barley", "rye", "spelt", "flour", "breadcrumb", "pasta", "couscous", "semolina"],
    "shellfish": ["shrimp", "crab", "lobster", "crawfish", "prawn", "langoustine", "scallop", "mussel", "clam", "oyster"],
    "fish": ["salmon", "tuna", "cod", "halibut", "anchovy", "sardine", "branzino", "sea bass", "swordfish", "trout", "snapper", "mahi"],
    "egg": ["egg", "egg yolk", "egg white", "meringue", "aioli", "mayonnaise"],
    "soy": ["soy", "soy sauce", "tofu", "edamame", "miso", "tempeh"],
    "sesame": ["sesame", "sesame oil", "tahini"],
    "sulfite": ["wine", "dried fruit", "vinegar"],
    "celery": ["celery", "celeriac"],
    "mustard": ["mustard", "mustard seed", "dijon"],
    "cilantro": ["cilantro", "coriander"],
}

FLAVOR_PROFILES: dict[str, list[str]] = {
    "rich_savory": ["umami", "earthy", "smoky", "meaty", "roasted"],
    "bright_acidic": ["citrus", "tart", "tangy", "vinaigrette", "pickled"],
    "creamy_mild": ["butter", "cream", "mild", "delicate", "smooth"],
    "spicy_bold": ["chili", "pepper", "ginger", "wasabi", "horseradish"],
    "sweet_aromatic": ["honey", "caramel", "vanilla", "cinnamon", "truffle"],
    "herbaceous": ["basil", "thyme", "rosemary", "sage", "dill", "tarragon", "mint"],
    "briny_oceanic": ["seaweed", "oyster", "clam", "anchovy", "caper", "olive"],
}

WINE_PAIRING_MAP: dict[str, list[dict[str, str]]] = {
    "rich_savory": [
        {"wine": "Barolo", "type": "red", "reason": "Bold tannins complement umami and roasted flavors"},
        {"wine": "Cabernet Sauvignon", "type": "red", "reason": "Full body matches rich, savory dishes"},
        {"wine": "Brunello di Montalcino", "type": "red", "reason": "Complex structure pairs with earthy depth"},
    ],
    "bright_acidic": [
        {"wine": "Sancerre", "type": "white", "reason": "Crisp acidity mirrors bright, tangy flavors"},
        {"wine": "Chablis", "type": "white", "reason": "Mineral backbone cuts through citrus dishes"},
        {"wine": "Vermentino", "type": "white", "reason": "Light and zesty, ideal for vinaigrette-dressed plates"},
    ],
    "creamy_mild": [
        {"wine": "Chardonnay (Burgundy)", "type": "white", "reason": "Buttery notes echo cream-based preparations"},
        {"wine": "Viognier", "type": "white", "reason": "Floral aromatics lift delicate dishes"},
        {"wine": "Champagne", "type": "sparkling", "reason": "Effervescence cleanses the palate between bites"},
    ],
    "spicy_bold": [
        {"wine": "Gewurztraminer", "type": "white", "reason": "Off-dry sweetness tames heat"},
        {"wine": "Riesling", "type": "white", "reason": "Acidity and residual sugar balance spice"},
        {"wine": "Zinfandel", "type": "red", "reason": "Jammy fruit stands up to bold flavors"},
    ],
    "sweet_aromatic": [
        {"wine": "Sauternes", "type": "dessert", "reason": "Honeyed sweetness harmonizes with caramel and truffle"},
        {"wine": "Moscato d'Asti", "type": "sparkling", "reason": "Light fizz and sweetness complement aromatic dishes"},
    ],
    "herbaceous": [
        {"wine": "Sauvignon Blanc", "type": "white", "reason": "Herbal notes in the wine echo fresh herbs on the plate"},
        {"wine": "Gruner Veltliner", "type": "white", "reason": "Peppery and green, a natural match for herb-forward dishes"},
    ],
    "briny_oceanic": [
        {"wine": "Muscadet", "type": "white", "reason": "Lean minerality complements ocean flavors"},
        {"wine": "Albarino", "type": "white", "reason": "Saline finish mirrors briny seafood"},
        {"wine": "Champagne", "type": "sparkling", "reason": "Classic pairing with oysters and shellfish"},
    ],
}


def get_allergen_family(ingredient: str) -> str | None:
    ingredient_lower = ingredient.lower().strip()
    for family, members in ALLERGEN_FAMILIES.items():
        if ingredient_lower in members:
            return family
    return None


def expand_allergen_to_family(allergen: str) -> list[str]:
    family = get_allergen_family(allergen)
    if family:
        return ALLERGEN_FAMILIES[family]
    return [allergen.lower().strip()]


def get_flavor_category(tags: list[str]) -> str | None:
    tag_set = {t.lower() for t in tags}
    best_match: str | None = None
    best_count = 0
    for category, keywords in FLAVOR_PROFILES.items():
        overlap = len(tag_set & set(keywords))
        if overlap > best_count:
            best_count = overlap
            best_match = category
    return best_match


def get_wine_pairings(flavor_category: str) -> list[dict[str, str]]:
    return WINE_PAIRING_MAP.get(flavor_category, [])
