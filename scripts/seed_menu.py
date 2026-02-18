#!/usr/bin/env python3
"""Seed the restaurant menu with structured ingredient and allergen data."""
from __future__ import annotations

import json
import sys
import time

import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
MENU_KEY_PREFIX = "aura:menu:"

MENU_ITEMS = [
    {
        "id": "branzino-grilled",
        "name": "Grilled Branzino",
        "category": "entree",
        "description": "Mediterranean sea bass with lemon, capers, and herb butter",
        "price": 42.00,
        "ingredients": ["branzino", "lemon", "caper", "butter", "thyme", "olive oil", "sea salt"],
        "allergens": ["fish", "dairy"],
        "allergen_family": ["fish", "dairy"],
        "flavor_profile": ["briny", "citrus", "butter", "delicate", "herbaceous"],
    },
    {
        "id": "ribeye-dry-aged",
        "name": "Dry-Aged Ribeye",
        "category": "entree",
        "description": "45-day aged prime ribeye with bone marrow jus and roasted shallots",
        "price": 68.00,
        "ingredients": ["ribeye", "bone marrow", "shallot", "garlic", "rosemary", "black pepper", "sea salt", "butter"],
        "allergens": ["dairy", "allium"],
        "allergen_family": ["dairy", "allium"],
        "flavor_profile": ["umami", "smoky", "roasted", "earthy", "meaty"],
    },
    {
        "id": "risotto-truffle",
        "name": "Black Truffle Risotto",
        "category": "entree",
        "description": "Arborio rice with black truffle, parmesan, and mascarpone",
        "price": 38.00,
        "ingredients": ["arborio rice", "black truffle", "parmesan", "mascarpone", "onion", "white wine", "butter", "vegetable stock"],
        "allergens": ["dairy", "allium", "sulfite"],
        "allergen_family": ["dairy", "allium", "sulfite"],
        "flavor_profile": ["truffle", "cream", "umami", "earthy", "smooth"],
    },
    {
        "id": "salmon-miso",
        "name": "Miso-Glazed Salmon",
        "category": "entree",
        "description": "Wild salmon with white miso glaze, bok choy, and sesame",
        "price": 36.00,
        "ingredients": ["salmon", "white miso", "bok choy", "sesame oil", "ginger", "soy sauce", "rice vinegar", "scallion"],
        "allergens": ["fish", "soy", "sesame", "allium"],
        "allergen_family": ["fish", "soy", "sesame", "allium"],
        "flavor_profile": ["umami", "smoky", "ginger", "soy", "roasted"],
    },
    {
        "id": "burrata-heirloom",
        "name": "Heirloom Tomato & Burrata",
        "category": "appetizer",
        "description": "Fresh burrata with heirloom tomatoes, basil oil, and aged balsamic",
        "price": 22.00,
        "ingredients": ["burrata", "heirloom tomato", "basil", "balsamic vinegar", "olive oil", "sea salt", "black pepper"],
        "allergens": ["dairy", "nightshade"],
        "allergen_family": ["dairy", "nightshade"],
        "flavor_profile": ["cream", "tart", "basil", "mild", "herbaceous"],
    },
    {
        "id": "tuna-tartare",
        "name": "Tuna Tartare",
        "category": "appetizer",
        "description": "Yellowfin tuna with avocado, sesame, and yuzu ponzu",
        "price": 26.00,
        "ingredients": ["tuna", "avocado", "sesame", "yuzu", "soy sauce", "chive", "ginger"],
        "allergens": ["fish", "sesame", "soy", "allium"],
        "allergen_family": ["fish", "sesame", "soy", "allium"],
        "flavor_profile": ["citrus", "umami", "delicate", "ginger", "briny"],
    },
    {
        "id": "lobster-bisque",
        "name": "Lobster Bisque",
        "category": "appetizer",
        "description": "Classic bisque with Maine lobster, cognac, and chive cream",
        "price": 24.00,
        "ingredients": ["lobster", "cream", "cognac", "tomato paste", "onion", "celery", "garlic", "butter", "chive"],
        "allergens": ["shellfish", "dairy", "allium", "celery"],
        "allergen_family": ["shellfish", "dairy", "allium", "celery"],
        "flavor_profile": ["cream", "smoky", "umami", "briny", "roasted"],
    },
    {
        "id": "chicken-herb",
        "name": "Herb-Roasted Free-Range Chicken",
        "category": "entree",
        "description": "Half chicken with lemon, thyme, and roasted root vegetables",
        "price": 32.00,
        "ingredients": ["chicken", "lemon", "thyme", "rosemary", "carrot", "parsnip", "olive oil", "garlic", "sea salt"],
        "allergens": ["allium"],
        "allergen_family": ["allium"],
        "flavor_profile": ["roasted", "herbaceous", "citrus", "earthy", "thyme"],
    },
    {
        "id": "mushroom-pappardelle",
        "name": "Wild Mushroom Pappardelle",
        "category": "entree",
        "description": "Fresh pappardelle with porcini, chanterelle, and truffle oil",
        "price": 28.00,
        "ingredients": ["pappardelle", "porcini mushroom", "chanterelle", "truffle oil", "parmesan", "garlic", "shallot", "butter", "thyme"],
        "allergens": ["gluten", "dairy", "allium"],
        "allergen_family": ["gluten", "dairy", "allium"],
        "flavor_profile": ["earthy", "truffle", "umami", "cream", "herbaceous"],
    },
    {
        "id": "beet-salad",
        "name": "Roasted Beet & Goat Cheese Salad",
        "category": "appetizer",
        "description": "Roasted beets with goat cheese, candied walnut, and arugula",
        "price": 18.00,
        "ingredients": ["beet", "goat cheese", "walnut", "arugula", "honey", "balsamic vinegar", "olive oil"],
        "allergens": ["dairy", "tree_nut"],
        "allergen_family": ["dairy", "tree_nut"],
        "flavor_profile": ["earthy", "honey", "tart", "mild", "herbaceous"],
    },
    {
        "id": "lamb-rack",
        "name": "Rack of Lamb",
        "category": "entree",
        "description": "New Zealand rack with pistachio crust, mint gremolata, and potato gratin",
        "price": 56.00,
        "ingredients": ["lamb rack", "pistachio", "breadcrumb", "mint", "parsley", "garlic", "dijon", "potato", "cream", "gruyere"],
        "allergens": ["tree_nut", "gluten", "dairy", "mustard", "allium"],
        "allergen_family": ["tree_nut", "gluten", "dairy", "mustard", "allium"],
        "flavor_profile": ["roasted", "herbaceous", "mint", "earthy", "meaty"],
    },
    {
        "id": "chocolate-fondant",
        "name": "Valrhona Chocolate Fondant",
        "category": "dessert",
        "description": "Molten chocolate cake with vanilla bean ice cream and salted caramel",
        "price": 16.00,
        "ingredients": ["dark chocolate", "butter", "egg", "sugar", "flour", "vanilla", "cream", "caramel", "sea salt"],
        "allergens": ["dairy", "egg", "gluten"],
        "allergen_family": ["dairy", "egg", "gluten"],
        "flavor_profile": ["caramel", "vanilla", "cream", "smooth"],
    },
    {
        "id": "creme-brulee",
        "name": "Classic Creme Brulee",
        "category": "dessert",
        "description": "Tahitian vanilla creme brulee with seasonal berries",
        "price": 14.00,
        "ingredients": ["cream", "egg yolk", "sugar", "vanilla bean", "berries"],
        "allergens": ["dairy", "egg"],
        "allergen_family": ["dairy", "egg"],
        "flavor_profile": ["vanilla", "caramel", "cream", "smooth", "delicate"],
    },
    {
        "id": "caesar-salad",
        "name": "Classic Caesar Salad",
        "category": "appetizer",
        "description": "Romaine hearts with house-made caesar dressing, croutons, and parmesan",
        "price": 16.00,
        "ingredients": ["romaine", "parmesan", "crouton", "anchovy", "garlic", "egg yolk", "lemon", "olive oil", "dijon"],
        "allergens": ["dairy", "fish", "gluten", "egg", "mustard", "allium"],
        "allergen_family": ["dairy", "fish", "gluten", "egg", "mustard", "allium"],
        "flavor_profile": ["umami", "citrus", "anchovy", "caper", "herbaceous"],
    },
    {
        "id": "scallop-seared",
        "name": "Pan-Seared Diver Scallops",
        "category": "special",
        "description": "Day-boat scallops with cauliflower puree, brown butter, and golden raisin",
        "price": 44.00,
        "ingredients": ["scallop", "cauliflower", "butter", "golden raisin", "caper", "lemon", "parsley"],
        "allergens": ["shellfish", "dairy"],
        "allergen_family": ["shellfish", "dairy"],
        "flavor_profile": ["briny", "butter", "delicate", "citrus", "caramel"],
    },
]


def seed(host: str = REDIS_HOST, port: int = REDIS_PORT) -> None:
    print(f"Connecting to Redis at {host}:{port}...")
    r = redis.Redis(host=host, port=port, decode_responses=True)

    for attempt in range(10):
        try:
            r.ping()
            break
        except redis.ConnectionError:
            print(f"  Redis not ready, retrying... ({attempt + 1}/10)")
            time.sleep(2)
    else:
        print("ERROR: Could not connect to Redis")
        sys.exit(1)

    print(f"Seeding {len(MENU_ITEMS)} menu items...")
    for item in MENU_ITEMS:
        key = f"{MENU_KEY_PREFIX}{item['id']}"
        r.json().set(key, "$", item)
        allergen_str = ", ".join(item["allergen_family"])
        print(f"  {item['name']:40s} [{item['category']:10s}] allergens: {allergen_str}")

    print("\nVerifying seed data...")
    for item in MENU_ITEMS:
        key = f"{MENU_KEY_PREFIX}{item['id']}"
        result = r.json().get(key, "$")
        if result and len(result) > 0:
            print(f"  OK: {item['name']}")
        else:
            print(f"  FAIL: {item['name']}")

    print(f"\nMenu seed complete. {len(MENU_ITEMS)} items loaded.")


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else REDIS_HOST
    port = int(sys.argv[2]) if len(sys.argv) > 2 else REDIS_PORT
    seed(host, port)
