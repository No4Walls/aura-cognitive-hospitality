from __future__ import annotations

import json
import os
import sys
import time

import redis

sys.path.insert(0, ".")
sys.path.insert(0, os.path.join(".", "agents"))
from agents.sommelier.app.ontology import (
    ALLERGEN_FAMILIES,
    expand_allergen_to_family,
)
from agents.sommelier.app.logic import (
    detect_intent,
    filter_menu_by_exclusion,
    find_pairing,
)

REDIS_HOST = "localhost"
REDIS_PORT = 6379
MENU_KEY_PREFIX = "aura:menu:"


def load_menu(r: redis.Redis) -> list[dict]:
    keys = r.keys(f"{MENU_KEY_PREFIX}*")
    items: list[dict] = []
    for key in sorted(keys):
        data = r.json().get(key, "$")
        if data and len(data) > 0:
            items.append(data[0])
    return items


def test_onion_filter(menu: list[dict]) -> dict:
    start = time.time()

    allium_members = expand_allergen_to_family("allium")

    intent = detect_intent("I'd like to order but I can't have any onions please")
    safe_items, removed_items, expanded = filter_menu_by_exclusion(menu, ["onion"])

    elapsed_ms = (time.time() - start) * 1000

    false_positives = 0
    for item in safe_items:
        item_ingredients = [i.lower() for i in item.get("ingredients", [])]
        item_families = [f.lower() for f in item.get("allergen_family", [])]
        for member in allium_members:
            if member in item_ingredients:
                false_positives += 1
                print(f"  FALSE POSITIVE: '{item['name']}' contains '{member}'")
        if "allium" in item_families:
            for ing in item_ingredients:
                if ing in allium_members:
                    false_positives += 1
                    print(f"  FALSE POSITIVE (family): '{item['name']}' has allium member '{ing}'")

    return {
        "test": "onion_filter",
        "input": "no onions",
        "expanded_to": expanded,
        "allium_family": allium_members,
        "total_menu_items": len(menu),
        "safe_items": len(safe_items),
        "removed_items": len(removed_items),
        "safe_names": [i["name"] for i in safe_items],
        "removed_names": [i["name"] for i in removed_items],
        "false_positives": false_positives,
        "latency_ms": round(elapsed_ms, 2),
        "pass": false_positives == 0 and elapsed_ms < 150,
    }


def test_garlic_filter(menu: list[dict]) -> dict:
    start = time.time()
    safe_items, removed_items, expanded = filter_menu_by_exclusion(menu, ["garlic"])
    elapsed_ms = (time.time() - start) * 1000

    allium_members = expand_allergen_to_family("allium")
    false_positives = 0
    for item in safe_items:
        item_ingredients = [i.lower() for i in item.get("ingredients", [])]
        for member in allium_members:
            if member in item_ingredients:
                false_positives += 1

    return {
        "test": "garlic_filter",
        "expanded_to": expanded,
        "safe_items": len(safe_items),
        "removed_items": len(removed_items),
        "false_positives": false_positives,
        "latency_ms": round(elapsed_ms, 2),
        "pass": false_positives == 0 and elapsed_ms < 150,
    }


def test_shellfish_filter(menu: list[dict]) -> dict:
    start = time.time()
    safe_items, removed_items, expanded = filter_menu_by_exclusion(menu, ["shrimp"])
    elapsed_ms = (time.time() - start) * 1000

    shellfish_members = expand_allergen_to_family("shellfish")
    false_positives = 0
    for item in safe_items:
        item_ingredients = [i.lower() for i in item.get("ingredients", [])]
        for member in shellfish_members:
            if member in item_ingredients:
                false_positives += 1

    return {
        "test": "shellfish_filter",
        "expanded_to": expanded,
        "safe_items": len(safe_items),
        "removed_items": len(removed_items),
        "false_positives": false_positives,
        "latency_ms": round(elapsed_ms, 2),
        "pass": false_positives == 0 and elapsed_ms < 150,
    }


def test_dairy_filter(menu: list[dict]) -> dict:
    start = time.time()
    safe_items, removed_items, expanded = filter_menu_by_exclusion(menu, ["dairy"])
    elapsed_ms = (time.time() - start) * 1000

    dairy_members = expand_allergen_to_family("dairy")
    false_positives = 0
    for item in safe_items:
        item_ingredients = [i.lower() for i in item.get("ingredients", [])]
        for member in dairy_members:
            if member in item_ingredients:
                false_positives += 1

    return {
        "test": "dairy_filter",
        "expanded_to": expanded,
        "safe_items": len(safe_items),
        "removed_items": len(removed_items),
        "false_positives": false_positives,
        "latency_ms": round(elapsed_ms, 2),
        "pass": false_positives == 0 and elapsed_ms < 150,
    }


def test_wine_pairing(menu: list[dict]) -> dict:
    start = time.time()
    pairing = find_pairing(menu, "branzino")
    elapsed_ms = (time.time() - start) * 1000

    return {
        "test": "wine_pairing",
        "dish_hint": "branzino",
        "pairing": pairing,
        "latency_ms": round(elapsed_ms, 2),
        "pass": pairing is not None and elapsed_ms < 150,
    }


def test_intent_detection() -> dict:
    cases = [
        ("I can't have any onions", "ingredient_filter"),
        ("What wine goes with the salmon?", "wine_pairing"),
        ("What's on the menu tonight?", "menu_inquiry"),
        ("I'm allergic to shellfish", "ingredient_filter"),
        ("No nuts please", "ingredient_filter"),
    ]
    results = []
    all_pass = True
    for text, expected in cases:
        intent = detect_intent(text)
        detected = intent["type"] if intent else None
        passed = detected == expected
        if not passed:
            all_pass = False
        results.append({"text": text, "expected": expected, "detected": detected, "pass": passed})

    return {"test": "intent_detection", "cases": results, "pass": all_pass}


def main() -> None:
    print("=" * 60)
    print("PROJECT AURA - ONION TEST SUITE (Phase 2)")
    print("=" * 60)

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        menu = load_menu(r)
        print(f"\nLoaded {len(menu)} menu items from Redis\n")
    except redis.ConnectionError:
        print("\nRedis not available - running with embedded test menu")
        menu = _fallback_menu()
        print(f"Using {len(menu)} fallback menu items\n")

    if not menu:
        print("No menu items found. Run seed_menu.py first.")
        menu = _fallback_menu()

    tests = [
        test_onion_filter(menu),
        test_garlic_filter(menu),
        test_shellfish_filter(menu),
        test_dairy_filter(menu),
        test_wine_pairing(menu),
        test_intent_detection(),
    ]

    all_pass = True
    for result in tests:
        status = "PASS" if result["pass"] else "FAIL"
        latency = result.get("latency_ms", "N/A")
        print(f"  [{status}] {result['test']} (latency: {latency}ms)")
        if not result["pass"]:
            all_pass = False
            print(f"         Details: {json.dumps(result, indent=2, default=str)}")

    print(f"\n{'=' * 60}")
    if all_pass:
        print("ALL TESTS PASSED - 0% false positives on allergen filtering")
    else:
        print("SOME TESTS FAILED - review details above")
    print(f"{'=' * 60}")

    sys.exit(0 if all_pass else 1)


def _fallback_menu() -> list[dict]:
    return [
        {
            "id": "menu_001", "name": "Grilled Branzino", "category": "entree",
            "ingredients": ["branzino", "lemon", "olive oil", "caper", "thyme"],
            "allergens": ["fish", "dairy"], "allergen_family": ["fish", "dairy"],
            "flavor_profile": ["briny", "citrus", "butter", "delicate", "herbaceous"],
            "price": 42.0,
        },
        {
            "id": "menu_002", "name": "Dry-Aged Ribeye", "category": "entree",
            "ingredients": ["ribeye", "garlic", "butter", "rosemary", "shallot"],
            "allergens": ["dairy", "allium"], "allergen_family": ["dairy", "allium"],
            "flavor_profile": ["umami", "smoky", "roasted", "earthy", "meaty"],
            "price": 68.0,
        },
        {
            "id": "menu_003", "name": "Black Truffle Risotto", "category": "entree",
            "ingredients": ["arborio rice", "black truffle", "parmesan", "onion", "butter", "white wine"],
            "allergens": ["dairy", "allium", "sulfite"], "allergen_family": ["dairy", "allium", "sulfite"],
            "flavor_profile": ["truffle", "cream", "umami", "earthy", "smooth"],
            "price": 38.0,
        },
        {
            "id": "menu_004", "name": "Classic Caesar Salad", "category": "appetizer",
            "ingredients": ["romaine", "parmesan", "crouton", "anchovy", "garlic", "egg yolk", "lemon", "dijon"],
            "allergens": ["dairy", "fish", "gluten", "egg", "mustard", "allium"],
            "allergen_family": ["dairy", "fish", "gluten", "egg", "mustard", "allium"],
            "flavor_profile": ["umami", "citrus", "anchovy", "caper", "herbaceous"],
            "price": 16.0,
        },
        {
            "id": "menu_005", "name": "Lobster Bisque", "category": "appetizer",
            "ingredients": ["lobster", "cream", "onion", "garlic", "tomato paste", "thyme"],
            "allergens": ["shellfish", "dairy", "allium"],
            "allergen_family": ["shellfish", "dairy", "allium"],
            "flavor_profile": ["cream", "smoky", "umami", "briny", "roasted"],
            "price": 24.0,
        },
        {
            "id": "menu_006", "name": "Roasted Beet & Goat Cheese Salad", "category": "appetizer",
            "ingredients": ["beet", "goat cheese", "walnut", "arugula", "balsamic vinegar"],
            "allergens": ["dairy", "tree_nut"],
            "allergen_family": ["dairy", "tree_nut"],
            "flavor_profile": ["earthy", "honey", "tart", "mild", "herbaceous"],
            "price": 18.0,
        },
        {
            "id": "menu_007", "name": "Pan-Seared Diver Scallops", "category": "special",
            "ingredients": ["scallop", "cauliflower", "butter", "caper", "lemon"],
            "allergens": ["shellfish", "dairy"],
            "allergen_family": ["shellfish", "dairy"],
            "flavor_profile": ["briny", "butter", "delicate", "citrus", "caramel"],
            "price": 44.0,
        },
    ]


if __name__ == "__main__":
    main()
