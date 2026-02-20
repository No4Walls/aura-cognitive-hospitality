"""Pure-logic helpers for the Sommelier agent.

These functions have **no** infrastructure dependencies (no Kafka, Redis,
Prometheus) so they can be imported safely from local test scripts without
requiring the full container environment.
"""
from __future__ import annotations

from sommelier.app.ontology import (
    expand_allergen_to_family,
    get_allergen_family,
    get_flavor_category,
    get_wine_pairings,
)


def detect_intent(text: str) -> dict | None:
    text_lower = text.lower()
    intent: dict = {"type": None, "details": {}}

    exclusion_keywords = [
        "no ", "without ", "allergic to ", "allergy ", "can't eat ", "cannot eat ",
        "don't want ", "avoid ", "intolerant ", "sensitive to ", "free from ",
    ]
    for kw in exclusion_keywords:
        if kw in text_lower:
            idx = text_lower.index(kw) + len(kw)
            remainder = text_lower[idx:].strip().rstrip(".,!?")
            excluded = [w.strip() for w in remainder.split(" and ")]
            if len(excluded) == 1:
                excluded = [w.strip() for w in remainder.split(",")]
            intent["type"] = "ingredient_filter"
            intent["details"]["excluded"] = excluded
            return intent

    pairing_keywords = [
        "pair", "wine", "drink with", "goes with", "complement",
        "recommend a wine", "what wine", "beverage",
    ]
    if any(kw in text_lower for kw in pairing_keywords):
        dish_hint = None
        for phrase in ["with the ", "for the ", "with my ", "for my "]:
            if phrase in text_lower:
                dish_hint = text_lower.split(phrase, 1)[1].strip().rstrip(".,!?")
                break
        intent["type"] = "wine_pairing"
        intent["details"]["dish_hint"] = dish_hint
        return intent

    menu_keywords = [
        "menu", "dish", "what do you have", "recommend", "special",
        "what's good", "suggest", "options",
    ]
    if any(kw in text_lower for kw in menu_keywords):
        intent["type"] = "menu_inquiry"
        return intent

    return None


def filter_menu_by_exclusion(
    menu: list[dict], excluded_terms: list[str]
) -> tuple[list[dict], list[dict], list[str]]:
    expanded_exclusions: set[str] = set()
    reasoning_notes: list[str] = []

    for term in excluded_terms:
        family_members = expand_allergen_to_family(term)
        if len(family_members) > 1:
            expanded_exclusions.update(family_members)
            family = get_allergen_family(term)
            family_name = family if family else term.lower().strip()
            reasoning_notes.append(
                f"'{term}' is part of the '{family_name}' family. "
                f"Expanding filter to include: {', '.join(family_members)}."
            )
        else:
            expanded_exclusions.add(term.lower().strip())
            reasoning_notes.append(
                f"'{term}' has no known allergen family. Filtering as exact ingredient."
            )

    safe_items: list[dict] = []
    excluded_items: list[dict] = []

    for item in menu:
        item_ingredients = {i.lower() for i in item.get("ingredients", [])}
        item_allergens = {a.lower() for a in item.get("allergens", [])}
        item_families = {f.lower() for f in item.get("allergen_family", [])}
        all_item_terms = item_ingredients | item_allergens | item_families

        if expanded_exclusions & all_item_terms:
            excluded_items.append(item)
        else:
            safe_items.append(item)

    return safe_items, excluded_items, reasoning_notes


def find_pairing(menu: list[dict], dish_hint: str | None) -> dict | None:
    if not dish_hint:
        return None

    dish_hint_lower = dish_hint.lower()
    matched_dish = None
    for item in menu:
        if dish_hint_lower in item.get("name", "").lower():
            matched_dish = item
            break
        if any(dish_hint_lower in ing.lower() for ing in item.get("ingredients", [])):
            matched_dish = item
            break

    if not matched_dish:
        return None

    flavor_tags = matched_dish.get("flavor_profile", [])
    category = get_flavor_category(flavor_tags)
    if not category:
        return None

    pairings = get_wine_pairings(category)
    return {
        "dish": matched_dish.get("name", ""),
        "flavor_category": category,
        "pairings": pairings,
    }
