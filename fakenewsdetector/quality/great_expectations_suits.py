import great_expectations as gx
import pandas as pd


def run_suite():
    """
    Suite de tests qualité adaptée à la problématique marocaine.
    Vérifie la complétude, cohérence et validité des données Silver.
    """
    context = gx.get_context()

    ds = context.sources.add_or_update_pandas(name="fakenews_maroc")
    da = ds.add_dataframe_asset(name="silver_articles")
    batch = da.get_batch_request()
    validator = context.get_validator(batch_request=batch)

    # ── Complétude ────────────────────────────────────────────────────────
    validator.expect_column_values_to_not_be_null("titre")
    validator.expect_column_values_to_not_be_null("contenu")
    validator.expect_column_values_to_not_be_null("source")
    validator.expect_column_values_to_not_be_null("date_publication")
    validator.expect_column_values_to_not_be_null("pays")
    validator.expect_column_values_to_not_be_null("langue_detectee")

    # ── Validité ──────────────────────────────────────────────────────────
    # Contenu minimal (50 caractères — articles très courts = suspects)
    validator.expect_column_value_lengths_to_be_between("contenu", min_value=50)

    # Score entre 0 et 1
    validator.expect_column_values_to_be_between(
        "fakeness_score", min_value=0.0, max_value=1.0
    )

    # Labels valides
    validator.expect_column_values_to_be_in_set(
        "label", ["FIABLE", "SUSPECT", "FAKE"]
    )

    # URL valide
    validator.expect_column_values_to_match_regex("url", r"^https?://")

    # Langues attendues : arabe, français, anglais principalement
    validator.expect_column_values_to_be_in_set(
        "langue_detectee", ["ar", "fr", "en", "inconnu"]
    )

    # Pays attendus
    validator.expect_column_values_to_be_in_set(
        "pays", ["Maroc", "International", "Inconnu"]
    )

    # Sources connues
    validator.expect_column_values_to_be_in_set(
        "source",
        ["Hespress", "Akhbarona", "Barlamane", "BBC News", "Al Jazeera", "Reuters"],
    )

    # Source type
    validator.expect_column_values_to_be_in_set(
        "source_type", ["marocaine", "reference"]
    )

    # ── Cohérence ─────────────────────────────────────────────────────────
    # Un article FAKE doit avoir un score > 0.6
    validator.expect_column_pair_values_to_be_equal(
        "label",
        "fakeness_score",
    )

    # Titre non vide
    validator.expect_column_value_lengths_to_be_between("titre", min_value=5)

    results = validator.validate()

    ok = results["statistics"]["successful_expectations"]
    total = results["statistics"]["evaluated_expectations"]
    print(f"[GE] Qualité données Silver : {ok}/{total} checks réussis")

    failed = [
        r["expectation_config"]["expectation_type"]
        for r in results["results"]
        if not r["success"]
    ]
    if failed:
        print(f"[GE] Checks échoués : {failed}")

    return results