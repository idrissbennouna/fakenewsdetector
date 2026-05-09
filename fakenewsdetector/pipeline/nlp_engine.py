import re
import numpy as np

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import text

try:
    import spacy
    nlp = spacy.load("fr_core_news_sm")
except (ImportError, OSError):
    spacy = None
    nlp = None
# APRÈS
if spacy is not None:
    try:
        nlp_fr = spacy.load("fr_core_news_md")
    except OSError:
        try:
            nlp_fr = spacy.load("fr_core_news_sm")
        except OSError:
            nlp_fr = None

    try:
        nlp_ar = spacy.load("ar_core_news_sm")
    except OSError:
        nlp_ar = None
        print("[NLP] Modèle arabe non disponible")
else:
    nlp_fr = None
    nlp_ar = None
    print("[NLP] spaCy non disponible — NER désactivé")

def get_nlp(langue: str):
    """Retourne le bon modèle spaCy selon la langue détectée."""
    if langue == "ar" and nlp_ar is not None:
        return nlp_ar
    return nlp_fr


MOTS_SENSATIONNALISTES = [
    # Français — presse marocaine francophone
    "exclusif", "choc", "scandale", "incroyable", "révélation",
    "secret", "urgent", "alerte", "catastrophe", "explosion",
    "complot", "vérité cachée", "on vous cache", "silence",
    # Arabe — termes sensationnalistes courants sur les réseaux marocains
    "عاجل",       # urgent
    "خبر عاجل",   # breaking news
    "حصري",       # exclusif
    "صادم",       # choquant
    "كارثة",      # catastrophe
    "خطير",       # dangereux
    "مثير",       # excitant/sensationnel
    "سر",         # secret
    "مؤامرة",     # complot
    "فضيحة",      # scandale
    "الحقيقة",    # la vérité (souvent utilisé pour attirer)
    "يخفون عنك",  # ils vous cachent
    "لن تصدق",    # vous n'y croirez pas
    # Darija marocaine translittérée
    "ach9ar",
    "3ajel",
]

SOURCES_REFERENCE = {"BBC News", "Al Jazeera", "Reuters", "Le Monde", "AFP"}
SOURCES_MAROCAINES = {"Hespress", "Akhbarona", "Barlamane", "Lakom"}


class FakeNewsEngine:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            max_features=5000,
            ngram_range=(1, 2),
            analyzer="word",
        )
        self.article_corpus = []
        self.corpus_vectors = None

    # ── Nettoyage ──────────────────────────────────────────────────────────
    def clean_text(self, text: str) -> str:
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"http\S+", " ", text)
        text = re.sub(r"[^\w\s\u0600-\u06FF\u0750-\u077F]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def lemmatize(self, text: str, langue: str = "fr") -> str:
        model = get_nlp(langue)
        if model is None:
            return text.lower()
        doc = model(text[:8000])
        return " ".join(
            token.lemma_.lower()
            for token in doc
            if not token.is_stop and not token.is_punct and len(token.text) > 2
        )

    def extract_entities(self, text: str, langue: str = "fr") -> dict:
        model = get_nlp(langue)
        if model is None:
            return {"personnes": [], "organisations": [], "lieux": []}
        doc = model(text[:5000])
        return {
            "personnes": list({e.text for e in doc.ents if e.label_ in ("PER", "PERSON")}),
            "organisations": list({e.text for e in doc.ents if e.label_ in ("ORG",)}),
            "lieux": list({e.text for e in doc.ents if e.label_ in ("LOC", "GPE")}),
        }

    # ── Scoring fake news — adapté au contexte marocain ────────────────────
    def compute_fakeness_score(self, article: dict) -> float:
        """
        Score de désinformation 0.0 (fiable) → 1.0 (probablement fake).
        Signaux calibrés pour la presse marocaine arabophone et francophone.
        """
        score = 0.0
        text = (article.get("contenu") or "").lower()
        title = (article.get("titre") or "").lower()
        full = title + " " + text

        # Signal 1 : titre en majuscules ou très ponctué
        titre_original = article.get("titre", "")
        if len(titre_original) > 5:
            ratio_maj = sum(1 for c in titre_original if c.isupper()) / len(titre_original)
            if ratio_maj > 0.5:
                score += 0.20
        if titre_original.count("!") + titre_original.count("?") > 2:
            score += 0.15

        # Signal 2 : mots sensationnalistes (FR + AR + darija)
        hits = sum(1 for mot in MOTS_SENSATIONNALISTES if mot.lower() in full)
        score += min(hits * 0.08, 0.30)

        # Signal 3 : contenu très court (< 150 mots) — souvent du clickbait
        nb_mots = len(text.split())
        if nb_mots < 150:
            score += 0.15
        elif nb_mots < 80:
            score += 0.25

        # Signal 4 : auteur inconnu ou absent
        auteur = article.get("auteur", "")
        if not auteur or auteur in ("Inconnu", "", "رأي", "مراسلنا"):
            score += 0.10

        # Signal 5 : source non référence (marocaine) = risque plus élevé
        source = article.get("source", "")
        if source in SOURCES_MAROCAINES:
            score += 0.05
        if source in SOURCES_REFERENCE:
            score -= 0.15

        # Signal 6 : patterns linguistiques de manipulation
        patterns_manipulation = [
            r"يخفون عنك",
            r"on (vous |te )?cache",
            r"la vérité sur",
            r"الحقيقة (التي|المخفية)",
            r"ce que les médias ne disent pas",
        ]
        for pat in patterns_manipulation:
            if re.search(pat, full, re.IGNORECASE):
                score += 0.15
                break

        return round(min(max(score, 0.0), 1.0), 3)

    # ── Similarité cross-sources ───────────────────────────────────────────
    def update_corpus(self, articles: list):
        texts = []
        for a in articles:
            langue = a.get("langue_detectee", "fr")
            cleaned = self.clean_text(a.get("contenu") or "")
            lemma = self.lemmatize(cleaned, langue)
            texts.append(lemma)
        self.article_corpus = articles
        if texts:
            try:
                self.corpus_vectors = self.vectorizer.fit_transform(texts)
            except Exception as e:
                print(f"[NLP] Erreur vectorisation : {e}")

    def find_similar_articles(self, article: dict, threshold: float = 0.30) -> list:
        """
        Cherche des articles similaires dans d'autres sources.
        Si un article marocain parle d'un sujet sans qu'aucune source
        internationale ne le couvre → signal suspect.
        """
        if self.corpus_vectors is None or not self.article_corpus:
            return []

        langue = article.get("langue_detectee", "fr")
        cleaned = self.clean_text(article.get("contenu") or "")
        lemma = self.lemmatize(cleaned, langue)

        try:
            query_vec = self.vectorizer.transform([lemma])
        except Exception:
            return []

        sims = cosine_similarity(query_vec, self.corpus_vectors)[0]
        similaires = [
            {
                "source": self.article_corpus[i]["source"],
                "pays": self.article_corpus[i].get("pays", "?"),
                "titre": self.article_corpus[i]["titre"],
                "similarite": round(float(sims[i]), 3),
            }
            for i in np.argsort(sims)[::-1]
            if sims[i] >= threshold
            and self.article_corpus[i]["id"] != article["id"]
        ]
        return similaires[:5]

    def has_international_corroboration(self, similar_articles: list) -> bool:
        """
        Vérifie si au moins une source internationale confirme l'info.
        C'est le signal clé de notre problématique marocaine :
        un article Hespress non corroboré par BBC/Al Jazeera est suspect.
        """
        return any(
            a["source"] in SOURCES_REFERENCE for a in similar_articles
        )

    # ── Enrichissement complet ─────────────────────────────────────────────
    def enrich(self, article: dict) -> dict:
        langue = article.get("langue_detectee", "fr")
        cleaned = self.clean_text(article.get("contenu") or "")
        lemmatized = self.lemmatize(cleaned, langue)
        entities = self.extract_entities(cleaned, langue)
        fakeness = self.compute_fakeness_score(article)
        similar = self.find_similar_articles(article)
        corrobore = self.has_international_corroboration(similar)

        # Bonus : si source marocaine et aucune corroboration internationale
        if article.get("source") in SOURCES_MAROCAINES and not corrobore:
            fakeness = min(fakeness + 0.10, 1.0)

        if fakeness > 0.60:
            label = "FAKE"
        elif fakeness > 0.35:
            label = "SUSPECT"
        else:
            label = "FIABLE"

        return {
            **article,
            "contenu_clean": cleaned,
            "contenu_lemma": lemmatized,
            "entites": entities,
            "fakeness_score": round(fakeness, 3),
            "label": label,
            "articles_similaires": similar,
            "nb_sources_corroborantes": len(similar),
            "corrobore_internationalement": corrobore,
            "source_type": (
                "reference" if article.get("source") in SOURCES_REFERENCE
                else "marocaine"
            ),
        }