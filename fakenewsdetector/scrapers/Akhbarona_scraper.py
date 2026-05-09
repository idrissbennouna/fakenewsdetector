from scrapers.base_scraper import BaseScraper


class AkhbaronaScraper(BaseScraper):
    """
    Scraper pour Akhbarona.com — site d'actualité marocain en arabe.
    Source centrale dans la problématique : Akhbarona est l'un des sites
    marocains les plus partagés sur WhatsApp et Facebook, souvent sans
    vérification préalable des faits par les lecteurs.
    """

    def __init__(self):
        super().__init__(
            source_name="Akhbarona",
            base_url="https://www.akhbarona.com",
            pays="Maroc",
        )

    def scrape(self):
        try:
            soup = self.fetch_page(self.base_url)
        except Exception as e:
            print(f"[Akhbarona] Impossible d'accéder au site : {e}")
            return

        articles = soup.select("article, .news-item, .article-box, li.item")

        print(f"[Akhbarona] {len(articles)} articles trouvés")

        for art in articles[:25]:
            try:
                title_tag = art.select_one("h2 a, h3 a, .title a, .news-title a")
                if not title_tag:
                    continue

                url = title_tag.get("href", "")
                if not url.startswith("http"):
                    url = self.base_url.rstrip("/") + "/" + url.lstrip("/")

                title = title_tag.get_text(strip=True)
                if not title:
                    continue

                detail = self.fetch_page(url)

                content_div = detail.select_one(
                    ".article-content, .news-content, .content-body, .post-body"
                )
                content = (
                    content_div.get_text(separator=" ", strip=True)
                    if content_div
                    else ""
                )

                author_tag = detail.select_one(".author, .writer-name, .post-author")
                author = author_tag.get_text(strip=True) if author_tag else None

                date_tag = detail.select_one("time, .date, .publish-date, .post-date")
                date = (
                    date_tag.get("datetime") or date_tag.get_text(strip=True)
                    if date_tag
                    else None
                )

                cat_tag = detail.select_one(".category, .section-name, .rubrique")
                category = cat_tag.get_text(strip=True) if cat_tag else None

                article = self.build_article(
                    title=title,
                    content=content,
                    author=author,
                    date=date,
                    category=category,
                    url=url,
                )

                self.send_to_kafka(article)

            except Exception as e:
                print(f"[Akhbarona] Erreur sur article : {e}")
                self.send_to_kafka(
                    {"error": str(e), "source": "Akhbarona"},
                    topic="dlq-errors",
                )