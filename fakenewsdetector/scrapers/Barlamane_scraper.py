from scrapers.base_scraper import BaseScraper


class BarlamaneScraper(BaseScraper):
    """
    Scraper pour Barlamane.com — site d'actualité marocain politique.
    Spécialisé dans l'actualité parlementaire et politique marocaine,
    souvent source de rumeurs politiques amplifiées sur les réseaux sociaux.
    """

    def __init__(self):
        super().__init__(
            source_name="Barlamane",
            base_url="https://www.barlamane.com",
            pays="Maroc",
        )

    def scrape(self):
        try:
            soup = self.fetch_page(self.base_url)
        except Exception as e:
            print(f"[Barlamane] Impossible d'accéder au site : {e}")
            return

        articles = soup.select("article, .post, .news-block")

        print(f"[Barlamane] {len(articles)} articles trouvés")

        for art in articles[:20]:
            try:
                title_tag = art.select_one("h2 a, h3 a, .entry-title a")
                if not title_tag:
                    continue

                url = title_tag.get("href", "")
                if not url.startswith("http"):
                    url = self.base_url + url

                title = title_tag.get_text(strip=True)
                if not title:
                    continue

                detail = self.fetch_page(url)

                content_div = detail.select_one(
                    ".entry-content, .post-content, .article-body"
                )
                content = (
                    content_div.get_text(separator=" ", strip=True)
                    if content_div
                    else ""
                )

                author_tag = detail.select_one(".author-name, .post-author, .byline")
                author = author_tag.get_text(strip=True) if author_tag else None

                date_tag = detail.select_one("time, .post-date, .entry-date")
                date = (
                    date_tag.get("datetime") or date_tag.get_text(strip=True)
                    if date_tag
                    else None
                )

                article = self.build_article(
                    title=title,
                    content=content,
                    author=author,
                    date=date,
                    category="Politique",
                    url=url,
                )

                self.send_to_kafka(article)

            except Exception as e:
                print(f"[Barlamane] Erreur : {e}")