import requests
from bs4 import BeautifulSoup
from scrapers.base_scraper import BaseScraper


class HespressScraper(BaseScraper):
    """
    Scraper Hespress via flux RSS.
    Hespress charge ses articles en JavaScript — le RSS est plus fiable.
    """

    RSS_URL = "https://hespress.com/feed"

    def __init__(self):
        super().__init__(
            source_name="Hespress",
            base_url="https://hespress.com",
            pays="Maroc",
        )

    def scrape(self):
        print("[Hespress] Démarrage via RSS...")

        try:
            r = requests.get(
                self.RSS_URL,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            r.raise_for_status()
        except Exception as e:
            print(f"[Hespress] Erreur RSS : {e}")
            return

        soup = BeautifulSoup(r.text, "lxml-xml")
        items = soup.select("item")
        print(f"[Hespress] {len(items)} articles trouvés dans le flux RSS")

        for item in items:
            try:
                title = item.find("title")
                title = title.get_text(strip=True) if title else ""

                url = item.find("link")
                url = url.get_text(strip=True) if url else ""

                date = item.find("pubDate")
                date = date.get_text(strip=True) if date else None

                author = item.find("dc:creator") or item.find("creator")
                author = author.get_text(strip=True) if author else None

                category = item.find("category")
                category = category.get_text(strip=True) if category else None

                description = item.find("description")
                content_raw = description.get_text(strip=True) if description else ""

                content_soup = BeautifulSoup(content_raw, "html.parser")
                content = content_soup.get_text(separator=" ", strip=True)

                if len(content) < 100 and url:
                    try:
                        detail = self.fetch_page(url)
                        content_div = detail.select_one(
                            ".jeg_post_content, .post-content, "
                            ".entry-content, .article-content"
                        )
                        if content_div:
                            content = content_div.get_text(separator=" ", strip=True)
                    except Exception:
                        pass

                if not title or not url:
                    continue

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
                print(f"[Hespress] Erreur article : {e}")

        print("[Hespress] Scraping terminé.")


if __name__ == "__main__":
    HespressScraper().scrape()