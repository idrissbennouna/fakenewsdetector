import feedparser
from scrapers.base_scraper import BaseScraper


class BBCScraper(BaseScraper):
    """
    Scraper BBC News via flux RSS.
    Source internationale de reference pour corroboration.
    """

    RSS_FEEDS = [
        ("https://feeds.bbci.co.uk/news/world/rss.xml", "Monde", "en", "International"),
        ("https://feeds.bbci.co.uk/news/technology/rss.xml", "Tech", "en", "International"),
        ("https://feeds.bbci.co.uk/arabic/rss.xml", "عربي", "ar", "International"),
    ]

    def __init__(self):
        super().__init__(
            source_name="BBC News",
            base_url="https://www.bbc.com",
            pays="International",
        )

    def scrape(self):
        for feed_url, category, langue_hint, pays in self.RSS_FEEDS:
            try:
                print(f"[BBC] Lecture flux : {feed_url}")
                feed = feedparser.parse(feed_url)
                print(f"[BBC] Flux '{category}' — {len(feed.entries)} entrees")

                for entry in feed.entries[:15]:
                    try:
                        print(f"[BBC] Traitement : {entry.title[:50]}")

                        detail = self.fetch_page(entry.link)
                        body_blocks = detail.select(
                            "div[data-component='text-block'] p, "
                            ".article__body p, "
                            ".story-body__inner p"
                        )
                        content = " ".join(
                            b.get_text(strip=True) for b in body_blocks
                        )
                        print(f"[BBC] Contenu : {len(content)} caracteres")

                        article = self.build_article(
                            title=entry.title,
                            content=content,
                            author=entry.get("author"),
                            date=entry.get("published"),
                            category=category,
                            url=entry.link,
                        )
                        article["pays"] = pays
                        article["source_reference"] = True
                        self.send_to_kafka(article)

                    except Exception as e:
                        print(f"[BBC] Erreur article : {e}")

            except Exception as e:
                print(f"[BBC] Erreur flux {feed_url} : {e}")


class AlJazeeraScraper(BaseScraper):
    """
    Scraper Al Jazeera via flux RSS.
    Source internationale arabe de reference.
    """

    RSS_FEEDS = [
        "https://www.aljazeera.net/rss",
        "https://www.aljazeera.com/xml/rss/all.xml",
    ]

    def __init__(self):
        super().__init__(
            source_name="Al Jazeera",
            base_url="https://www.aljazeera.net",
            pays="International",
        )

    def scrape(self):
        for feed_url in self.RSS_FEEDS:
            try:
                print(f"[Al Jazeera] Lecture flux : {feed_url}")
                feed = feedparser.parse(feed_url)
                print(f"[Al Jazeera] {len(feed.entries)} entrees")

                for entry in feed.entries[:15]:
                    try:
                        print(f"[Al Jazeera] Traitement : {entry.title[:50]}")

                        detail = self.fetch_page(entry.link)
                        content_div = detail.select_one(
                            ".article-p-wrapper, .wysiwyg, .article__body"
                        )
                        content = (
                            content_div.get_text(separator=" ", strip=True)
                            if content_div
                            else entry.get("summary", "")
                        )

                        article = self.build_article(
                            title=entry.title,
                            content=content,
                            author=entry.get("author"),
                            date=entry.get("published"),
                            category=entry.get("tags", [{}])[0].get("term") if entry.get("tags") else None,
                            url=entry.link,
                        )
                        article["source_reference"] = True
                        self.send_to_kafka(article)

                    except Exception as e:
                        print(f"[Al Jazeera] Erreur article : {e}")

            except Exception as e:
                print(f"[Al Jazeera] Erreur flux : {e}")


if __name__ == "__main__":
    BBCScraper().scrape()
    AlJazeeraScraper().scrape()