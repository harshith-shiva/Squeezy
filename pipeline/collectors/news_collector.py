# pipeline/collectors/news_collector.py

import yfinance as yf
import requests
import pymongo
import os
import time
import hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

class NewsCollector:

    # Keywords that signal a squeeze catalyst
    BULLISH_CATALYSTS = [
        'short squeeze', 'gamma squeeze', 'short interest',
        'buyback', 'partnership', 'fda approval',
        'earnings beat', 'activist investor', 'takeover',
        'acquisition', 'merger', 'revenue beat',
        'raised guidance', 'upgraded', 'insider buying'
    ]

    BEARISH_CATALYSTS = [
        'sec investigation', 'fraud', 'bankruptcy',
        'dilution', 'offering', 'lawsuit', 'downgraded',
        'missed earnings', 'guidance cut', 'delisted',
        'short seller', 'resignation', 'accounting'
    ]

    def __init__(self):
        client = pymongo.MongoClient(
            os.getenv('MONGO_URL',
                      'mongodb://localhost:27017'))
        db = client[os.getenv('MONGO_DB', 'squeezradar')]
        self.collection = db['news_articles']

        # PostgreSQL for collection_log
        import psycopg2
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))

    # ─────────────────────────────────────────
    # MAIN METHOD
    # ─────────────────────────────────────────
    def collect(self, ticker: str) -> dict:
        start = time.time()
        all_articles = []

        # Source 1 — yfinance news (fastest, most reliable)
        yf_articles = self._fetch_yfinance(ticker)
        all_articles.extend(yf_articles)

        # Source 2 — Google News RSS (more coverage)
        rss_articles = self._fetch_google_rss(ticker)
        all_articles.extend(rss_articles)

        # Deduplicate by URL hash
        seen = set()
        unique_articles = []
        for article in all_articles:
            h = article.get('url_hash')
            if h and h not in seen:
                seen.add(h)
                unique_articles.append(article)

        # Detect catalysts
        for article in unique_articles:
            article['catalyst_flags'] = self._detect_catalyst(
                article.get('headline', ''))
            article['has_catalyst'] = len(
                article['catalyst_flags']) > 0

        # Save to MongoDB
        saved = self._save(unique_articles)

        duration = int((time.time() - start) * 1000)
        self._log(ticker, 'success',
                  saved, None, duration)

        result = {
            'ticker': ticker,
            'total_found': len(unique_articles),
            'saved': saved,
            'has_catalyst': any(
                a['has_catalyst'] for a in unique_articles),
            'catalyst_flags': list(set(
                flag
                for a in unique_articles
                for flag in a.get('catalyst_flags', [])
            )),
            'articles': unique_articles,
            'collected_at': datetime.utcnow()
        }

        print(f"  ✓ {ticker} news — "
              f"{len(unique_articles)} articles | "
              f"catalyst: {result['has_catalyst']} "
              f"{result['catalyst_flags']}")

        return result

    # ─────────────────────────────────────────
    # FETCH METHODS
    # ─────────────────────────────────────────
    def _fetch_yfinance(self, ticker: str) -> list:
        try:
            stock = yf.Ticker(ticker)
            raw_news = stock.news or []
            articles = []

            for item in raw_news:
                headline = item.get('title', '')
                url = item.get('link', '')

                articles.append({
                    'ticker': ticker,
                    'source': 'yfinance',
                    'headline': headline,
                    'url': url,
                    'url_hash': self._hash(url),
                    'publisher': item.get(
                        'publisher', 'unknown'),
                    'published_at': datetime.fromtimestamp(
                        item.get('providerPublishTime', 0),
                        tz=timezone.utc
                    ),
                    'collected_at': datetime.utcnow()
                })

            return articles

        except Exception as e:
            print(f"  yfinance news failed for {ticker}: {e}")
            return []

    def _fetch_google_rss(self, ticker: str) -> list:
        """
        Google News RSS — free, no API key
        Returns last 10 articles mentioning ticker
        """
        try:
            url = (f"https://news.google.com/rss/search"
                   f"?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en")
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers,
                                timeout=10)

            # Parse RSS XML manually (avoid extra dependency)
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.content)

            articles = []
            items = root.findall('.//item')

            for item in items[:10]:  # limit to 10
                title_el = item.find('title')
                link_el  = item.find('link')
                pubdate_el = item.find('pubDate')

                headline = title_el.text if title_el is not None else ''
                url = link_el.text if link_el is not None else ''

                if not headline or not url:
                    continue

                articles.append({
                    'ticker': ticker,
                    'source': 'google_rss',
                    'headline': headline,
                    'url': url,
                    'url_hash': self._hash(url),
                    'publisher': 'google_news',
                    'published_at': datetime.utcnow(),
                    'collected_at': datetime.utcnow()
                })

            return articles

        except Exception as e:
            print(f"  Google RSS failed for {ticker}: {e}")
            return []

    # ─────────────────────────────────────────
    # CATALYST DETECTION
    # ─────────────────────────────────────────
    def _detect_catalyst(self, headline: str) -> list:
        """
        Scans headline for squeeze-relevant catalyst keywords
        Returns list of matched catalyst types
        """
        headline_lower = headline.lower()
        flags = []

        for keyword in self.BULLISH_CATALYSTS:
            if keyword in headline_lower:
                flags.append(f"bullish:{keyword.replace(' ', '_')}")

        for keyword in self.BEARISH_CATALYSTS:
            if keyword in headline_lower:
                flags.append(f"bearish:{keyword.replace(' ', '_')}")

        return flags

    # ─────────────────────────────────────────
    # DATABASE WRITE
    # ─────────────────────────────────────────
    def _save(self, articles: list) -> int:
        if not articles:
            return 0
        try:
            result = self.collection.insert_many(
                articles, ordered=False)
            return len(result.inserted_ids)
        except pymongo.errors.BulkWriteError as e:
            # Some duplicates — that's fine
            return e.details.get('nInserted', 0)

    def _hash(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def _log(self, ticker: str, status: str,
              rows: int, error: str, duration_ms: int):
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO collection_log
            (ticker, source, status, rows_collected,
             error_message, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (ticker, 'news', status,
              rows, error, duration_ms))
        self.pg.commit()


    