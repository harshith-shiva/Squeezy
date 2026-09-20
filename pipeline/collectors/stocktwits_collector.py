# pipeline/collectors/stocktwits_collector.py

import requests
import pymongo
import psycopg2
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class StockTwitsCollector:

    BASE_URL = "https://api.stocktwits.com/api/2"

    def __init__(self):
        client = pymongo.MongoClient(
            os.getenv('MONGO_URL', 'mongodb://localhost:27017'))
        self.db = client[os.getenv('MONGO_DB', 'squeezradar')]
        self.pg = psycopg2.connect(os.getenv('POSTGRES_URL'))
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    def collect(self, ticker: str) -> dict:
        start = time.time()

        try:
            # Get message stream for ticker
            url = f"{self.BASE_URL}/streams/symbol/{ticker}.json"
            resp = self.session.get(url, timeout=10)

            if resp.status_code == 429:
                print(f"  ⚠ StockTwits rate limited — wait 60s")
                return {'ticker': ticker,
                        'error': True,
                        'error_message': 'rate_limited'}

            if resp.status_code != 200:
                return {'ticker': ticker, 'error': True,
                        'error_message': f'HTTP {resp.status_code}'}

            data = resp.json()
            messages = data.get('messages', [])
            symbol_data = data.get('symbol', {})

            # Count bull/bear sentiment
            bull_count = 0
            bear_count = 0
            processed_messages = []

            for msg in messages:
                sentiment = msg.get('entities', {}) \
                               .get('sentiment', {})
                sentiment_basic = sentiment.get('basic', '')

                if sentiment_basic == 'Bullish':
                    bull_count += 1
                elif sentiment_basic == 'Bearish':
                    bear_count += 1

                processed_messages.append({
                    'ticker': ticker,
                    'message_id': msg.get('id'),
                    'body': msg.get('body', ''),
                    'sentiment': sentiment_basic,
                    'likes': msg.get('likes', {}).get(
                        'total', 0),
                    'created_at': msg.get('created_at'),
                    'collected_at': datetime.utcnow()
                })

            total = len(messages)
            bull_ratio = bull_count / total if total > 0 else 0
            bear_ratio = bear_count / total if total > 0 else 0

            result = {
                'ticker': ticker,
                'message_count': total,
                'bull_count': bull_count,
                'bear_count': bear_count,
                'bull_ratio': round(bull_ratio, 4),
                'bear_ratio': round(bear_ratio, 4),
                'watchers': symbol_data.get(
                    'watchlist_count', 0),
                'collected_at': datetime.utcnow(),
                'error': False
            }

            # Save messages to MongoDB
            if processed_messages:
                try:
                    self.db['stocktwits'].insert_many(
                        processed_messages, ordered=False)
                except:
                    pass

            # Save summary to PostgreSQL
            self._save_summary(ticker, result)

            duration = int((time.time() - start) * 1000)
            self._log(ticker, 'success', total,
                      None, duration)

            print(f"  ✓ {ticker} StockTwits — "
                  f"{total} msgs | "
                  f"Bull: {bull_ratio:.0%} | "
                  f"Bear: {bear_ratio:.0%} | "
                  f"Watchers: {result['watchers']:,}")

            return result

        except Exception as e:
            self._log(ticker, 'failed', 0, str(e), 0)
            print(f"  ✗ {ticker} StockTwits failed: {e}")
            return {'ticker': ticker,
                    'error': True,
                    'error_message': str(e)}

    def get_trending(self) -> list:
        """
        Returns list of trending tickers on StockTwits right now
        Useful for fast-track detection
        No API key needed
        """
        try:
            url = f"{self.BASE_URL}/trending/symbols.json"
            resp = self.session.get(url, timeout=10)
            data = resp.json()
            symbols = data.get('symbols', [])
            return [s['symbol'] for s in symbols]
        except Exception as e:
            print(f"  ✗ StockTwits trending failed: {e}")
            return []

    def _save_summary(self, ticker: str, result: dict):
        cur = self.pg.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stocktwits_snapshots (
                id              SERIAL PRIMARY KEY,
                ticker          VARCHAR(10),
                message_count   INTEGER,
                bull_ratio      FLOAT,
                bear_ratio      FLOAT,
                watchers        INTEGER,
                collected_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            INSERT INTO stocktwits_snapshots
            (ticker, message_count, bull_ratio,
             bear_ratio, watchers)
            VALUES (%s, %s, %s, %s, %s)
        """, (ticker, result['message_count'],
              result['bull_ratio'], result['bear_ratio'],
              result['watchers']))
        self.pg.commit()

    def _log(self, ticker, status, rows, error, duration_ms):
        cur = self.pg.cursor()
        cur.execute("""
            INSERT INTO collection_log
            (ticker, source, status, rows_collected,
             error_message, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (ticker, 'stocktwits', status,
              rows, error, duration_ms))
        self.pg.commit()


