"""
News Aggregator Data Pipeline
Demonstrates ETL process: Extract -> Transform -> Load
Scrapes news articles, processes with NLP, and stores in structured format
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import json
import re
from collections import Counter
import time
from urllib.parse import urlparse
import hashlib

class NewsAggregatorPipeline:
    """
    Complete data pipeline for news aggregation and processing
    """
    
    def __init__(self):
        self.raw_articles = []
        self.processed_articles = []
        self.categories = {
            'technology': ['ai', 'software', 'tech', 'computer', 'data', 'cloud', 'cyber'],
            'business': ['economy', 'market', 'finance', 'stock', 'trade', 'company'],
            'politics': ['election', 'government', 'policy', 'congress', 'president', 'vote'],
            'science': ['research', 'study', 'scientist', 'discovery', 'space', 'climate'],
            'health': ['medical', 'health', 'disease', 'patient', 'doctor', 'vaccine'],
            'sports': ['game', 'team', 'player', 'championship', 'score', 'league']
        }
        
    # ==================== EXTRACTION PHASE ====================
    
    def extract_from_rss(self, rss_urls):
        """
        Extract articles from RSS feeds
        """
        print("📥 EXTRACTION PHASE: Fetching from RSS feeds...")
        
        for url in rss_urls:
            try:
                response = requests.get(url, timeout=10)
                soup = BeautifulSoup(response.content, 'xml')
                
                items = soup.find_all('item')
                for item in items:
                    article = {
                        'title': item.find('title').text if item.find('title') else 'No Title',
                        'description': item.find('description').text if item.find('description') else '',
                        'link': item.find('link').text if item.find('link') else '',
                        'pub_date': item.find('pubDate').text if item.find('pubDate') else '',
                        'source': urlparse(url).netloc,
                        'extracted_at': datetime.now().isoformat()
                    }
                    self.raw_articles.append(article)
                    
                print(f"  ✓ Extracted {len(items)} articles from {urlparse(url).netloc}")
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"  ✗ Error fetching {url}: {str(e)}")
        
        print(f"📊 Total articles extracted: {len(self.raw_articles)}\n")
        return self.raw_articles
    
    def extract_from_websites(self, websites):
        """
        Scrape articles directly from websites
        """
        print("📥 EXTRACTION PHASE: Scraping websites...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for site in websites:
            try:
                response = requests.get(site['url'], headers=headers, timeout=10)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Generic article extraction (customize per site)
                articles = soup.find_all(site.get('article_tag', 'article'), 
                                        class_=site.get('article_class'))
                
                for article in articles[:10]:  # Limit to 10 per site
                    title_tag = article.find(['h1', 'h2', 'h3'])
                    link_tag = article.find('a')
                    
                    if title_tag and link_tag:
                        self.raw_articles.append({
                            'title': title_tag.text.strip(),
                            'description': article.get_text()[:200],
                            'link': link_tag.get('href', ''),
                            'source': site['name'],
                            'extracted_at': datetime.now().isoformat()
                        })
                
                print(f"  ✓ Scraped {site['name']}")
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                print(f"  ✗ Error scraping {site['name']}: {str(e)}")
        
        return self.raw_articles
    
    # ==================== TRANSFORMATION PHASE ====================
    
    def clean_text(self, text):
        """
        Clean and normalize text data
        """
        if not text:
            return ""
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove special characters
        text = re.sub(r'[^\w\s\.]', ' ', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def extract_keywords(self, text, top_n=5):
        """
        Extract top keywords from text
        """
        # Simple keyword extraction (in production, use NLP libraries like spaCy or NLTK)
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
                     'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
                     'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
                     'should', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those'}
        
        words = re.findall(r'\b[a-z]{3,}\b', text.lower())
        words = [w for w in words if w not in stop_words]
        
        word_freq = Counter(words)
        return [word for word, _ in word_freq.most_common(top_n)]
    
    def categorize_article(self, text):
        """
        Categorize article based on keywords
        """
        text_lower = text.lower()
        scores = {}
        
        for category, keywords in self.categories.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            scores[category] = score
        
        # Return category with highest score, or 'general' if no match
        max_category = max(scores, key=scores.get)
        return max_category if scores[max_category] > 0 else 'general'
    
    def calculate_sentiment(self, text):
        """
        Simple sentiment analysis (positive/negative/neutral)
        In production, use libraries like VADER or TextBlob
        """
        positive_words = ['good', 'great', 'excellent', 'amazing', 'wonderful', 'fantastic',
                         'success', 'win', 'growth', 'improve', 'innovation']
        negative_words = ['bad', 'terrible', 'awful', 'crisis', 'fail', 'loss', 'decline',
                         'problem', 'issue', 'concern', 'risk', 'threat']
        
        text_lower = text.lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        return 'neutral'
    
    def remove_duplicates(self):
        """
        Remove duplicate articles based on title similarity
        """
        seen_hashes = set()
        unique_articles = []
        
        for article in self.raw_articles:
            # Create hash of normalized title
            title_hash = hashlib.md5(
                article['title'].lower().strip().encode()
            ).hexdigest()
            
            if title_hash not in seen_hashes:
                seen_hashes.add(title_hash)
                unique_articles.append(article)
        
        removed_count = len(self.raw_articles) - len(unique_articles)
        print(f"  ✓ Removed {removed_count} duplicate articles")
        self.raw_articles = unique_articles
    
    def transform_data(self):
        """
        Transform and enrich raw article data
        """
        print("🔄 TRANSFORMATION PHASE: Processing articles...")
        
        # Remove duplicates first
        self.remove_duplicates()
        
        for article in self.raw_articles:
            # Clean text
            clean_title = self.clean_text(article['title'])
            clean_desc = self.clean_text(article.get('description', ''))
            combined_text = f"{clean_title} {clean_desc}"
            
            # Process article
            processed = {
                'id': hashlib.md5(article['link'].encode()).hexdigest()[:12],
                'title': clean_title,
                'description': clean_desc[:300],  # Limit description length
                'url': article['link'],
                'source': article['source'],
                'category': self.categorize_article(combined_text),
                'keywords': self.extract_keywords(combined_text),
                'sentiment': self.calculate_sentiment(combined_text),
                'word_count': len(combined_text.split()),
                'published_date': article.get('pub_date', ''),
                'processed_at': datetime.now().isoformat()
            }
            
            self.processed_articles.append(processed)
        
        print(f"  ✓ Processed {len(self.processed_articles)} articles")
        print(f"  ✓ Categorized into: {set(a['category'] for a in self.processed_articles)}\n")
        
        return self.processed_articles
    
    # ==================== LOADING PHASE ====================
    
    def load_to_json(self, filename='news_data.json'):
        """
        Save processed data to JSON file
        """
        print(f"💾 LOADING PHASE: Saving to {filename}...")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.processed_articles, f, indent=2, ensure_ascii=False)
        
        print(f"  ✓ Saved {len(self.processed_articles)} articles to JSON\n")
    
    def load_to_csv(self, filename='news_data.csv'):
        """
        Save processed data to CSV file
        """
        print(f"💾 LOADING PHASE: Saving to {filename}...")
        
        df = pd.DataFrame(self.processed_articles)
        df['keywords'] = df['keywords'].apply(lambda x: ', '.join(x))
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"  ✓ Saved {len(df)} articles to CSV\n")
    
    def load_to_database(self, db_path='news.db'):
        """
        Save processed data to SQLite database
        """
        import sqlite3
        
        print(f"💾 LOADING PHASE: Saving to database {db_path}...")
        
        conn = sqlite3.connect(db_path)
        df = pd.DataFrame(self.processed_articles)
        df['keywords'] = df['keywords'].apply(lambda x: ', '.join(x))
        
        df.to_sql('articles', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"  ✓ Saved {len(df)} articles to database\n")
    
    # ==================== ANALYSIS & REPORTING ====================
    
    def generate_report(self):
        """
        Generate summary report of processed data
        """
        print("📊 GENERATING REPORT...")
        print("=" * 60)
        
        df = pd.DataFrame(self.processed_articles)
        
        print(f"\nTotal Articles Processed: {len(df)}")
        print(f"Date Range: {datetime.now().strftime('%Y-%m-%d')}")
        
        print("\n📰 Articles by Category:")
        category_counts = df['category'].value_counts()
        for category, count in category_counts.items():
            print(f"  {category.capitalize()}: {count}")
        
        print("\n📰 Articles by Source:")
        source_counts = df['source'].value_counts().head(5)
        for source, count in source_counts.items():
            print(f"  {source}: {count}")
        
        print("\n💭 Sentiment Distribution:")
        sentiment_counts = df['sentiment'].value_counts()
        for sentiment, count in sentiment_counts.items():
            print(f"  {sentiment.capitalize()}: {count}")
        
        print("\n🔥 Top Keywords:")
        all_keywords = [kw for keywords in df['keywords'] for kw in keywords]
        top_keywords = Counter(all_keywords).most_common(10)
        for keyword, count in top_keywords:
            print(f"  {keyword}: {count}")
        
        print("\n" + "=" * 60)
    
    # ==================== MAIN PIPELINE ====================
    
    def run_pipeline(self, data_sources):
        """
        Execute complete ETL pipeline
        """
        print("\n" + "=" * 60)
        print("🚀 STARTING NEWS AGGREGATOR PIPELINE")
        print("=" * 60 + "\n")
        
        start_time = time.time()
        
        # Extract
        if 'rss_feeds' in data_sources:
            self.extract_from_rss(data_sources['rss_feeds'])
        
        if 'websites' in data_sources:
            self.extract_from_websites(data_sources['websites'])
        
        # Transform
        if self.raw_articles:
            self.transform_data()
            
            # Load
            self.load_to_json()
            self.load_to_csv()
            self.load_to_database()
            
            # Report
            self.generate_report()
        else:
            print("⚠️  No articles extracted. Check your data sources.")
        
        elapsed_time = time.time() - start_time
        print(f"\n✅ Pipeline completed in {elapsed_time:.2f} seconds")
        print("=" * 60 + "\n")


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":
    # Initialize pipeline
    pipeline = NewsAggregatorPipeline()
    
    # Define data sources
    data_sources = {
        'rss_feeds': [
            'http://rss.cnn.com/rss/cnn_topstories.rss',
            'http://feeds.bbci.co.uk/news/rss.xml',
            'https://www.theguardian.com/world/rss',
        ],
        'websites': [
            {
                'name': 'TechCrunch',
                'url': 'https://techcrunch.com',
                'article_tag': 'article',
                'article_class': 'post-block'
            }
        ]
    }
    
    # Run the complete pipeline
    pipeline.run_pipeline(data_sources)
    
    # Access processed data
    print("Sample processed article:")
    if pipeline.processed_articles:
        print(json.dumps(pipeline.processed_articles[0], indent=2))