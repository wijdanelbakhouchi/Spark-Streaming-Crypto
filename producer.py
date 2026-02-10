import time
import json
import requests
from kafka import KafkaProducer
from datetime import datetime
from textblob import TextBlob 

# Kafka Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("🚀 Smart Producer Started...")

while True:
    try:
        # 1. Bitcoin Price
        btc_resp = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd", timeout=5)
        price = btc_resp.json()['bitcoin']['usd']

        # 2. News Fetching
        # Using a broader query to ensure we get headlines for sentiment
        try:
            news_resp = requests.get("https://api.gdeltproject.org/api/v2/doc/doc?query=bitcoin&mode=ArtList&maxrecords=5&format=json", timeout=5)
            articles = news_resp.json().get('articles', [])
        except:
            articles = []

        # 3. Sentiment Analysis
        sentiment_sum = 0
        for article in articles:
            title = article.get('title', '')
            # Polarity: -1 (Negative) to +1 (Positive)
            blob = TextBlob(title)
            sentiment_sum += blob.sentiment.polarity
        
        avg_sentiment = sentiment_sum / len(articles) if articles else 0

        # 4. Payload
        payload = {
            "timestamp": datetime.now().isoformat(),
            "btc_price": float(price),
            "news_count": len(articles),
            "sentiment_score": float(avg_sentiment) # Crucial for correlation
        }

        producer.send('crypto-topic', value=payload)
        print(f"✅ Sent: Price=${price} | Sentiment={avg_sentiment:.2f}")

    except Exception as e:
        print(f"⚠️ Error: {e}")

    time.sleep(10)