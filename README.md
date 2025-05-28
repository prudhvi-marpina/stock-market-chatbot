# Stock Market Intelligence Platform

A high-throughput data ingestion and analysis platform for stock market intelligence, featuring real-time data processing, machine learning predictions, and an AI-powered chatbot.

## 🚀 Features

- **High-Performance Data Ingestion**
  - Streams 10,000+ real-time stock data points per minute
  - Uses Alpha Vantage API for comprehensive market data
  - Efficient MongoDB storage with optimized indexing

- **Rich Data Collection**
  - Daily, weekly, and monthly price data
  - Company fundamentals and financial statements
  - Real-time quotes and trading data
  - News sentiment analysis
  - SEC filings
  - Social media sentiment

- **Intelligent Analysis**
  - LSTM-based price prediction (~65% directional accuracy)
  - AI-powered stock analysis chatbot
  - Fast retrieval using FAISS
  - Natural language understanding with LLaMA.cpp

- **User Interface**
  - Modern Qt-based interface
  - Real-time data visualization
  - Interactive query system
  - Persistent storage for user preferences

## 🛠️ Technology Stack

- **Backend**
  - Python 3.8+
  - MongoDB for data storage
  - Boost.Asio for async operations
  - cURL for data streaming
  - nlohmann/json for parsing

- **AI/ML**
  - PyTorch/TorchScript
  - LSTM models
  - FAISS for similarity search
  - LLaMA.cpp for NLP

- **Frontend**
  - Qt framework
  - Real-time charting
  - Interactive dashboards

## 📋 Prerequisites

- Python 3.8 or higher
- MongoDB 4.4 or higher
- Alpha Vantage API key
- Git
- C++ compiler (for LLaMA.cpp)

## 🔧 Installation

1. Clone the repository:
```bash
git clone https://github.com/prudhvi-marpina/stock_chatbot.git
cd stock_chatbot
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
# Create .env file with your Alpha Vantage API key
echo "ALPHA_VANTAGE_API_KEY=your_api_key_here" > .env
```

5. Initialize MongoDB:
```bash
python mongodb_setup.py
```

## 🚀 Usage

1. Start data ingestion:
```bash
python data_ingestion.py
```

2. Run the prediction engine:
```bash
python prediction_engine.py
```

3. Launch the chatbot interface:
```bash
python chatbot.py
```

## 📊 Data Structure

The platform maintains several MongoDB collections:
- `daily_prices`: Daily stock price data
- `company_overview`: Company fundamental data
- `financial_statements`: Income statements, balance sheets, cash flows
- `earnings`: Historical earnings data
- `news_sentiment`: News articles and sentiment analysis
- `social_sentiment`: Social media sentiment data
- `sec_filings`: SEC filing information
- `time_series`: Weekly and monthly historical data
- `real_time_quotes`: Real-time trading data

## 🤖 AI Components

1. **Price Prediction**
   - LSTM-based model
   - Historical price analysis
   - Technical indicator integration
   - Sentiment score incorporation

2. **Chatbot**
   - Natural language query processing
   - Context-aware responses
   - Financial data integration
   - Real-time market updates

## 📈 Performance

- Data Ingestion: 10,000+ data points/minute
- Query Response: Sub-10 second responses
- Prediction Accuracy: ~65% directional accuracy
- Database Performance: Optimized indexes for fast retrieval

## 🔒 Security

- Secure API key handling
- Rate limiting implementation
- Error handling and logging
- Data validation and sanitization

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Alpha Vantage for market data API
- MongoDB team for the database
- PyTorch community for ML tools
- Meta AI for LLaMA model 
