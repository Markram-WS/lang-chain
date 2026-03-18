class TradingURL:
    def __init__(self):
        self.get = [
            "https://tradingeconomics.com/commodities"
            "https://tradingeconomics.com/stocks"
            "https://tradingeconomics.com/currencies"
            "https://tradingeconomics.com/crypto"
            "https://tradingeconomics.com/bonds"
        ]

    def col(self, url, x):
        if "commodities" in url:
            return self.col_commodities(x)
        elif "stocks" in url:
            return self.col_stocks(x)
        elif "currencies" in url:
            return self.col_currencies(x)
        elif "crypto" in url:
            return self.col_crypto(x)
        elif "bonds" in url:
            return self.col_bonds(x)

    def col_commodities(self, x):
        return {
            x: "product",
            "Day": "change_val",
            "%": "day_pct",
            "Price": "price",
            "Weekly": "weekly",
            "Monthly": "monthly",
            "YTD": "ytd",
            "YoY": "yoy",
            "Date": "date",
        }

    def col_stocks(self, x):
        return {
            x: "product",
            "Day": "change_val",
            "%": "day_pct",
            "Price": "price",
            "Weekly": "weekly",
            "Monthly": "monthly",
            "YTD": "ytd",
            "YoY": "yoy",
            "Date": "date",
        }

    def col_currencies(self, x):
        return {
            x: "product",
            "Day": "change_val",
            "%": "day_pct",
            "Price": "price",
            "Weekly": "weekly",
            "Monthly": "monthly",
            "YTD": "ytd",
            "YoY": "yoy",
            "MarketCap": "marketcap",
            "Date": "date",
        }

    def col_bonds(self, x):
        return {
            x: "product",
            "Yield": "yield",
            "Day": "change_val",
            "%": "day_pct",
            "Price": "price",
            "Weekly": "weekly",
            "Monthly": "monthly",
            "YTD": "ytd",
            "YoY": "yoy",
            "Date": "date",
        }
