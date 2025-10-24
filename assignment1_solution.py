#Task 1 Solution

# Stock details
stock_name = "TCS"
purchase_price = 3050
current_price = 3080
quantity = 16


# Calculate profit/loss for the stock
stock_profit_loss = (current_price - purchase_price) * quantity
stock_profit_loss_percent = ((current_price - purchase_price) / purchase_price) * 100


# Portfolio details
total_investment = 50000
current_portfolio_value = 48800


# Calculate profit/loss for the portfolio
portfolio_profit_loss = current_portfolio_value - total_investment
portfolio_profit_loss_percent = (portfolio_profit_loss / total_investment) * 100


# Print results
print(f"\nStock: {stock_name}")
print(f"Profit/Loss Amount: Rs. {stock_profit_loss}")
print(f"Profit/Loss Percentage: {stock_profit_loss_percent:.2f}%")
print(f"\nPortfolio:")
print(f"Total Investment: Rs. {total_investment}")
print(f"Current Value: Rs. {current_portfolio_value}")
print(f"Profit/Loss Amount: Rs. {portfolio_profit_loss}")
print(f"Profit/Loss Percentage: {portfolio_profit_loss_percent:.2f}%")

# Task 2 Solution

# Part 1: Variables of different data types
company_name = "TCS"  # String
number_of_shares = 16  # Integer
stock_price = 3080.0  # Float
is_profitable = False  # Boolean (since it's a loss)

# Part 2: Print the type of each variable
print("\nVariable Types:")
print(f"Type of company_name: {type(company_name)}")
print(f"Type of number_of_shares: {type(number_of_shares)}")
print(f"Type of stock_price: {type(stock_price)}")
print(f"Type of is_profitable: {type(is_profitable)}")

# Part 3: Demonstrate type conversion
stock_price_as_int = int(stock_price)
print("\nType Conversion:")
print(f"Stock price as integer: {stock_price_as_int} (Type: {type(stock_price_as_int)})")

# --- Task 3: String Operations ---

# 1. Create a stock ticker string using the company_name from Task 1/2
stock_ticker = company_name + ".NS"

# 2. Extract just the stock symbol (before the dot)
stock_symbol = stock_ticker.split(".")[0]

# 3. Create a formatted string that displays: "Stock: TCS, Price: ₹3080.25"
formatted_string = f"Stock: {company_name}, Price: 3080.25"

# 4. Convert the stock name to uppercase and lowercase
stock_upper = company_name.upper()
stock_lower = company_name.lower()

# Print results for Task 3
print("\n--- Task 3: String Operations ---")
print(f"Stock ticker: {stock_ticker}")
print(f"Stock symbol: {stock_symbol}")
print(formatted_string)
print(f"Uppercase: {stock_upper}")
print(f"Lowercase: {stock_lower}")

