import dateparser
import pandas as pd
from price_parser import parse_price

print(
    pd.to_datetime(
        dateparser.parse('Fri, 12 Dec 2014 10:55:50')
    )
)

print(
    pd.to_datetime(
        dateparser.parse('2022-03-03T10:10:10Z')
    )
)

print(
    parse_price("2,290", currency_hint='¥')
)
