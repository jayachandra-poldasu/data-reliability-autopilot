import csv
import random
import os

if not os.path.exists('data'):
    os.makedirs('data')

header = ['id', 'name', 'amount', 'timestamp']
rows = []

# Generate 50 rows of data
for i in range(1, 51):
    name = random.choice(['Alice', 'Bob', 'Charlie', 'Diana', 'Edward', 'Fiona'])
    timestamp = f"2026-04-{random.randint(10, 20)}"
    
    # Mix in different types of "Bad" data
    if i % 10 == 0:
        amount = "MISSING"         # Text instead of number
    elif i % 15 == 0:
        amount = "NULL"            # String 'NULL'
    elif i % 7 == 0:
        amount = "1,250.50"        # Number with a comma (breaks some parsers)
    elif i == 42:
        amount = "$-99"            # Currency symbol and negative
    else:
        amount = str(random.randint(100, 5000))
        
    rows.append([i, name, amount, timestamp])

with open('data/raw_data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

print("✅ data/raw_data.csv generated with 50 rows of realistic, messy data!")