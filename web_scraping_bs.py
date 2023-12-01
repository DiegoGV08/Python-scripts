# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

headers_list = []
rows_list = []

# Navigate through the pages from 1 to 1996
for i in range(1, 2356):
    url = f'https://bintable.com/scheme/MASTERCARD?page={i}'

    # Make an HTTP request to the web page
    response = requests.get(url)

    # Verify that the request was successful
    if response.status_code == 200:

        # Analyze the content of the page with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the table with the specific class
        table = soup.find('table', {'class': 'mt-4 table table-sm table-striped table-hover'})

        # Find the table headers on the first page
        if i == 1:
            for th in table.find('thead').findAll('th'):
                headers_list.append(th.text.strip())

        # Find the rows of the table
        for tr in table.find('tbody').findAll('tr'):
            row = []
            for td in tr.findAll('td'):
                row.append(td.text.strip())
            rows_list.append(row)
    else:
        print(f'Error al obtener la página {i}: {response.status_code}')

    # Add a pause of 1 second between each request
    time.sleep(1)

# Create a dataframe with headers and rows
df = pd.DataFrame(rows_list, columns=headers_list)

# Display the dataframe
print(df)

# Export the dataframe to an Excel file
df.to_excel('output.xlsx', index=False)

print("The process has completed successfully, and the file 'output.xlsx' has been created.")

