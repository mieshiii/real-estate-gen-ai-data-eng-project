#draft scraper

import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_website(url):
    # Send an HTTP request to the URL
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all the div elements with class "row ListingCell-row ListingCell-agent-redesign"
        listings = soup.find_all('div', class_='row ListingCell-row ListingCell-agent-redesign')
        
        # Initialize lists to store data
        prices = []
        categories = []
        subcategories = []
        year_builts = []
        furnished = []
        bedrooms = []
        bathrooms = []
        building_sizes = []
        land_sizes = []
        subdivision_names = []
        skus = []
        geo_points = []
        new_developments = []

        # Iterate through each listing
        for listing in listings:
            # Extract data attributes
            data = listing.find('div', class_='ListingCell-AllInfo ListingUnit')
            prices.append(data['data-price'])
            categories.append(data['data-category'])
            subcategories.append(eval(data['data-subcategories'])) # Convert string to list
            year_builts.append(data['data-year_built'])
            furnished.append(data['data-furnished'])
            bedrooms.append(data['data-bedrooms'])
            bathrooms.append(data['data-bathrooms'])
            building_sizes.append(data['data-building_size'])
            land_sizes.append(data['data-land_size'])
            subdivision_names.append(data['data-subdivisionname'])
            skus.append(data['data-sku'])
            geo_points.append(eval(data['data-geo-point'])) # Convert string to list
            new_developments.append(data['data-listing-new-development'])

        # Create a DataFrame
        df = pd.DataFrame({
            'Price': prices,
            'Category': categories,
            'Subcategory': subcategories,
            'Year Built': year_builts,
            'Furnished': furnished,
            'Bedrooms': bedrooms,
            'Bathrooms': bathrooms,
            'Building Size': building_sizes,
            'Land Size': land_sizes,
            'Subdivision Name': subdivision_names,
            'SKU': skus,
            'Geo Point': geo_points,
            'New Development': new_developments
        })

        # Display the DataFrame
        print(df)

    else:
        print("Failed to fetch the webpage.")

# Example usage
url = 'YOUR_URL_HERE'
scrape_website(url)
