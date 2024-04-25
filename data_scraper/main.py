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
            prices.append(data.get('data-price', 'N/A'))
            categories.append(data.get('data-category', 'N/A'))
            subcategories.append(eval(data.get('data-subcategories', '[]'))) # Convert string to list
            year_builts.append(data.get('data-year_built', 'N/A'))
            furnished.append(data.get('data-furnished', 'N/A'))
            bedrooms.append(data.get('data-bedrooms', 'N/A'))
            bathrooms.append(data.get('data-bathrooms', 'N/A'))
            building_sizes.append(data.get('data-building_size', 'N/A'))
            land_sizes.append(data.get('data-land_size', 'N/A'))
            subdivision_names.append(data.get('data-subdivisionname', 'N/A'))
            skus.append(data.get('data-sku', 'N/A'))
            geo_points.append(eval(data.get('data-geo-point', '[N/A, N/A]'))) # Convert string to list
            new_developments.append(data.get('data-listing-new-development', 'false'))

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
        return df

    else:
        print("Failed to fetch the webpage.")