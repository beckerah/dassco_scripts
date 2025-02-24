import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get output folder path from .env
output_folder_path = os.getenv("output_folder_path", "./")

def fetch_literature(dataset_key):
    # Search for publications based by datasetKey and year published
    url = f"https://api.gbif.org/v1/literature/search?datasetKey={dataset_key}&year=2024"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get("results", [])  # Returns a list of publications
    else:
        print(f"Failed to fetch data for {dataset_key}: {response.status_code}")
        return []

def main(input_file, output_file):
    # Read dataset keys from spreadsheet
    input_df = pd.read_csv(input_file)
    dataset_keys = input_df["datasetKey"].dropna().unique()
    
    all_results = []
    
    # Pull data for each datasetKey
    for dataset_key in dataset_keys:
        print(f"Fetching literature for datasetKey: {dataset_key}")
        publications = fetch_literature(dataset_key)
        for pub in publications:
            all_results.append({
                "datasetKey": dataset_key,
                "title": pub.get("title", "N/A"),
                "authors": pub.get("authors", "N/A"),
                "source": pub.get("source", "N/A"),
                "year": pub.get("year", "N/A"),
                "published": pub.get("published", "N/A"),
                "doi": pub.get("doi", "N/A"),
                "websites": pub.get("websites", "N/A"),
                "abstract": pub.get("abstract", "N/A"),
                "publisher": pub.get("publisher", "N/A"),
                "publishing_country": pub.get("publishingCountry", "N/A"),
                "open_access": pub.get("openAccess", "N/A"),
                "peer_review": pub.get("peerReview", "N/A"),
                "citation_type": pub.get("citationType", "N/A"),
                "countries_of_coverage": pub.get("countriesOfCoverage", "N/A"),
                "countries_of_researcher": pub.get("countriesOfResearcher", "N/A"),
                "keywords": pub.get("keywords", "N/A"),
                "literature_type": pub.get("literatureType", "N/A"),
                "identifiers": pub.get("identifiers", "N/A"),
                "id": pub.get("id", "N/A"),
                "topics": pub.get("topics", "N/A"),
                "gbif_download_key": pub.get("gbifDownloadKey", "N/A")
            })
        time.sleep(1)  # Avoid hitting API rate limits
    
    # Convert to DataFrame
    results_df = pd.DataFrame(all_results)

    # Add publishingInstitution and datasetName to results_df
    input_df_renamed = input_df[["datasetKey", "publisher", "datasetName"]].rename(columns={"publisher": "datasetPublisher"})
    merged_df = pd.merge(input_df_renamed, results_df, on='datasetKey', how='right')

    # Additional processing needed

    # Save dataframe to CSV
    merged_df.to_csv(output_file, index=False)
    print(f"Saved results to {output_file}")

if __name__ == "__main__":
    input_file = os.path.join(output_folder_path, "all_publishers_dataset_counts.csv") # Construct input file path
    output_file = os.path.join(output_folder_path, "gbif_publications.csv")  # Construct output file path
    main(input_file, output_file)
