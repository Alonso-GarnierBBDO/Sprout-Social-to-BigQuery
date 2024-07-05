# Sending Sprout Social Data to BigQuery

This Node.js script fetches data from Sprout Social and loads it into BigQuery.

## Configuration

Before running the script, make sure to configure the following variables in the `index.js` file:

- `bearer_token`: Sprout Social authentication token.
- `dataset_id`: ID of the BigQuery dataset where data will be stored.
- `table_id`: ID of the specific table in BigQuery where data will be inserted.
- `key_file_path`: Path to the BigQuery Service Account JSON file.

## Fetching Data from the Previous Day

The script is set to automatically retrieve data from the previous day.

## Usage Example

1. Clone this repository.
2. Install dependencies with `npm install`.
3. Ensure Node.js is installed.
4. Run the script with `node index.js`.

## Dependencies

- `@google-cloud/bigquery`: To interact with BigQuery from Node.js.
- `fs`: For file system operations in Node.js.
- `node-fetch`: For making HTTP requests in Node.js.

## Useful Links

- [Sprout Social API Documentation](https://api.sproutsocial.com/docs/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs/)

## Notes

This script was developed for a specific project and may require additional adjustments based on your needs.
