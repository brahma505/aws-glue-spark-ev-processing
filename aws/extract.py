import requests
import boto3
import json


def fetch_json(url):
    """Fetch JSON data from a URL."""
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()  # returns the JSON data as a dictionary
    else:
        raise Exception(
            "Failed to fetch data: {}, {}".format(response.status_code, response.text)
        )


def save_to_s3(data, bucket_name, object_name):
    """Save data to an S3 bucket."""
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=json.dumps(data))
    print("Data uploaded to {}/{}".format(bucket_name, object_name))


def main():
    url = "https://grnhse-use1-prod-s2-ghr.s3.amazonaws.com/temp_uploads/data/204/845/880/original/ElectricVehiclePopulationData.json?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVQGOLGY36AIC4MO3%2F20240510%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240510T164910Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=c3b1dde22f37c180f3ad28c173a528f651a0be34cfec690962ef0269e48a6df7"  # Replace with your actual URL
    bucket_name = "snowflake-ev-data"  # Replace with your S3 bucket name
    object_name = "raw_data/data.json"  # Name of the file as it will appear in S3

    try:
        # Fetch data from the endpoint
        json_data = fetch_json(url)

        # Save data to S3
        save_to_s3(json_data, bucket_name, object_name)
    except Exception as e:
        print("An error occurred:{}".format(str(e)))


main()
if __name__ == "__main__":
    main()
