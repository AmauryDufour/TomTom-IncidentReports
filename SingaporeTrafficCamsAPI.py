import requests
import os
import schedule
import time
from datetime import datetime, timezone
import logging

# Define logger for module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Default logging level

# If  logger has no handlers add console handler
if not logger.hasHandlers():
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.propagate = False

def fetch_and_save_images():
    """
    Fetches traffic images from the API and saves them locally.
    """
    url = "https://api.data.gov.sg/v1/transport/traffic-images"
    params = {
        'date_time': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S%z')
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        cameras = data.get('items', [])[0].get('cameras', [])
        os.makedirs('traffic_images', exist_ok=True)

        if not cameras:
            logger.warning("No cameras data found in the API response.")
            return

        for camera in cameras:
            image_url = camera.get('image')
            camera_id = camera.get('camera_id')
            image_id = camera.get('image_id')

            if image_url:
                try:
                    image_response = requests.get(image_url, timeout=10)
                    image_response.raise_for_status()
                    filename = f"traffic_images/camera_{camera_id}_image_{image_id}.jpg"
                    with open(filename, 'wb') as file:
                        file.write(image_response.content)
                    logger.info(f"Saved image: {filename}")
                except requests.exceptions.RequestException as img_err:
                    logger.error(f"Failed to download image from {image_url}: {img_err}")
                except IOError as io_err:
                    logger.error(f"Failed to save image {filename}: {io_err}")
            else:
                logger.warning(f"No image URL found for camera ID {camera_id}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching traffic images: {e}")
    except (IndexError, KeyError, TypeError) as parse_err:
        logger.error(f"Error parsing API response: {parse_err}")

def main():
    """
    Sets up the scheduling for fetching images every minute.
    """
    # Schedule the fetch_and_save_images function to run every minute
    schedule.every(1).minutes.do(fetch_and_save_images)

    logger.info("Starting traffic image fetcher. Press Ctrl+C to exit.")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()