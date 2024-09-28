import datetime
import time

import requests
import dotenv
import os
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from niels_coloredlogger.logger import logger

dotenv.load_dotenv(".env")


def main():
    response: dict[str, str] = requests.get(os.getenv("AQUASUITE_URL")).json()
    logger.info(f"Fetched {len(response)} values")
    points: list[Point] = []
    for key in response.keys():
        points.append((
            Point("aquasuite")
            .tag("location", key)
            .field("value", float(response[key]))
            .time(int(datetime.datetime.now().timestamp()), WritePrecision.S)
        ))

    write_api.write(os.getenv("BUCKET"), record=points)
    logger.info(f"Wrote {len(response)} values to influx")


if __name__ == "__main__":
    client: influxdb_client.InfluxDBClient = influxdb_client.InfluxDBClient(url=os.getenv("INFLUX_URL"), token=os.getenv("INFLUX_KEY"), org=os.getenv("ORG"))
    write_api: influxdb_client.WriteApi = client.write_api(write_options=SYNCHRONOUS)

    while True:
        try:
            main()
        except Exception as e:
            logger.error(e)

        time.sleep(10)
