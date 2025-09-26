#!/usr/bin/env python

import argparse
import json
import sys
import time
from concurrent import futures

import requests

noaa_url = "https://api.weather.gov"
baseurl = noaa_url


def parse_args(args):
    parser = argparse.ArgumentParser(
        prog="noaapi",
        description="API Wrapper to get Weather Data",
    )
    parser.add_argument("subject", choices=["alerts", "stations", "zones"])
    parser.add_argument("-t", "--target", type=str, required=True)
    parser.add_argument(
        "-p", "--proxy-server", type=str, required=False, default=noaa_url
    )

    return parser.parse_args(args)


def test_endpoint(endpoint):
    return requests.get(f"{baseurl}/{endpoint}")


def endpoint_to_json(endpoint, fileloc, baseurl):
    response = requests.get(f"{baseurl}/{endpoint}")
    if not response.ok:
        print(f"Problem with endpoint: {endpoint}")
    else:
        with open(f"{fileloc}/{endpoint.replace('/', '.')}.json", "w") as W:
            W.write(json.dumps(response.json(), indent="    "))


def test_rate_limit(tries=100, endpoint="alerts"):
    r = []
    for i in range(0, tries):
        r.append(requests.get(f"{baseurl}/{endpoint}"))
    return r


def handle(response, error_count=0):
    if response.status_code in (200, 204) and "features" in response.json().keys():
        return response
    else:
        print(f"Error {error_count} for requested url {response.request.url}")
        print(response.json())
        if error_count == 3:
            print(f"Error limit reached for url {response.request.url}. Skipping...")
            return None
        error_count += 1
        time.sleep(60)
        handle(
            requests.get(response.request.url, headers=response.request.headers),
            error_count,
        )


def get_observations_distributed(station_dict_list):
    print("Grabbing distributed observations")
    station_list = list(map(lambda x: x["id"], station_dict_list))
    count = 0
    num_stations = len(station_list)
    wlen = len(str(num_stations))
    result_list = []
    with futures.ThreadPoolExecutor(max_workers=5) as executor:
        for result in executor.map(get_observations, station_list):
            count += 1
            if count % 100 == 0:
                print(
                    f"Queries {count:{wlen}.0f} of {num_stations} observation stations. Percent Completion: {100.0*count/num_stations:.2f}"
                )
            result_list.append(result)
    return result_list


def get_observations(station_id):
    j = handle(requests.get(f"{baseurl}/stations/{station_id}/observations"))
    if j is None:
        return {"station_id": station_id, "observation_count": 0}
    else:
        j = j.json()["features"]
        return {
            "station_id": station_id,
            "observation_count": len(j),
            "observations": j,
        }


def paged_endpoint(endpoint):
    collection = []
    response = requests.get(f"{baseurl}/{endpoint}")
    print(f"grabbing collection for endpoint: {endpoint}")
    j = response.json()
    collection.append(j)
    total_count = 1
    error_count = 0
    while "pagination" in j.keys():
        if not "next" in j["pagination"].keys():
            break
        # time.sleep(10) # Update if there are issues
        r = requests.get(j["pagination"]["next"])
        if not r.status_code == 200 or "features" not in r.json().keys():
            error_count += 1
            print(f"Error at collection length {len(collection)}.")
            print(f"Error Content:\n{r.content}")
            if error_count == 3:
                print(
                    f"the error count hit {error_count}. Most recent endpoint was {endpoint}"
                )
                return ""
            else:
                print("Sleeping...")
                time.sleep(12)
        else:
            error_count = 0
        j = r.json()
        if len(j["features"]) == 0:
            print(
                f"returned featureless page. Returning collection of size {total_count} for endpoint {endpoint}"
            )
            return collection

        collection.append(j)
        if total_count % 100 == 0:
            print(f"Grabbed {total_count} pages from endpoint {endpoint}")
            if total_count % 10000 == 0:
                print("Hit limit. Returning collection")
                return collection
        total_count += 1
    return collection


def paged_endpoint_alt(endpoint, endpoint_label=None):
    if endpoint_label is None:
        endpoint_label = endpoint
    if endpoint == endpoint_label:
        response = requests.get(f"{baseurl}/{endpoint}")
    else:
        response = requests.get(endpoint)
    return response


def main(args):
    args = parse_args(args)
    endpoint_to_json(args.subject, args.target, args.proxy_server)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
