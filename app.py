import requests
import pandas as pd
import json
import os
from typing import List, Any, Union
import operator
import time
import logging

_API = "https://api.krakenflex.systems/interview-tests-mock-api/v1"


def get_x_api_key() -> str:
    """
    Get the value of X_API_KEY environment variable.

    Returns:
        The value of X_API_KEY environment variable as a string.

    Raises:
        ValueError: If X_API_KEY environment variable is not set.
    """
    x_api_key = os.environ.get("X_API_KEY")

    if not x_api_key:
        raise ValueError(
            "X_API_KEY is not provided, please set it as an environment variable"
        )

    return x_api_key


def get(
    endpoint: str = None,
    headers={},
    max_retry=3,
    wait_time_in_seconds=3,
    **kwargs,
) -> requests.Response:
    """
    Sends an HTTP GET request to the specified endpoint and returns the server's response.

    Args:
        endpoint (str): The endpoint to send the GET request to.
        headers (dict): A dictionary of headers to include in the request.
        max_retry (int): The max number of times to retry the request
        wait_time_in_seconds (int): wait time between retries.
        **kwargs: Additional args

    Returns:
        requests.Response: The server's response to the GET request.

    Raises:
        ValueError: If `endpoint` is not provided.
         Exception: If the request fails with a status code of 400, 403, 404 or all retry attempts are exhausted.

    """

    if not endpoint:
        raise ValueError("Please provide an endpoint")

    url = f"{_API}/{endpoint}"

    try:
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()
        logging.basicConfig(level=logging.WARNING)

        if response.status_code == 200:
            return response

    except:
        if max_retry <= 0:
            return

        max_retry -= 1

        if response.status_code in (429, 500):
            logger = logging.getLogger(f" Status code {response.status_code}")

            logger.warning(
                f"Request failed,The request will be send again after {wait_time_in_seconds}s. Remaining trial count is {max_retry}"
            )

            time.sleep(wait_time_in_seconds)
            return get(
                endpoint=endpoint,
                headers=headers,
                max_retry=max_retry,
                wait_time_in_seconds=wait_time_in_seconds,
                **kwargs,
            )

        if response.status_code in (400, 403, 404):
            mapping_status_code = {
                400: "We cannot process your request because it doesn't match the required format.",
                403: "You do not have the required permissions to make this request. Please set your api key as an env variable or check your Apikey is correct.",
                404: "You have requested a resource that does not exist. Pls check your endpoint url.",
            }

            raise Exception(
                f"Error: Request failed with status code {response.status_code}, {mapping_status_code[response.status_code]}."
            )


def parse_json(text: str = None) -> Union[list, dict]:
    """
    Parse a JSON string and return into a dict or list.

    Args:
        text (str): A JSON-formatted string to parse.

    Returns:
        The resulting dict or list object.

    Raises:
        ValueError: If the `text` argument is `None`.
    """
    if not text:
        raise ValueError("Text must be provided")

    return json.loads(text)


def get_outages(headers={}) -> List[dict]:
    """
    Fetches a list of outages from a REST API.

    Args:
        headers (dict): A dictionary of headers to include in the request.

    Returns:
        A list of outages and their information(id,begin,end).

    Raises:
        Exception: If the request fails or returns an unexpected response status code.
    """
    endpoint = "outages"
    r = get(endpoint=endpoint, headers=headers)
    outages = parse_json(r.text)

    return outages


def get_site_info(site_id: str = None, headers={}) -> dict:
    """
    Retrieve information about a site by its ID.

    Args:
        site_id (str): The ID of the site to retrieve information about.
        headers (dict): Optional HTTP headers to include in the request.

    Returns:
        A dictionary containing information about the requested site.

    Raises:
        ValueError: If `site_id` is not provided.
        Exception: If the request fails or the response contains an error message.
    """
    if not site_id:
        raise ValueError("Site id must be provided")

    endpoint = f"site-info/{site_id}"
    r = get(endpoint=endpoint, headers=headers)
    site_info = parse_json(r.text)

    return site_info


def create_df(data) -> pd.DataFrame:
    """
    Convert iterable objects to a pandas DataFrame.

    Args:
        data  The data to be converted.

    Returns:
        A pandas DataFrame with the data.

    Raises:
        TypeError: If `data` is not iterable.
    """
    return pd.DataFrame(data)


def filter_by_column(data: List[dict], column: str, value: Any, op: str):
    """
    Filters a list of dictionaries by a given column, value and comparison operator.

    Args:
        data (List[dict]): data to filter
        column (str): filter column
        value (Any): The value to compare against.
        op (str): The comparison operator to use. Allowed values are: ">", ">=", "<=", "<", "=", "in".

    Returns:
        A list of dictionaries where the value of the specified column satisfies the comparison criteria.

    Raises:
        ValueError: If the comparison operator is not allowed.
    """
    mapping = {
        ">": operator.gt,
        ">=": operator.ge,
        "<=": operator.le,
        "<": operator.lt,
        "=": operator.eq,
    }

    if op == "in":
        return [row for row in data if row[column] in value]

    if op not in mapping.keys():
        raise ValueError("Operation is not allowed")

    op = mapping[op]
    return [row for row in data if op(row[column], value)]


def filter_by_another_json(
    first_data: Union[dict, List[dict]],
    first_filter_column_name: str,
    second_filter_column_name: str,
    data2: List[dict],
):
    """
        Filter a list of dicts based on the values in another list of dicts.

        Args:
            first_data (List[Dict]): The first list of dictionaries to filter.
            first_filter_column_name (str): The name of the column to filter on in the first list of dictionaries.
            second_filter_column_name (str): The name of the column to filter on in the second list of dictionaries.
            data2 (List[Dict]): The second list of dictionaries to filter.

        Returns:
            List[Dict]: The filtered list of dictionaries.

        Raises:
            ValueError: If any of the required arguments are `None`.

        Example usage:
        ```
        site_devices=site_info[devices]
        site_devices_id=site['id'] for site in site_devices
        outages for outage['id'] in site_devices_id
    ```
    """
    filtered_data = first_data[first_filter_column_name]
    second_filtered_data = [row[second_filter_column_name] for row in filtered_data]
    return filter_by_column(
        data2, second_filter_column_name, second_filtered_data, "in"
    )


def df_join(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    key: str,
    type: str,
    sort_columns: list,
):
    """
    Joins two pandas dataframes on a specified key and returns the resulting dataframe.

    Args:
        df1 (pd.DataFrame): The first df to be joined
        df2 (pd.DataFrame): The second df to be joined.
        key (str): column for join
        type (str): The type of join to perform. Can be one of "inner", "outer", "left", or "right".
        sort_columns (List[str]): A list of column names to sort

    Returns:
        joined pandas df

    Raises:
        TypeError: If `df1` or `df2` is not a df.
        ValueError: If `key` is not a column in both df, or if `type` is not a valid join type.
    """
    df1 = df1.set_index(key)
    df2 = df2.set_index(key)
    final = df2.join(df1, how=type).reset_index()
    sorted_final = final.sort_values(by=sort_columns)
    return sorted_final


def df_to_json(data: pd.DataFrame) -> List[dict]:
    """
    Convert a pandas df to a list[dict]

    Args:
        data (pd.DataFrame): The DataFrame to be converted to JSON.

    Returns:
        list of dict version of the given dataframe

    """
    sorted_json = data.to_json(orient="records")
    final_json = json.loads(sorted_json)
    return final_json


def post(
    endpoint: str = None,
    data: Union[list, dict] = None,
    headers={},
    max_retry=3,
    wait_time_in_seconds=3,
    **kwargs,
) -> requests.Response:
    """
    Sends a POST request to the specified endpoint with the given data and headers.

    Args:
        endpoint (str): The endpoint to which the post request should be sent.
        data (Union[list, dict]): The data to be sent in the request body.
        headers (dict): The headers to be sent along with the request.
        max_retry (int): The max number of times to retry the request.
        wait_time_in_seconds (int):wait time between retries.
        **kwargs: Any additional args.

    Returns:
        requests.Response: The response from the API.

    Raises:
        ValueError: If the endpoint(siteid info) or data field is not provided.
        Exception: If the request fails with a status code of 400, 403, 404 or all retry attempts are exhausted.
    """
    if not endpoint:
        raise ValueError("Please provide an endpoint")

    if not data:
        raise ValueError("data field cannot be empty")

    url = f"{_API}/{endpoint}"

    try:
        response = requests.post(url=url, headers=headers, json=data)
        response.raise_for_status()
        logging.basicConfig(level=logging.WARNING)

        if response.status_code == 200:
            print(
                f"The post request finished with status code {str(response.status_code)} successfully"
            )
            return response

    except:
        if max_retry <= 0:
            return

        max_retry -= 1

        if response.status_code in (429, 500):
            logger = logging.getLogger(f" Status code {response.status_code}")
            logger.warning(
                f"Request failed,The request will be send again after {wait_time_in_seconds}s. Remaining trial count is {max_retry}"
            )

            time.sleep(wait_time_in_seconds)
            return post(
                endpoint=endpoint,
                data=data,
                headers=headers,
                max_retry=max_retry,
                wait_time_in_seconds=wait_time_in_seconds,
                **kwargs,
            )

        if response.status_code in (400, 403, 404):
            mapping_status_code = {
                400: "We cannot process your request because it doesn't match the required format.",
                403: "You do not have the required permissions to make this request. Please set your api key as an env variable or check your Apikey is correct.",
                404: "You have requested a resource that does not exist. Pls check your endpoint url.",
            }

            raise Exception(
                f"Error: Request failed with status code {response.status_code}, {mapping_status_code[response.status_code]}."
            )


def post_outages(site_id: str, data: dict, headers={}) -> List[dict]:
    """
    Send a POST request to create a new outage for a site specified by site_id.

    Args:
    - site_id (str): The ID of the site for which the outage is being created.
    - data (dict): The data for the new outage to be created.
    - headers (dict, optional): Any additional headers to be included in the request. Default is an empty dictionary.

    Returns:
    - List[dict]: The response from the API as a list of dictionaries.

    Raises:
    - ValueError: If site_id or data are not provided.

    Example usage:
    ```
    site_id = "norwich-pear-tree"
    data = [{'id': '0e4d59ba-43c7-4451-a8ac-ca628bcde417', 'name': 'Battery 6', 'begin': '2022-02-15T11:28:26.735Z', 'end': '2022-08-28T03:37:48.568Z'},
    {'id': '111183e7-fb90-436b-9951-63392b36bdd2', 'name': 'Battery 1', 'begin': '2022-01-01T00:00:00.000Z', 'end': '2022-09-15T19:45:10.341Z'}]
    headers = {"X-API-KEY": "myapikey123"}
    response = post_outages(site_id, data, headers)
    ```


    """
    endpoint = f"site-outages/{site_id}"

    if not site_id:
        raise ValueError("site_id must be given")

    if not data:
        raise ValueError("Data must be given")

    r = post(endpoint=endpoint, data=data, headers=headers)

    return r
