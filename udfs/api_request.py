import requests
import pandas as pd
import json

def load_api(
    url: str,
    method: str = "GET",
    headers: dict = None,
    params: dict = None,
    body: dict = None,
    auth: tuple = None,
    timeout: int = 30
):
    """
    Generic API loader similar to load_csv().
    Returns:
      - DataFrame (if JSON array is returned)
      - response_json
      - shape_info (rows x columns)
      - status_code
    """

    method = method.upper()
    print(f"Calling API: {method} {url}")

    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=body,
            auth=auth,
            timeout=timeout
        )

        status_code = response.status_code
        print(f"Status Code: {status_code}")

        # check response type
        print(type(response))

        response_json = response.json()

        # If the JSON is a list → convert to DataFrame
        if isinstance(response_json, list):
            df = pd.DataFrame(response_json)
            shape_info = f"{df.shape[0]} rows and {df.shape[1]} columns"
            return df, shape_info, response_json, status_code

        # If JSON is a dict containing a list
        elif isinstance(response_json, dict):
            list_items = None
            for v in response_json.values():
                if isinstance(v, list):
                    list_items = v
                    break

            if list_items:
                df = pd.DataFrame(list_items)
                shape_info = f"{df.shape[0]} rows and {df.shape[1]} columns"
                return df, shape_info, response_json, status_code

            # Only dictionary → no DataFrame
            return None, "0 rows x 0 columns", response_json, status_code

        else:
            return None, "0 rows x 0 columns", response_json, status_code

    except Exception as e:
        print(f"API Call Failed: {e}")
        return None, None, None, None

# print(load_api(url="https://api.restful-api.dev/objects", method="GET"))