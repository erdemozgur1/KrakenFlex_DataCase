import unittest
from app import (
    get,
    get_site_info,
    filter_by_column,
    _API,
    get_x_api_key,
    get_outages,
    parse_json,
    df_join,
    df_to_json,
    create_df,
    post,
    post_outages,
)
import requests
import json
import requests_mock
import pandas as pd


class testapp(unittest.TestCase):
    maxDiff = None

    def test_get_raises_value_error_when_endpoint_not_provided(self):
        with self.assertRaises(ValueError):
            r = get()

    def test_parse_json_valid_input(self):
        input_data = '{"id": "0101e9d3-ab78-408a-b54f-2a4b88efe048","begin": "2022-11-21T12:05:03.195Z","end": "2022-11-30T18:22:19.422Z"}'

        expected_data = {
            "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
            "begin": "2022-11-21T12:05:03.195Z",
            "end": "2022-11-30T18:22:19.422Z",
        }

        self.assertEqual(parse_json(input_data), expected_data)

    def test_parse_json_empty_input(self):
        with self.assertRaises(ValueError):
            parse_json()

    def test_parse_json_invalid_input(self):
        invalid_json_str = "this is not a valid json string"
        with self.assertRaises(json.JSONDecodeError):
            parse_json(invalid_json_str)

    def test_get(self):
        endpoint = "outages"
        url = f"{_API}/{endpoint}"

        actual = get(endpoint=endpoint, headers={"X-API-Key": get_x_api_key()})
        self.assertEqual(actual.status_code, 200, msg="Status code is not 200.")

    def test_get_outages(self):
        endpoint = "outages"
        url = f"{_API}/{endpoint}"

        expected = requests.get(url=url, headers={"X-API-Key": get_x_api_key()}).text
        actual = get_outages(headers={"X-API-Key": get_x_api_key()})

        expected_data = parse_json(expected)

        sorted_actual_data = actual.sort(
            key=lambda outage: (outage["id"], outage["begin"])
        )
        sorted_expected_data = expected_data.sort(
            key=lambda outage: (outage["id"], outage["begin"])
        )
        self.assertEqual(
            sorted_actual_data, sorted_expected_data, msg="Texts are not match"
        )

    def test_get_raises_value_error_when_status_code_is_429_or_500(self):
        with requests_mock.Mocker() as m:
            # Mock a 500,429 error for the first 4 requests then 200 for success
            m.get(
                "https://api.krakenflex.systems/interview-tests-mock-api/v1/outages",
                [
                    {"status_code": 500},
                    {"status_code": 429},
                    {"status_code": 500},
                    {"status_code": 429},
                    {"status_code": 200},
                ],
            )

            # Call the function with retry parameters
            response = get(
                endpoint="outages",
                max_retry=6,
                wait_time_in_seconds=3,
            )

            self.assertEqual(response.status_code, 200, msg="Status code is not 200.")
            assert m.call_count == 5

    def test_get_raises_value_error_when_status_code_is_400_403_404(self):
        with requests_mock.Mocker() as m:
            # Mock a 404 error
            m.get(
                "https://api.krakenflex.systems/interview-tests-mock-api/v1/outages",
                status_code=404,
            )

        with self.assertRaises(Exception) as e:
            # Call the function with the endpoint that causes a 404 error
            response = get(endpoint="example1")
            self.assertEqual(
                str(e.exception),
                f"Error: Request failed with status code 400, We cannot process your request because it doesn't match the required format.",
            )

    def test_get_site_info_raises_value_error_when_site_id_not_provided(self):
        with self.assertRaises(ValueError):
            site_info = get_site_info()

    def test_get_site_info_request_fails_for_wrong_site_id_or_and_api_key(self):
        self.site_id = "1234"
        self.headers = {"X-API-KEY": "1234567890abcdef"}
        with self.assertRaises(Exception):
            get_site_info(site_id=self.site_id, headers=self.headers)

    def test_filter_by_column_ge(self):
        input_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]

        filtered_value = "2022-07-05T08:15:39.279Z"
        filtered_op = ">="
        filtered_column_name = "begin"

        expected_output_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
        ]
        actual_output_data = filter_by_column(
            input_data, filtered_column_name, filtered_value, filtered_op
        )
        self.assertEqual(
            actual_output_data,
            expected_output_data,
            msg="Expected output data and actual output data are not match",
        )

    def test_filter_by_column_lt(self):
        input_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]

        filtered_value = "2022-01-01T00:00:00.000Z"
        filtered_op = "<"
        filtered_column_name = "begin"

        expected_output_data = [
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            }
        ]
        actual_output_data = filter_by_column(
            input_data, filtered_column_name, filtered_value, filtered_op
        )
        self.assertEqual(
            actual_output_data,
            expected_output_data,
            msg="Expected output data and actual output data are not match",
        )

    def test_filter_by_column_in(self):
        input_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]

        filtered_value = "2022-11-21T12:05:03.195Z"
        filtered_op = "in"
        filtered_column_name = "begin"

        expected_output_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            }
        ]
        actual_output_data = filter_by_column(
            input_data, filtered_column_name, filtered_value, filtered_op
        )
        self.assertEqual(
            actual_output_data,
            expected_output_data,
            msg="Expected output data and actual output data are not match",
        )

    def test_filter_by_column_le(self):
        input_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]

        filtered_value = "2022-07-05T08:15:39.279Z"
        filtered_op = "<="
        filtered_column_name = "begin"

        expected_output_data = [
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]
        actual_output_data = filter_by_column(
            input_data, filtered_column_name, filtered_value, filtered_op
        )
        self.assertEqual(
            actual_output_data,
            expected_output_data,
            msg="Expected output data and actual output data are not match",
        )

    def test_filter_by_column_bt(self):
        input_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]

        filtered_value = "2022-07-05T08:15:39.279Z"
        filtered_op = ">"
        filtered_column_name = "begin"

        expected_output_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            }
        ]
        actual_output_data = filter_by_column(
            input_data, filtered_column_name, filtered_value, filtered_op
        )
        self.assertEqual(
            actual_output_data,
            expected_output_data,
            msg="Expected output data and actual output data are not match",
        )

    def test_filter_by_column_raises_value_error_when_unknown_op_provided(self):
        input_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]

        filtered_value = "2022-11-21T12:05:03.195Z"
        filtered_op = "invailid"
        filtered_column_name = "begin"
        with self.assertRaises(ValueError):
            filtered = filter_by_column(
                data=input_data,
                column=filtered_column_name,
                value=filtered_value,
                op=filtered_op,
            )

    def test_filter_by_column_raises_key_error_when_key_is_not_in_row(self):
        input_data = [
            {
                "id": "0101e9d3-ab78-408a-b54f-2a4b88efe048",
                "begin": "2022-11-21T12:05:03.195Z",
                "end": "2022-11-30T18:22:19.422Z",
            },
            {
                "id": "05e353d8-96f2-4906-bc91-0b869b9a4a6c",
                "begin": "2022-07-05T08:15:39.279Z",
                "end": "2022-08-16T18:09:54.458Z",
            },
            {
                "id": "0b4a44f2-3f7f-4a62-b3e3-56d487b5900b",
                "begin": "2021-03-21T15:03:47.019Z",
                "end": "2022-01-18T14:24:44.651Z",
            },
        ]

        filtered_value = "2022-11-21T12:05:03.195Z"
        filtered_op = ">="
        filtered_column_name = "invalid"
        with self.assertRaises(KeyError):
            filtered = filter_by_column(
                data=input_data,
                column=filtered_column_name,
                value=filtered_value,
                op=filtered_op,
            )

    def test_df_join(self):
        df1 = pd.DataFrame({"id": [1, 2, 3], "begin": ["A", "B", "C"]})
        df2 = pd.DataFrame({"id": [1, 2, 5], "name": ["D", "E", "F"]})

        key = "id"
        join_type = "inner"
        sort_columns = ["id"]

        result = df_join(df1, df2, key, join_type, sort_columns)

        expected = pd.DataFrame(
            {
                "id": [1, 2],
                "name": [
                    "D",
                    "E",
                ],
                "begin": [
                    "A",
                    "B",
                ],
            }
        )

        pd.testing.assert_frame_equal(
            result, expected
        )  # in order to compare the df, we use  pd.testing.assert_frame_equal function.

    def test_create_df(self):
        # Test case 1: valid input
        data = [
            {
                "id": "0e4d59ba-43c7-4451-a8ac-ca628bcde417",
                "begin": "2022-02-15T11:28:26.735Z",
                "end": "2022-08-28T03:37:48.568Z",
            },
            {
                "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                "begin": "2022-01-01T00:00:00.000Z",
                "end": "2022-09-15T19:45:10.341Z",
            },
        ]
        expected_df = pd.DataFrame(data)

        pd.testing.assert_frame_equal(
            create_df(data), expected_df
        )  # in order to compare the df, we use  pd.testing.assert_frame_equal function.

        # # Test case 2: empty input
        data = []
        expected_df = pd.DataFrame()
        pd.testing.assert_frame_equal(create_df(data), expected_df)

    def test_df_to_json(self):
        # Create test data
        data = pd.DataFrame(
            [
                {
                    "id": "0e4d59ba-43c7-4451-a8ac-ca628bcde417",
                    "name": "Battery 6",
                    "begin": "2022-02-15T11:28:26.735Z",
                    "end": "2022-08-28T03:37:48.568Z",
                },
                {
                    "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                    "name": "Battery 1",
                    "begin": "2022-01-01T00:00:00.000Z",
                    "end": "2022-09-15T19:45:10.341Z",
                },
            ]
        )

        # Call the function and check the output
        result = df_to_json(data)
        expected_result = json.loads(data.to_json(orient="records"))
        self.assertEqual(
            result,
            expected_result,
            msg="Expected output data and actual output data are not match",
        )

        # Test with empty dataframe
        data = pd.DataFrame()
        result = df_to_json(data)
        expected_result = []
        self.assertEqual(
            result,
            expected_result,
            msg="Expected output data and actual output data are not match",
        )

    def test_post(self):
        siteid = "norwich-pear-tree"
        endpoint = f"site-outages/{siteid}"
        data = [
            {
                "id": "0e4d59ba-43c7-4451-a8ac-ca628bcde417",
                "name": "Battery 6",
                "begin": "2022-02-15T11:28:26.735Z",
                "end": "2022-08-28T03:37:48.568Z",
            },
            {
                "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                "name": "Battery 1",
                "begin": "2022-01-01T00:00:00.000Z",
                "end": "2022-09-15T19:45:10.341Z",
            },
            {
                "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                "name": "Battery 1",
                "begin": "2022-02-18T01:01:20.142Z",
                "end": "2022-08-15T14:34:50.366Z",
            },
            {
                "id": "20f6e664-f00e-4621-9ca4-5ec588aadeaf",
                "name": "Battery 7",
                "begin": "2022-02-15T11:28:26.965Z",
                "end": "2023-12-24T14:20:37.532Z",
            },
            {
                "id": "70656668-571e-49fa-be2e-099c67d136ab",
                "name": "Battery 3",
                "begin": "2022-04-08T16:29:22.128Z",
                "end": "2022-06-09T22:10:59.718Z",
            },
            {
                "id": "75e96db4-bba2-4035-8f43-df2cbd3da859",
                "name": "Battery 8",
                "begin": "2023-05-11T14:35:15.359Z",
                "end": "2023-12-27T11:19:19.393Z",
            },
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2",
                "begin": "2022-02-16T07:01:50.149Z",
                "end": "2022-10-03T07:46:31.410Z",
            },
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2",
                "begin": "2022-05-09T04:47:25.211Z",
                "end": "2022-12-02T18:37:16.039Z",
            },
            {
                "id": "9ed11921-1c5b-40f4-be66-adb4e2f016bd",
                "name": "Battery 4",
                "begin": "2022-01-12T08:11:21.333Z",
                "end": "2022-12-13T07:20:57.984Z",
            },
            {
                "id": "a79fe094-087b-4b1e-ae20-ac4bf7fa429b",
                "name": "Battery 5",
                "begin": "2022-02-23T11:33:58.552Z",
                "end": "2022-12-16T00:52:16.126Z",
            },
        ]
        headers = {"X-API-Key": get_x_api_key()}

        response = post(endpoint=endpoint, data=data, headers=headers)

        # Check the response
        assert response.status_code == 200

    def test_post_outages(self):
        siteid = "norwich-pear-tree"
        data = [
            {
                "id": "0e4d59ba-43c7-4451-a8ac-ca628bcde417",
                "name": "Battery 6",
                "begin": "2022-02-15T11:28:26.735Z",
                "end": "2022-08-28T03:37:48.568Z",
            },
            {
                "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                "name": "Battery 1",
                "begin": "2022-01-01T00:00:00.000Z",
                "end": "2022-09-15T19:45:10.341Z",
            },
            {
                "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                "name": "Battery 1",
                "begin": "2022-02-18T01:01:20.142Z",
                "end": "2022-08-15T14:34:50.366Z",
            },
            {
                "id": "20f6e664-f00e-4621-9ca4-5ec588aadeaf",
                "name": "Battery 7",
                "begin": "2022-02-15T11:28:26.965Z",
                "end": "2023-12-24T14:20:37.532Z",
            },
            {
                "id": "70656668-571e-49fa-be2e-099c67d136ab",
                "name": "Battery 3",
                "begin": "2022-04-08T16:29:22.128Z",
                "end": "2022-06-09T22:10:59.718Z",
            },
            {
                "id": "75e96db4-bba2-4035-8f43-df2cbd3da859",
                "name": "Battery 8",
                "begin": "2023-05-11T14:35:15.359Z",
                "end": "2023-12-27T11:19:19.393Z",
            },
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2",
                "begin": "2022-02-16T07:01:50.149Z",
                "end": "2022-10-03T07:46:31.410Z",
            },
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2",
                "begin": "2022-05-09T04:47:25.211Z",
                "end": "2022-12-02T18:37:16.039Z",
            },
            {
                "id": "9ed11921-1c5b-40f4-be66-adb4e2f016bd",
                "name": "Battery 4",
                "begin": "2022-01-12T08:11:21.333Z",
                "end": "2022-12-13T07:20:57.984Z",
            },
            {
                "id": "a79fe094-087b-4b1e-ae20-ac4bf7fa429b",
                "name": "Battery 5",
                "begin": "2022-02-23T11:33:58.552Z",
                "end": "2022-12-16T00:52:16.126Z",
            },
        ]
        headers = {"X-API-Key": get_x_api_key()}

        response = post_outages(site_id=siteid, data=data, headers=headers)

        # Check the response
        assert response.status_code == 200


if __name__ == "__main__":
    """To run the py directly"""
    unittest.main()
