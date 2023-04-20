# KrakenFlex Back End Test

## Introduction

The goal of that project is that write a program which get the all outages and site information from relative endpoints and
filtering them according to requirements to find outage devices informations for sites. 
After that, post this prepared data to the relative endpoint.

## Getting Started

To use this project, you will need to do the following:

- Firstly, after cloning repo from git, set the virtual environment to install and use necessary dependencies.

      python -m venv venvname

- After that, you need to activate that virtual environment by using command on below.

      .\venvname\Scripts\activate

- After that, you need to install all dependencies that are using in that project
from requirements.txt

      pip install -r requirements.txt

- After these are done, you need to assign a environment variable for X_API_KEY before run the program

    If you are using windows then

      $Env:X_API_KEY = 'YOUR_API_KEY'

    for Mac/linux

      export X_API_KEY=YOUR_API_KEY

- After you set your virtual environment,dependencies, and your environment variable which is YOUR_API_KEY. you can run the main or/and test files.
  To run the files, you can write following commands

      python test.py 

      python main.py

- When you are running the main.py, you can run with site-id parameter, default value is 'norwich-pear-tree'.
  You can change that default parameter by using following command

                    python main.py --site-id parametername 
      --> example : python main.py --site-id norwich-pear-tree

- You can use help parameter to see the running parameters that you can use.

      python main.py --help 



## Project Files

The project is organized into the following files.
- app.py -> contains functions 
- main.py -> main file to run the program
- test.py -> contains tests of functions

## main.py

```python
def run(site_id: str):
    x_api_key = get_x_api_key()
    headers = {"X-API-Key": x_api_key}

    outages = get_outages(headers=headers)

    site_info = get_site_info(site_id=site_id, headers=headers)

    filtered_data = filter_by_column(
        data=outages,
        column="begin",
        value="2022-01-01T00:00:00.000Z",
        op=">=",
    ) 

    filter_outages_id = filter_by_another_json(
        site_info,
        "devices",
        "id",
        filtered_data,
    ) 

    df_outages = create_df(filter_outages_id)
    df_site_devices = create_df(site_info["devices"])

    final_site_outages_df = df_join(
        df_outages,
        df_site_devices,
        "id",
        "inner",
        ["id", "begin"],
    )  

    data = df_to_json(final_site_outages_df) 

    print(f"Posted data: {data}") --> site-outage-data that we create

    post_outages(site_id=site_id, data=data, headers=headers) --> post the site-outage data that we create


@click.command()
@click.option("--site-id", default="norwich-pear-tree", help="Site id of site info")
def cli(site_id: str):
    run(site_id=site_id)


if __name__ == "__main__":
    cli()


```

In main.py we use our function to get the outages and site-info and transforming them according to business requirements and posting them into relative endpoint. The steps are in below:

- Get the X-API-KEY which is environment variable by using `get_x_api_key()` function.
- Put x-api-key into headers, to use it when send get/post request `headers = {"X-API-Key": x_api_key}`
- Call `get_outages(headers)` function to get the outages from relative endpoint url.
- Call `get_site_info(site_id,headers)` to get site info with relative site_id
- Filter outages data according to begin column 
`filter_by_column(
        data=outages,
        column="begin",
        value="2022-01-01T00:00:00.000Z",
        op=">=" )`
- Filter outages data according to site devices id information(site_info[devices][id]) 
`filter_outages_id = filter_by_another_json(
        site_info,
        "devices",
        "id",
        filtered_data
    )` 
- Create dataframes for filtered outages and site devices information(site_info[devices])

    `df_outages = create_df(filter_outages_id)`

    `df_site_devices = create_df(site_info["devices"])`

- Join outages and site devices information according to `id` key and sorting them according to `id and begin` columns

- Convert the site-outage dataframe to post the data `df_to_json(final_site_outages_df)`

- Post the site-outage data that we create `post_outages(site_id=site_id, data=data, headers=headers)`  

Click was used to give site-id parameter when running the program, you can go back to getting started section to see how parameters are given to the py file.

## app.py
In app.py, you can see the functions and their explanations that we used on main.py to get outage and site info and posting them.

## test.py
In test.py, there are unittest to test functions which are into app.py.

In order to have knowledge about how run the files properly, you can go back to getting started section !

