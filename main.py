import click

from app import (
    get_x_api_key,
    get_outages,
    get_site_info,
    filter_by_column,
    filter_by_another_json,
    create_df,
    df_join,
    df_to_json,
    post_outages,
)


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

    print(f"Posted data: {data}")

    post_outages(site_id=site_id, data=data, headers=headers)


@click.command()
@click.option("--site-id", default="norwich-pear-tree", help="Site id of site info")
def cli(site_id: str):
    run(site_id=site_id)


if __name__ == "__main__":
    cli()
