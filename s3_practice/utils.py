def get_datalake_bucket_name(layer, company, region, account, env):
    return f"{company}-{layer}-{region}-{account}-{env}"


def get_datalake_raw_layer_path(
        source, source_region,
        table, year=None, month=None, day=None, hour=None):
    path = f"{source}/{source_region}/{table}"

    if year:
        path += f"/year={year}"
    if month:
        path += f"/month={str(month).zfill(2)}"
    if day:
        path += f"/day={str(day).zfill(2)}"
    if hour:
        path += f"/hour={str(hour).zfill(2)}"

    return path
