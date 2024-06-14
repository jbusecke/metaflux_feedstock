"""
MetaFlux is a global, long-term carbon flux dataset of gross
primary production and ecosystem respiration that is generated
using meta-learning. This dataset will be added to the existing
rodeo forecast model in order to improve its performances."
"""

import os
import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
)
from leap_data_management_utils.data_management_transforms import (
    Copy,
    InjectAttrs,
    get_catalog_store_urls,
)

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")

# if not run in a github workflow, assume local testing and deactivate the copy stage by setting all urls to False (see https://github.com/leap-stc/leap-data-management-utils/blob/b5762a17cbfc9b5036e1cd78d62c4e6a50c9691a/leap_data_management_utils/data_management_transforms.py#L121-L145)
if os.getenv("GITHUB_ACTIONS") == "true":
    print("Running inside GitHub Actions.")
else:
    print("Running locally. Deactivating final copy stage.")
    catalog_store_urls = {k: False for k in catalog_store_urls.keys()}

print("Final output locations")
print(f"{catalog_store_urls=}")

# Common Parameters
years = range(2001, 2022)
months = range(1, 13)
dataset_url = "https://zenodo.org/record/7761881/files"

## Monthly version
input_urls_monthly = [f"{dataset_url}/METAFLUX_GPP_RECO_monthly_{y}.nc" for y in years]

pattern_monthly = pattern_from_file_sequence(input_urls_monthly, concat_dim="time")
METAFLUX_GPP_RECO_monthly = (
    beam.Create(pattern_monthly.items())
    | OpenURLWithFSSpec(max_concurrency=1)
    | OpenWithXarray()
    | StoreToZarr(
        store_name="METAFLUX_GPP_RECO_monthly.zarr",
        combine_dims=pattern_monthly.combine_dim_keys,
    )
    | InjectAttrs()
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["metaflux-monthly"])
)

## daily version
input_urls_daily = [
    f"{dataset_url}/METAFLUX_GPP_RECO_daily_{y}{m:02}.nc" for y in years for m in months
]
pattern_daily = pattern_from_file_sequence(input_urls_daily, concat_dim="time")
METAFLUX_GPP_RECO_daily = (
    beam.Create(pattern_daily.items())
    | OpenURLWithFSSpec(max_concurrency=1)
    | OpenWithXarray()
    | StoreToZarr(
        store_name="METAFLUX_GPP_RECO_daily.zarr",
        combine_dims=pattern_daily.combine_dim_keys,
    )
    | InjectAttrs()
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["metaflux-daily"])
)
