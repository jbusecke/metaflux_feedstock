import xarray as xr
from dataclasses import dataclass
import apache_beam as beam

from pangeo_forge_recipes.patterns import pattern_from_file_sequence, FileType
from pangeo_forge_recipes.transforms import (
    OpenWithXarray,
)
from pangeo_forge_ndpyramid.transforms import StoreToPyramid
from leap_data_management_utils.data_management_transforms import (
    Copy,
    get_catalog_store_urls,
)


## local testing
# user_path = f"gs://leap-scratch/{os.environ['JUPYTERHUB_USER']}/pgf/"
# fs_target = gcsfs.GCSFileSystem()
# target_root = FSSpecTarget(fs_target, user_path)


catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")

pattern = pattern_from_file_sequence(
    [
        "gs://leap-persistent-ro/data-library/feedstocks/metaflux_feedstock/meatflux_monthly.zarr"
    ],
    concat_dim="time",
)


@dataclass
class SelVars(beam.PTransform):
    def _selvars(self, ds: xr.Dataset) -> xr.Dataset:
        return ds[["GPP", "RECO"]]

    def expand(self, pcoll):
        return pcoll | "sel" >> beam.MapTuple(lambda k, v: (k, self._selvars(v)))


with beam.Pipeline() as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
        | SelVars()
        | "Write Pyramid Levels"
        >> StoreToPyramid(
            # target_root=target_root,
            store_name="metaflux_monthly_3_lvl.zarr",
            epsg_code="4326",
            levels=3,
            combine_dims=pattern.combine_dim_keys,
        )
        | Copy(target=catalog_store_urls["metaflux-monthly-pyramid"])
    )
