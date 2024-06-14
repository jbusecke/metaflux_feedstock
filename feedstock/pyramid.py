"""
metaflux pyramids
"""

import xarray as xr
import gcsfs
from dataclasses import dataclass
from typing import Literal
import apache_beam as beam

from pangeo_forge_recipes.transforms import (
    OpenWithXarray,
)
from pangeo_forge_ndpyramid.transforms import StoreToPyramid

from pangeo_forge_recipes.patterns import (
    pattern_from_file_sequence,
    FileType,
    FilePattern,
)
from pangeo_forge_recipes.storage import FSSpecTarget


user_path = "gs://leap-scratch/data-library/feedstocks/output/metaflux_pyramid/"

fs_target = gcsfs.GCSFileSystem()

target_root = FSSpecTarget(fs_target, user_path)


def build_file_pattern(time_step: Literal["monthly", "daily"]) -> FilePattern:
    # temp lookup dict for naming typo in source zarr store
    tmp_name_lookup = {"monthly": "meatflux", "daily": "metaflux"}
    return pattern_from_file_sequence(
        [
            f"gs://leap-persistent-ro/data-library/feedstocks/metaflux_feedstock/{tmp_name_lookup[time_step]}_{time_step}.zarr"
        ],
        concat_dim="time",
    )


@dataclass
class SelVars(beam.PTransform):
    def _selvars(self, ds: xr.Dataset) -> xr.Dataset:
        ds = ds[["GPP", "RECO"]]
        ds = ds.rename({"longitude": "lon", "latitude": "lat"})
        ds = ds.transpose("time", "lat", "lon")

        return ds

    def expand(self, pcoll):
        return pcoll | "sel" >> beam.MapTuple(lambda k, v: (k, self._selvars(v)))


# Monthly
monthly_pattern = build_file_pattern("monthly")
monthly_pyarmid = (
    beam.Create(monthly_pattern.items())
    | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
    | SelVars()
    | "Write Pyramid Levels"
    >> StoreToPyramid(
        target_root=target_root,
        store_name="metaflux_monthly_resample_dataflow.zarr",
        epsg_code="4326",
        pyramid_method="resample",
        pyramid_kwargs={"x": "lon", "y": "lat"},
        levels=3,
        combine_dims=monthly_pattern.combine_dim_keys,
    )
)

# Daily
daily_pattern = build_file_pattern("daily")
daily_pyramid = (
    beam.Create(monthly_pattern.items())
    | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
    | SelVars()
    | "Write Pyramid Levels"
    >> StoreToPyramid(
        target_root=target_root,
        store_name="metaflux_daily_resample_dataflow.zarr",
        epsg_code="4326",
        pyramid_method="resample",
        pyramid_kwargs={"x": "lon", "y": "lat"},
        levels=3,
        combine_dims=monthly_pattern.combine_dim_keys,
    )
)
