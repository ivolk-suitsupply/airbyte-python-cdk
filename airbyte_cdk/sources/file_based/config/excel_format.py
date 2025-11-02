#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from pydantic.v1 import BaseModel, Field

from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig


class ExcelFormat(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Excel Format"
        discriminator = "filetype"

    skip_rows_before_header: int = Field(
        title="Skip Rows Before Header",
        default=0,
        description="The number of rows to skip before the header row. For example, if the header row is on the 3rd row, enter 2 in this field.",
    )        

    filetype: str = Field(
        "excel",
        const=True,
    )
