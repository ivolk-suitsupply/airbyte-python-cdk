
#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from pydantic.v1 import BaseModel, Field

from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig


class JsonlFormat(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Jsonl Format"
        discriminator = "filetype"

    skip_rows_before_payload: int = Field(
        title="Skip Rows Before Payload",
        default=0,
        description="The number of rows to skip before the payload. For example, if the payload starts on the 3rd row, enter 2 in this field.",
    )

    convert_from_xml: bool = Field(
        title="Convert From XML",
        default=False,
        description="Whether to convert from XML to JSON. If true, the XML will be converted to JSON before being parsed.",
    )

    filetype: str = Field(
        "jsonl",
        const=True,
    )
