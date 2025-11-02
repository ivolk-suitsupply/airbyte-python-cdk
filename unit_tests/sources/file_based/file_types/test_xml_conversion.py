import io
import json

from airbyte_cdk.sources.file_based.file_types import JsonlParser


def test_xml_conversion_strips_namespaces_and_preserves_attributes() -> None:
    xml = (
        '<ns:root xmlns:ns="urn:x">'
        '  <ns:parent attr="v">'
        '    <ns:child attribute-id="A">TEXT</ns:child>'
        '    <ns:child attribute-id="B">123</ns:child>'
        '    <ns:single attribute-id="C" />'
        '    <ns:value>42</ns:value>'
        '  </ns:parent>'
        '</ns:root>'
    )

    parser = JsonlParser()
    converted = parser._convert_xml_content(io.BytesIO(xml.encode("utf-8")))
    data = json.loads(converted.read().decode("utf-8"))

    assert "root" in data
    parent = data["root"]["parent"]
    # parent attribute kept and namespaced stripped
    assert parent["@attr"] == "v"
    # repeated children become list with attributes and #text preserved
    assert isinstance(parent["child"], list)
    assert parent["child"][0]["@attribute-id"] == "A"
    assert parent["child"][0]["#text"] == "TEXT"
    assert parent["child"][1]["@attribute-id"] == "B"
    assert parent["child"][1]["#text"] == "123"
    # single child remains object with attributes
    assert parent["single"]["@attribute-id"] == "C"
    # pure text child without attributes collapses to string
    assert parent["value"] == "42"


def test_xml_conversion_removes_parlevelupdates_version() -> None:
    xml = '<ParLevelUpdates version="1.0"><Item id="1"/></ParLevelUpdates>'
    parser = JsonlParser()
    converted = parser._convert_xml_content(io.BytesIO(xml.encode("utf-8")))
    data = json.loads(converted.read().decode("utf-8"))

    assert "ParLevelUpdates" in data
    plu = data["ParLevelUpdates"]
    # @version should be removed
    assert "@version" not in plu
    assert plu["Item"]["@id"] == "1"


