import pytest
from unittest.mock import patch
from eventCascadeBuilder_proprosed import process_all_obsreward_files

def test_dispatch_to_an_builder():
    with patch("eventParser_AN.buildEvents_AN", return_value=["mock"]) as mock_an:
        process_all_obsreward_files(
            dataDir="mock/path",
            metadata="mock/meta.xlsx",
            allowed_statuses=["complete"],
            subDirs=["pair_008"],
            role="AN"
        )
        mock_an.assert_called_once()

def test_dispatch_to_po_builder():
    with patch("eventParser_PO.buildEvents_PO", return_value=["mock"]) as mock_po:
        process_all_obsreward_files(
            dataDir="mock/path",
            metadata="mock/meta.xlsx",
            allowed_statuses=["complete"],
            subDirs=["pair_009"],
            role="PO"
        )
        mock_po.assert_called_once()
