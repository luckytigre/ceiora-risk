from backend.risk_model.model_status import derive_model_status


def test_model_status_derivation() -> None:
    assert derive_model_status(is_core_regression_member=True, is_projectable=True) == "core_estimated"
    assert derive_model_status(is_core_regression_member=False, is_projectable=True) == "projected_only"
    assert derive_model_status(is_core_regression_member=False, is_projectable=False) == "ineligible"
