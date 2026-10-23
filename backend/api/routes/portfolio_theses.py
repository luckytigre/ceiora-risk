"""Session-authenticated portfolio classification routes on the serve surface."""
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from backend.api.auth import require_authenticated_session
from backend.services import portfolio_thesis_service as service

router = APIRouter()
Quantity = Annotated[Decimal, Field(max_digits=20, decimal_places=6, allow_inf_nan=False)]


class RevisionRequest(BaseModel):
    expected_revision: int = Field(ge=0, strict=True)


class NameRequest(RevisionRequest):
    name: str = Field(min_length=1, max_length=80)


class Allocation(BaseModel):
    thesis_id: UUID
    quantity: Quantity


class AllocationsRequest(RevisionRequest):
    account_id: str = Field(min_length=2, max_length=64)
    ric: str = Field(min_length=1, max_length=128)
    allocations: list[Allocation] = Field(max_length=1000)


class OrderRequest(RevisionRequest):
    scope: str = Field(min_length=1, max_length=120)
    keys: list[Annotated[str, Field(max_length=256)]] = Field(max_length=10000)


def _request(token, operation, payload=None):
    principal = require_authenticated_session(x_app_session_token=token)
    try:
        return service.request_for_principal(principal=principal, operation=operation, payload=payload)
    except service.AccountScopeDenied as exc:
        raise HTTPException(status_code=403, detail="Account is not authorized for theses.") from exc
    except service.AccountScopeProvisioningError as exc:
        raise HTTPException(status_code=409, detail="Account provisioning is required.") from exc
    except service.ThesisError as exc:
        raise HTTPException(status_code=exc.status, detail={"code": exc.code, "message": str(exc)}) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail={"code": "theses_unavailable", "message": "Thesis workspace is temporarily unavailable."}) from exc


@router.get("/portfolio/theses/capability")
def capability(x_app_session_token: str | None = Header(default=None)):
    return _request(x_app_session_token, "capability")


@router.get("/portfolio/theses")
def get_state(x_app_session_token: str | None = Header(default=None)):
    return _request(x_app_session_token, "read")


@router.post("/portfolio/theses", status_code=201)
def create(payload: NameRequest, x_app_session_token: str | None = Header(default=None)):
    return _request(x_app_session_token, "create", payload.model_dump())


@router.patch("/portfolio/theses/{thesis_id}")
def rename(thesis_id: UUID, payload: NameRequest, x_app_session_token: str | None = Header(default=None)):
    return _request(x_app_session_token, "rename", {**payload.model_dump(), "thesis_id": thesis_id})


@router.delete("/portfolio/theses/{thesis_id}")
def delete(thesis_id: UUID, payload: RevisionRequest, x_app_session_token: str | None = Header(default=None)):
    return _request(x_app_session_token, "delete", {**payload.model_dump(), "thesis_id": thesis_id})


@router.put("/portfolio/thesis-allocations/position")
def allocate(payload: AllocationsRequest, x_app_session_token: str | None = Header(default=None)):
    return _request(x_app_session_token, "allocate", payload.model_dump())


@router.put("/portfolio/manual-order")
def order(payload: OrderRequest, x_app_session_token: str | None = Header(default=None)):
    return _request(x_app_session_token, "order", payload.model_dump())
