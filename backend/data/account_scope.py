"""Account scope resolution for app-authenticated holdings access."""

from __future__ import annotations

from dataclasses import dataclass, replace

from backend import config
from backend.api.auth import AppPrincipal
from backend.data import app_identity


class AccountScopeError(RuntimeError):
    """Base error for account-scoping failures."""


class AccountScopeAuthRequired(AccountScopeError):
    """Raised when account enforcement is enabled but no principal is present."""


class AccountScopeProvisioningError(AccountScopeError):
    """Raised when a principal cannot be mapped into app account memberships."""


class AccountScopeBootstrapDisabled(AccountScopeProvisioningError):
    """Raised when Neon login works but automatic personal-account bootstrap is off."""


class AccountScopeDenied(AccountScopeError):
    """Raised when a principal asks for an account outside its allowed scope."""


@dataclass(frozen=True)
class AccountScope:
    enforced: bool
    is_admin: bool
    subject: str | None
    default_account_id: str | None
    account_ids: tuple[str, ...]


def account_enforcement_enabled() -> bool:
    return bool(config.APP_ACCOUNT_ENFORCEMENT_ENABLED)


def shared_auth_legacy_allowed() -> bool:
    return bool(config.APP_SHARED_AUTH_ACCEPT_LEGACY)


def _normalize_email(value: str | None) -> str | None:
    clean = str(value or "").strip().lower()
    return clean or None


def _allowed_neon_emails() -> set[str]:
    return {value for value in config.NEON_AUTH_ALLOWED_EMAILS if value}


def _admin_neon_emails() -> set[str]:
    return {value for value in config.NEON_AUTH_BOOTSTRAP_ADMINS if value}


def resolve_effective_principal(pg_conn, *, principal: AppPrincipal | None) -> AppPrincipal:
    if principal is None:
        raise AccountScopeAuthRequired("Authenticated app principal required for account-scoped holdings access.")

    if principal.provider == "shared" and not shared_auth_legacy_allowed():
        raise AccountScopeDenied(
            "Shared sessions are not allowed to access account-scoped holdings while Neon account enforcement is enabled."
        )

    if principal.provider != "neon":
        return principal

    if pg_conn is None:
        return principal

    neon_user = app_identity.load_neon_auth_user(pg_conn, auth_user_id=principal.subject)
    canonical_email = _normalize_email(neon_user.email if neon_user else None) or _normalize_email(principal.email)
    if neon_user is None and not canonical_email:
        raise AccountScopeProvisioningError(
            f"Neon user '{principal.subject}' is not yet available for Ceiora account bootstrap."
        )

    allowed_emails = _allowed_neon_emails()
    if allowed_emails and canonical_email not in allowed_emails:
        raise AccountScopeDenied(
            "This Neon account is not allowlisted for Ceiora access."
        )

    is_admin = bool(principal.is_admin)
    if canonical_email and canonical_email in _admin_neon_emails():
        is_admin = True

    return replace(
        principal,
        email=canonical_email,
        display_name=(neon_user.display_name if neon_user else None) or principal.display_name,
        is_admin=is_admin,
    )


def _normalize_account_ids(rows: list[app_identity.MembershipRow]) -> tuple[str | None, tuple[str, ...]]:
    default_account_id: str | None = None
    ordered: list[str] = []
    seen: set[str] = set()
    for row in rows:
        account_id = str(row.account_id or "").strip().lower()
        if not account_id:
            continue
        if account_id not in seen:
            ordered.append(account_id)
            seen.add(account_id)
        if bool(row.is_default) and default_account_id is None:
            default_account_id = account_id
    if default_account_id is None and ordered:
        default_account_id = ordered[0]
    return default_account_id, tuple(ordered)


def resolve_account_scope(pg_conn, *, principal: AppPrincipal | None) -> AccountScope:
    if not account_enforcement_enabled():
        return AccountScope(
            enforced=False,
            is_admin=bool(principal and principal.is_admin),
            subject=(principal.subject if principal else None),
            default_account_id=None,
            account_ids=(),
        )

    principal = resolve_effective_principal(pg_conn, principal=principal)

    rows = app_identity.load_membership_rows(pg_conn, principal=principal)
    if not rows and principal.provider == "neon":
        if app_identity.bootstrap_personal_account(pg_conn, principal=principal):
            rows = app_identity.load_membership_rows(pg_conn, principal=principal)
        elif not app_identity.auth_bootstrap_enabled():
            raise AccountScopeBootstrapDisabled(
                f"No account memberships found for principal '{principal.subject}'; automatic personal workspace bootstrap is disabled."
            )

    default_account_id, account_ids = _normalize_account_ids(rows)
    if not account_ids:
        raise AccountScopeProvisioningError(
            f"No account memberships found for principal '{principal.subject}'."
        )

    return AccountScope(
        enforced=True,
        is_admin=bool(principal.is_admin),
        subject=principal.subject,
        default_account_id=default_account_id,
        account_ids=account_ids,
    )


def validate_requested_account(scope: AccountScope, requested_account_id: str | None) -> str | None:
    requested = str(requested_account_id or "").strip().lower() or None
    if not scope.enforced:
        return requested
    if requested is None:
        return None
    if requested not in scope.account_ids:
        raise AccountScopeDenied(f"Account '{requested}' is outside the authenticated scope.")
    return requested
