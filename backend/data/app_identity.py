"""App-user identity and bootstrap helpers for Neon-backed auth."""

from __future__ import annotations

from dataclasses import dataclass
import uuid

from backend.api.auth import AppPrincipal
from backend import config


@dataclass(frozen=True)
class MembershipRow:
    account_id: str
    is_default: bool


@dataclass(frozen=True)
class NeonAuthUserRow:
    auth_user_id: str
    email: str | None
    display_name: str | None
    role: str | None


def auth_bootstrap_enabled() -> bool:
    return bool(config.APP_AUTH_BOOTSTRAP_ENABLED)


def bootstrap_reuses_existing_membership() -> bool:
    return bool(config.APP_AUTH_BOOTSTRAP_REUSE_EXISTING_MEMBERSHIP)


def _normalize_email(value: str | None) -> str | None:
    clean = str(value or "").strip().lower()
    return clean or None


def _display_name(value: str | None, *, fallback: str) -> str:
    clean = str(value or "").strip()
    return clean or fallback


def _personal_account_name(principal: AppPrincipal) -> str:
    fallback = principal.email or principal.subject
    display_name = _display_name(principal.display_name, fallback=fallback)
    return f"{display_name} portfolio"


def _personal_account_id() -> str:
    return f"acct_{uuid.uuid4().hex[:12]}"


def load_neon_auth_user(pg_conn, *, auth_user_id: str) -> NeonAuthUserRow | None:
    clean_user_id = str(auth_user_id or "").strip()
    if not clean_user_id:
        return None
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, email, name, role
              FROM neon_auth.user
             WHERE id::text = %s
             LIMIT 1
            """,
            (clean_user_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    auth_id, email, display_name, role = row
    return NeonAuthUserRow(
        auth_user_id=str(auth_id or "").strip(),
        email=_normalize_email(email),
        display_name=str(display_name or "").strip() or None,
        role=str(role or "").strip().lower() or None,
    )


def load_membership_rows(pg_conn, *, principal: AppPrincipal) -> list[MembershipRow]:
    with pg_conn.cursor() as cur:
        if principal.provider == "shared":
            cur.execute(
                """
                SELECT m.account_id, m.is_default
                FROM app_users u
                JOIN account_memberships m
                  ON m.user_id = u.user_id
                WHERE u.auth_user_id = %s
                ORDER BY m.is_default DESC, m.created_at ASC, m.account_id ASC
                """,
                (principal.subject,),
            )
        else:
            cur.execute(
                """
                SELECT m.account_id, m.is_default
                FROM app_users u
                JOIN account_memberships m
                  ON m.user_id = u.user_id
                WHERE u.auth_user_id = %s
                ORDER BY m.is_default DESC, m.created_at ASC, m.account_id ASC
                """,
                (principal.subject,),
            )
        return [
            MembershipRow(account_id=str(account_id), is_default=bool(is_default))
            for account_id, is_default in cur.fetchall()
        ]


def bootstrap_personal_account(pg_conn, *, principal: AppPrincipal) -> bool:
    if principal.provider != "neon" or not auth_bootstrap_enabled():
        return False

    email = _normalize_email(principal.email)
    previous_autocommit = getattr(pg_conn, "autocommit", None)
    if previous_autocommit:
        pg_conn.autocommit = False

    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_users (
                    user_id,
                    auth_user_id,
                    email,
                    display_name,
                    is_admin,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (auth_user_id) DO UPDATE
                SET
                    email = COALESCE(EXCLUDED.email, app_users.email),
                    display_name = COALESCE(EXCLUDED.display_name, app_users.display_name),
                    is_admin = (app_users.is_admin OR EXCLUDED.is_admin),
                    updated_at = NOW()
                RETURNING user_id
                """,
                (
                    str(uuid.uuid4()),
                    principal.subject,
                    email,
                    principal.display_name,
                    bool(principal.is_admin),
                ),
            )
            (user_id,) = cur.fetchone()
            cur.execute(
                """
                SELECT default_account_id
                  FROM app_users
                 WHERE user_id = %s
                 FOR UPDATE
                """,
                (user_id,),
            )
            (default_account_id,) = cur.fetchone()
            if default_account_id:
                pg_conn.commit()
                return False

            cur.execute(
                """
                SELECT account_id
                  FROM account_memberships
                 WHERE user_id = %s
                 ORDER BY is_default DESC, created_at ASC, account_id ASC
                 LIMIT 1
                """,
                (user_id,),
            )
            existing_membership = cur.fetchone()
            if existing_membership and bootstrap_reuses_existing_membership():
                account_id = str(existing_membership[0] or "").strip() or None
                if account_id:
                    cur.execute(
                        """
                        UPDATE account_memberships
                           SET is_default = CASE WHEN account_id = %s THEN TRUE ELSE FALSE END,
                               updated_at = NOW()
                         WHERE user_id = %s
                        """,
                        (account_id, user_id),
                    )
                    cur.execute(
                        """
                        UPDATE app_users
                           SET default_account_id = %s,
                               updated_at = NOW()
                         WHERE user_id = %s
                        """,
                        (account_id, user_id),
                    )
                    pg_conn.commit()
                    return False

            account_id = _personal_account_id()
            cur.execute(
                """
                INSERT INTO holdings_accounts (
                    account_id,
                    account_name,
                    account_type,
                    is_active,
                    created_at,
                    updated_at,
                    created_by_user_id,
                    default_owner_user_id
                ) VALUES (%s, %s, 'personal', TRUE, NOW(), NOW(), %s, %s)
                ON CONFLICT (account_id) DO NOTHING
                """,
                (account_id, _personal_account_name(principal), user_id, user_id),
            )
            cur.execute(
                """
                INSERT INTO account_memberships (
                    membership_id,
                    account_id,
                    user_id,
                    role,
                    is_default,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, 'owner', TRUE, NOW(), NOW())
                ON CONFLICT (account_id, user_id) DO UPDATE
                SET
                    role = EXCLUDED.role,
                    is_default = TRUE,
                    updated_at = NOW()
                """,
                (str(uuid.uuid4()), account_id, user_id),
            )
            cur.execute(
                """
                UPDATE account_memberships
                   SET is_default = CASE WHEN account_id = %s THEN TRUE ELSE FALSE END,
                       updated_at = NOW()
                 WHERE user_id = %s
                """,
                (account_id, user_id),
            )
            cur.execute(
                """
                UPDATE app_users
                   SET default_account_id = %s,
                       updated_at = NOW()
                 WHERE user_id = %s
                """,
                (account_id, user_id),
            )
        pg_conn.commit()
        return True
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        if previous_autocommit is not None:
            pg_conn.autocommit = previous_autocommit
