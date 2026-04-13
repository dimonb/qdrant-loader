"""Configuration for Yandex Wiki connector."""

import os
from enum import StrEnum
from typing import Self

from pydantic import ConfigDict, Field, field_validator, model_validator

from qdrant_loader.config.source_config import SourceConfig


class OrgType(StrEnum):
    """Yandex organization type."""

    YANDEX360 = "yandex360"
    YANDEX_CLOUD = "yandex_cloud"


class AuthType(StrEnum):
    """Authentication type for Yandex Wiki API."""

    OAUTH = "oauth"
    IAM = "iam"


class YandexWikiConfig(SourceConfig):
    """Configuration for a Yandex Wiki source."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    org_id: str = Field(..., description="Yandex organization ID")
    org_type: OrgType = Field(
        default=OrgType.YANDEX360,
        description="Organization type: yandex360 or yandex_cloud",
    )
    auth_type: AuthType = Field(
        default=AuthType.OAUTH,
        description="Authentication type: oauth (personal Yandex ID) or iam (personal or service account, Yandex Cloud only)",
    )
    token: str | None = Field(
        default=None,
        description="OAuth token or static IAM token. Loaded from YANDEX_WIKI_TOKEN env var if not set.",
    )
    sa_key_json: str | None = Field(
        default=None,
        description="Service account key JSON for automatic IAM token refresh (Yandex Cloud only). Loaded from YANDEX_SA_KEY_JSON env var if not set.",
    )
    root_page_slug: str | None = Field(
        default=None,
        description="Root page slug to index (e.g. 'my-team'). If None, fetches all pages in the organization.",
    )
    requests_per_minute: int = Field(
        default=60,
        description="Maximum number of API requests per minute.",
        ge=1,
        le=1000,
    )
    include_page_slugs: list[str] = Field(
        default=[],
        description="List of page slugs to include (empty = all pages).",
    )
    exclude_page_slugs: list[str] = Field(
        default=[],
        description="List of page slugs to exclude.",
    )

    @field_validator("token", mode="after")
    @classmethod
    def load_token_from_env(cls, v: str | None) -> str | None:
        """Load token from environment variable if not provided."""
        return v or os.getenv("YANDEX_WIKI_TOKEN")

    @field_validator("sa_key_json", mode="after")
    @classmethod
    def load_sa_key_from_env(cls, v: str | None) -> str | None:
        """Load service account key JSON from environment variable if not provided."""
        return v or os.getenv("YANDEX_SA_KEY_JSON")

    @model_validator(mode="after")
    def validate_auth_config(self) -> Self:
        """Validate authentication configuration."""
        if self.auth_type == AuthType.IAM and self.org_type == OrgType.YANDEX360:
            raise ValueError(
                "IAM authentication is only supported for Yandex Cloud organizations. "
                "Use auth_type=oauth for Yandex 360 organizations."
            )

        # Note: Yandex Wiki API does not support IAM service accounts.
        # If auth_type=iam, only a personal IAM token (not SA) may work.
        # For reliable access, use auth_type=oauth with a personal OAuth token.

        if self.auth_type == AuthType.OAUTH and not self.token:
            raise ValueError(
                "OAuth token is required for OAuth authentication. "
                "Set token or YANDEX_WIKI_TOKEN environment variable."
            )

        if self.auth_type == AuthType.IAM and not self.token and not self.sa_key_json:
            raise ValueError(
                "Either token (static IAM token) or sa_key_json (for automatic IAM token refresh) "
                "is required for IAM authentication. "
                "Set YANDEX_WIKI_TOKEN or YANDEX_SA_KEY_JSON environment variable."
            )

        return self
