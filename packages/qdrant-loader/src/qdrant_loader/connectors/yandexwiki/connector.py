"""Yandex Wiki connector."""

import json
import time
from datetime import UTC, datetime
from typing import Any

import requests

from qdrant_loader.config.types import SourceType
from qdrant_loader.connectors.base import BaseConnector, ConnectorConfigurationError
from qdrant_loader.connectors.shared.http import RateLimiter, request_with_policy
from qdrant_loader.connectors.yandexwiki.config import AuthType, OrgType, YandexWikiConfig
from qdrant_loader.core.document import Document
from qdrant_loader.utils.logging import LoggingConfig

logger = LoggingConfig.get_logger(__name__)

_WIKI_API_BASE = "https://api.wiki.yandex.net/v1"
_IAM_TOKEN_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"


class YandexWikiConnector(BaseConnector):
    """Connector for Yandex Wiki."""

    def __init__(self, config: YandexWikiConfig):
        super().__init__(config)
        self.config = config
        self.session = requests.Session()
        self._rate_limiter = RateLimiter.per_minute(config.requests_per_minute)
        # IAM token cache: (token_str, expires_at_timestamp)
        self._iam_token_cache: tuple[str, float] | None = None

    async def __aenter__(self):
        self._initialized = True
        # Eagerly validate connectivity / credentials
        await self._validate_access()
        return self

    async def __aexit__(self, exc_type, exc_val, _exc_tb):
        self.session.close()
        self._initialized = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_documents(self) -> list[Document]:
        """Fetch all pages from Yandex Wiki and return them as Documents."""
        logger.info(
            "Starting Yandex Wiki ingestion",
            source=self.config.source,
            root_slug=self.config.root_page_slug,
        )

        pages = await self._get_all_pages()
        logger.info("Fetched page list", count=len(pages), source=self.config.source)

        documents: list[Document] = []
        for page in pages:
            try:
                doc = await self._page_to_document(page)
                if doc is not None:
                    documents.append(doc)
            except Exception as exc:
                logger.warning(
                    "Failed to process page",
                    page_id=page.get("id"),
                    slug=page.get("slug"),
                    error=str(exc),
                )

        logger.info(
            "Yandex Wiki ingestion complete",
            source=self.config.source,
            documents=len(documents),
        )
        return documents

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _validate_access(self) -> None:
        """Verify credentials by fetching a known page or listing pages."""
        try:
            # Use pageSize=1 to minimise traffic — just need a 200 response
            await self._request("GET", "/pages/descendants", params={"slug": "homepage", "pageSize": 1})
        except Exception as exc:
            raise ConnectorConfigurationError(
                f"Yandex Wiki authentication failed for source '{self.config.source}': {exc}"
            ) from exc

    async def _get_all_pages(self) -> list[dict]:
        """Return a flat list of all pages to process."""
        if self.config.root_page_slug:
            return await self._get_descendants(slug=self.config.root_page_slug)

        # No root slug: fetch all pages in the organization via pagination
        return await self._get_pages_paginated()

    async def _get_descendants(self, *, slug: str) -> list[dict]:
        """Fetch all descendants of a page identified by slug.

        The API returns {results: [{id, slug}, ...], next_cursor, prev_cursor}.
        We also prepend the root page itself.
        """
        pages: list[dict] = []

        # Include the root page itself first
        try:
            root = await self._request("GET", "/pages", params={"slug": slug})
            pages.append(root)
        except Exception as exc:
            logger.warning("Could not fetch root page", slug=slug, error=str(exc))

        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"slug": slug, "pageSize": 100}
            if cursor:
                params["cursor"] = cursor

            data = await self._request("GET", "/pages/descendants", params=params)
            items = data.get("results", [])
            pages.extend(items)

            cursor = data.get("next_cursor")
            if not cursor:
                break

        return pages

    async def _get_pages_paginated(self) -> list[dict]:
        """Fetch all pages in the organization using cursor-based pagination."""
        pages: list[dict] = []
        cursor: str | None = None

        while True:
            params: dict[str, Any] = {"pageSize": 100}
            if cursor:
                params["cursor"] = cursor

            data = await self._request("GET", "/pages", params=params)
            items = data.get("results", [])
            pages.extend(items)

            if len(pages) % 500 == 0 and len(pages) > 0:
                logger.debug("Fetched pages so far", count=len(pages))

            cursor = data.get("next_cursor")
            if not cursor:
                break

        return pages

    async def _page_to_document(self, page: dict) -> Document | None:
        """Convert a raw page dict to a Document, fetching full content if needed."""
        slug = page.get("slug", "")
        title = page.get("title") or slug

        # Apply slug filters
        if self.config.include_page_slugs and slug not in self.config.include_page_slugs:
            return None
        if slug in self.config.exclude_page_slugs:
            return None

        page_id = page.get("id")
        if not page_id:
            logger.warning("Page has no id, skipping", slug=slug)
            return None

        # Fetch full page with content (content is only returned when explicitly requested)
        full_page = await self._request(
            "GET", f"/pages/{page_id}", params={"fields": "content,breadcrumbs"}
        )
        content = full_page.get("content", "")
        title = full_page.get("title") or title

        base_url = str(self.config.base_url).rstrip("/")
        url = f"{base_url}/{slug}" if slug else base_url

        # Build breadcrumb path from breadcrumbs array if available
        breadcrumbs = full_page.get("breadcrumbs", [])
        breadcrumb_path = " / ".join(b.get("title", "") for b in breadcrumbs if b.get("title"))

        metadata: dict[str, Any] = {
            "page_id": page_id,
            "slug": slug,
            "page_type": full_page.get("page_type"),
        }
        if breadcrumb_path:
            metadata["breadcrumb"] = breadcrumb_path

        now = datetime.now(UTC)
        return Document(
            title=title,
            content=content or "",
            content_type="wiki",
            source_type=SourceType.YANDEXWIKI,
            source=self.config.source,
            url=url,
            metadata=metadata,
            created_at=now,
            updated_at=now,
        )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _get_auth_header(self) -> str:
        """Build the Authorization header value."""
        if self.config.auth_type == AuthType.OAUTH:
            return f"OAuth {self.config.token}"

        # IAM auth
        if self.config.sa_key_json:
            token = await self._get_iam_token_from_sa()
            return f"Bearer {token}"

        # Static IAM token
        return f"Bearer {self.config.token}"

    async def _get_iam_token_from_sa(self) -> str:
        """Exchange a service account key for an IAM token, with caching."""
        now = time.monotonic()

        if self._iam_token_cache is not None:
            cached_token, expires_at = self._iam_token_cache
            # Refresh 5 minutes before expiry
            if now < expires_at - 300:
                return cached_token

        token, lifetime_seconds = await self._fetch_iam_token()
        self._iam_token_cache = (token, now + lifetime_seconds)
        return token

    async def _fetch_iam_token(self) -> tuple[str, float]:
        """Call Yandex Cloud IAM to get a token using a service account JWT.

        Returns (token_string, lifetime_seconds).
        """
        try:
            import jwt as pyjwt  # PyJWT
        except ImportError as exc:
            raise ConnectorConfigurationError(
                "PyJWT is required for IAM service account authentication. "
                "Install it with: pip install PyJWT cryptography"
            ) from exc

        sa_key = json.loads(self.config.sa_key_json)  # type: ignore[arg-type]

        now = int(time.time())
        payload = {
            "aud": _IAM_TOKEN_URL,
            "iss": sa_key["service_account_id"],
            "iat": now,
            "exp": now + 3600,
        }
        # Yandex Cloud SA keys have a comment line before the PEM block — strip it
        raw_key = sa_key["private_key"]
        if "-----BEGIN" in raw_key:
            pem_start = raw_key.index("-----BEGIN")
            private_key = raw_key[pem_start:]
        else:
            private_key = raw_key
        key_id = sa_key["id"]

        signed_jwt = pyjwt.encode(
            payload,
            private_key,
            algorithm="PS256",
            headers={"kid": key_id},
        )

        response = await request_with_policy(
            self.session,
            "POST",
            _IAM_TOKEN_URL,
            json={"jwt": signed_jwt},
            rate_limiter=None,
        )
        response.raise_for_status()
        data = response.json()

        token = data["iamToken"]
        # IAM tokens are valid for up to 12 hours; use that as default lifetime
        lifetime: float = 12 * 3600
        if "expiresAt" in data:
            try:
                expires = datetime.fromisoformat(data["expiresAt"].replace("Z", "+00:00"))
                lifetime = max(0.0, (expires - datetime.now(UTC)).total_seconds())
            except Exception:
                pass

        return token, lifetime

    def _org_header(self) -> dict[str, str]:
        """Return the organization header dict."""
        if self.config.org_type == OrgType.YANDEX_CLOUD:
            return {"X-Cloud-Org-Id": self.config.org_id}
        return {"X-Org-Id": self.config.org_id}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
    ) -> dict:
        """Perform an authenticated API request and return the parsed JSON body."""
        auth_header = await self._get_auth_header()
        headers = {
            "Authorization": auth_header,
            **self._org_header(),
            "Accept": "application/json",
        }

        url = f"{_WIKI_API_BASE}{path}"
        response = await request_with_policy(
            self.session,
            method,
            url,
            headers=headers,
            params=params,
            json=json,
            rate_limiter=self._rate_limiter,
        )
        response.raise_for_status()
        return response.json()


# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------

def _parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO 8601 datetime string to an aware datetime object."""
    if not value:
        return None
    try:
        # Handle both 'Z' suffix and '+00:00'
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, TypeError):
        return None
