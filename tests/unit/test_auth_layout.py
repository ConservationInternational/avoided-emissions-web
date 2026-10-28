"""Tests for the sign-in page's Trends.Earth links."""

from urllib.parse import parse_qs, urlparse

import pytest
from dash import html
from layouts.auth import login_layout


def _links(component):
    if isinstance(component, html.A):
        yield component
    children = getattr(component, "children", None)
    if isinstance(children, (list, tuple)):
        for child in children:
            yield from _links(child)
    elif children is not None:
        yield from _links(children)


@pytest.mark.unit
@pytest.mark.parametrize("error", ["", "unavailable", "failed"])
@pytest.mark.parametrize("next_url", ["", "/tasks/task-id?tab=results"])
def test_login_offers_direct_registration_without_changing_sign_in(error, next_url):
    links = {
        link.id: link
        for link in _links(login_layout(error=error, next_url=next_url))
        if getattr(link, "id", None)
    }

    register = links["register-button"]
    assert register.children == "Register with Trends.Earth"
    assert register.href == "https://api.trends.earth/register"
    assert "btn" in register.className.split()
    assert "w-100" in register.className.split()

    sign_in = urlparse(links["login-button"].href)
    assert sign_in.path == "/auth/login"
    assert parse_qs(sign_in.query) == ({"next": [next_url]} if next_url else {})
