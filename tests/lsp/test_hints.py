"""Tests for type hinting SQLMesh models"""

import pytest

from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.hints import get_hints
from sqlmesh.lsp.uri import URI


@pytest.mark.fast
def test_hints() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find model URIs
    active_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.active_customers" in info.names
    )
    customer_revenue_lifetime_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customer_revenue_lifetime" in info.names
    )
    customer_revenue_by_day_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customer_revenue_by_day" in info.names
    )

    active_customers_uri = URI.from_path(active_customers_path)
    ac_hints = get_hints(lsp_context, active_customers_uri, start_line=0, end_line=9999)
    assert len(ac_hints) == 2
    assert ac_hints[0].label == "::INT"
    assert ac_hints[1].label == "::TEXT"

    customer_revenue_lifetime_uri = URI.from_path(customer_revenue_lifetime_path)
    crl_hints = get_hints(
        lsp_context=lsp_context,
        document_uri=customer_revenue_lifetime_uri,
        start_line=0,
        end_line=9999,
    )
    assert len(crl_hints) == 3
    assert crl_hints[0].label == "::INT"
    assert crl_hints[1].label == "::DOUBLE"
    assert crl_hints[2].label == "::DATE"

    customer_revenue_by_day_uri = URI.from_path(customer_revenue_by_day_path)
    crbd_hints = get_hints(
        lsp_context=lsp_context,
        document_uri=customer_revenue_by_day_uri,
        start_line=0,
        end_line=9999,
    )
    assert len(crbd_hints) == 4
    assert crbd_hints[0].label == "::INT"
    assert crbd_hints[1].label == "::DOUBLE"
    assert crbd_hints[2].label == "::INT"
    assert crbd_hints[3].label == "::DATE"
