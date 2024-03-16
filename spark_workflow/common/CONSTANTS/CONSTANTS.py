QUERY_EXECUTION_ORDER = {
    "join": None,
    "where": None,
    "group_by": None,
    "aggregate": None,
    "having": None,
    "select": None,
    "distinct": None,
    "order_by": None,
    "limit": None
}

QUERY_EXECUTION = ["where", "group_by", "aggregate", "having", "select", "distinct", "order_by", "limit", "join"]

JOIN_TYPES = ["inner", "full", "left", "right"]

AGG_FUNC = ["sum", "count", "avg", "min", "max", "mean", "sumDistinct"]
