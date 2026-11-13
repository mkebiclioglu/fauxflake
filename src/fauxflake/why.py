"""Why a model came out empty.

A model that builds with no rows proves that its SQL binds and little else. This walks the model's
query on the warehouse a build left behind and names the place where the rows stop: the CTE, and in
it the join, the WHERE condition, the QUALIFY or the HAVING that keeps nothing, with what the columns
involved actually hold.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp


@dataclass
class Finding:
    scope: str  # the CTE (or "final select") where rows stop
    kind: str  # input | join | flatten | where | qualify | having | group | unknown | error
    clause: str = ""  # the SQL that keeps nothing
    detail: str = ""
    columns: list[tuple[str, list]] = field(default_factory=list)  # (expression, sample of its values)
    inputs: dict[str, int] = field(default_factory=dict)  # rows in each relation the scope reads
    said: str = "rows stop in"

    def text(self) -> str:
        lines = [f"{self.said} {self.scope}: {self.kind}" + (f" ({self.detail})" if self.detail else "")]
        if self.clause:
            lines.append(f"  {self.clause}")
        for name, rows in self.inputs.items():
            lines.append(f"  reads {name}: {rows} row(s)")
        for expression, sample in self.columns:
            shown = ", ".join(repr(v) if isinstance(v, str) else str(v) for v in sample)
            lines.append(f"  {expression} holds: {shown or 'nothing'}")
        return "\n".join(lines)


def _conjuncts(node: exp.Expression) -> list[exp.Expression]:
    node = node.unnest() if isinstance(node, exp.Paren) else node
    if isinstance(node, exp.And):
        return _conjuncts(node.this) + _conjuncts(node.expression)
    return [node]


class Why:
    def __init__(self, cursor, sql: str, timeout: float = 60.0) -> None:  # noqa: ANN001
        self.cur, self.timeout = cursor, timeout
        self.tree = sqlglot.parse_one(sql, read="snowflake")
        self.ctes: list[exp.CTE] = list(self.tree.args["with_"].expressions) if self.tree.args.get("with_") else []
        self.recursive = bool(self.tree.args.get("with_") and self.tree.args["with_"].args.get("recursive"))

    # -- running pieces of the query ------------------------------------------------------------------
    def prefix(self, upto: int | None = None) -> str:
        ctes = self.ctes if upto is None else self.ctes[:upto]
        if not ctes:
            return ""
        return "with " + ("recursive " if self.recursive else "") + ", ".join(c.sql(dialect="snowflake") for c in ctes) + " "

    def rows(self, sql: str) -> list[tuple]:
        self.cur.execute(sql)
        return self.cur.fetchall()

    def count(self, body: str, upto: int | None = None) -> int | None:
        try:
            return int(self.rows(f"{self.prefix(upto)}select count(*) from ({body}) as _fx_why")[0][0])
        except Exception:  # noqa: BLE001 - a piece that does not run on its own says nothing
            return None

    def sample(self, expression: str, source: str, upto: int | None) -> list:
        try:
            return [r[0] for r in self.rows(
                f"{self.prefix(upto)}select distinct _v from (select {expression} as _v from {source} limit 20000) where _v is not null limit 5")]
        except Exception:  # noqa: BLE001
            return []

    @staticmethod
    def _relation(select: exp.Select) -> str:
        """The FROM and JOINs of `select`, as text to select other things from."""
        only = exp.Select(expressions=[exp.Star()])
        only.set("from_", select.args["from_"].copy())
        only.set("joins", [j.copy() for j in select.args.get("joins") or []])
        return only.sql(dialect="snowflake").split(" FROM ", 1)[1]

    # -- the walk -------------------------------------------------------------------------------------
    def find(self) -> Finding:
        names = {c.alias.upper(): i for i, c in enumerate(self.ctes)}
        for i, cte in enumerate(self.ctes):
            total = self.count(f"select * from {cte.alias}", upto=i + 1)
            if total == 0:
                return self.inside(cte.this, cte.alias, i, names)
            if total is None:
                continue
        body = self.tree.copy()
        body.set("with_", None)
        return self.inside(body, "the final select", len(self.ctes), names)

    def inside(self, query: exp.Expression, scope: str, upto: int, names: dict[str, int]) -> Finding:
        query = query.unnest()
        while isinstance(query, exp.SetOperation):  # every branch is empty: the first says why
            query = query.left.unnest()
        if isinstance(query, exp.Subquery):
            query = query.this
        if not isinstance(query, exp.Select) or query.args.get("from_") is None:
            return Finding(scope, "unknown", query.sql(dialect="snowflake")[:300])
        source = query.args["from_"].this
        joins = list(query.args.get("joins") or [])
        inputs: dict[str, int] = {}
        for item in [source, *[j.this for j in joins]]:
            if isinstance(item, exp.Table):
                text = item.copy()
                text.set("alias", None)
                found = self.count(f"select * from {text.sql(dialect='snowflake')}", upto)
                if found is not None:
                    inputs[text.sql(dialect="snowflake")] = found
            elif isinstance(item, exp.Subquery) and isinstance(item.this, (exp.Select, exp.SetOperation)):
                found = self.count(item.this.sql(dialect="snowflake"), upto)
                if found == 0:
                    return self.inside(item.this, f"{scope}, subquery {item.alias or ''}".strip(), upto, names)
        empty = [name for name, n in inputs.items() if n == 0]
        first = source.copy()
        first.set("alias", None)
        if isinstance(source, exp.Table) and inputs.get(first.sql(dialect="snowflake")) == 0:
            return Finding(scope, "input", detail=f"{first.sql(dialect='snowflake')} has no rows", inputs=inputs)

        # the SELECT list stays when it only names values (a WHERE may use its aliases); `*` otherwise
        plain = query.args.get("group") is None and not query.args.get("distinct") and not any(
            p.find(exp.AggFunc, exp.Window) for p in query.expressions)
        base = exp.Select(expressions=[p.copy() for p in query.expressions] if plain else [exp.Star()])
        base.set("from_", query.args["from_"].copy())

        def rows_of(select: exp.Select) -> int | None:
            return self.count(select.sql(dialect="snowflake"), upto)

        if rows_of(base) == 0:
            return Finding(scope, "input", detail="its first relation has no rows", inputs=inputs)
        # joins, one at a time
        unsure = False  # a count that did not come back (too slow to finish): what follows may be its doing
        for i, join in enumerate(joins):
            before = base.copy()
            base.append("joins", join.copy())
            found = rows_of(base)
            if found is None:
                # the pairing so far is too large to count: this join by itself, against the first relation
                alone = exp.Select(expressions=[exp.Star()])
                alone.set("from_", query.args["from_"].copy())
                alone.append("joins", join.copy())
                found = rows_of(alone)
                unsure = unsure or found is None
            if found == 0:
                side = str(join.side or "").upper()
                if side in ("LEFT", "FULL") and not join.args.get("on"):
                    continue
                lateral = isinstance(join.this, exp.Lateral) or join.this.find(exp.Explode) is not None
                finding = Finding(scope, "flatten" if lateral else "join", join.sql(dialect="snowflake")[:600], inputs=inputs,
                                  detail="an earlier join could not be counted; it may be the one" if unsure else "")
                if lateral:
                    call = join.this.find(exp.Explode)
                    given = call.find(exp.Kwarg)
                    what = (given.expression if given is not None else call.this).sql(dialect="snowflake") if call is not None else ""
                    if what:
                        finding.columns.append((what, self.sample(what, self._relation(before), upto)))
                    return finding
                if empty:
                    finding.detail = f"{', '.join(empty)} has no rows"
                on = join.args.get("on")
                # each side of the condition, read from the one relation it names (never from the two paired up)
                relations = {(item.alias_or_name or "").upper(): item for item in [source, *[j.this for j in joins]]}
                for test in _conjuncts(on) if on is not None else []:
                    sides = [test.this, test.expression] if isinstance(test, exp.EQ) else [test]
                    for part in sides:
                        if isinstance(part, (exp.Literal, exp.Boolean, exp.Null)):
                            continue
                        named = {c.table.upper() for c in part.find_all(exp.Column)}
                        owner = relations.get(next(iter(named))) if len(named) == 1 else None
                        if owner is None and len(relations) == 1:
                            owner = next(iter(relations.values()))
                        if owner is None:
                            continue
                        finding.columns.append((part.sql(dialect="snowflake"), self.sample(
                            part.sql(dialect="snowflake"), owner.sql(dialect="snowflake"), upto)))
                return finding
        # WHERE, one condition at a time
        where = query.args.get("where")
        if where is not None:
            filtered = base.copy()
            filtered.set("where", where.copy())
            if rows_of(filtered) == 0:
                tests = _conjuncts(where.this)
                alone = []
                for test in tests:
                    only = base.copy()
                    only.set("where", exp.Where(this=test.copy()))
                    if rows_of(only) == 0:
                        alone.append(test)
                culprits = alone or tests
                finding = Finding(scope, "where", " AND ".join(t.sql(dialect="snowflake") for t in culprits)[:900],
                                  detail="" if alone else "no single condition; together they keep nothing", inputs=inputs)
                relation = self._relation(base)
                seen: set[str] = set()
                for test in culprits[:6]:
                    for column in test.find_all(exp.Column):
                        top: exp.Expression = column
                        while isinstance(top.parent, (exp.JSONExtract, exp.Bracket, exp.Cast, exp.TryCast, exp.Lower, exp.Upper, exp.Trim)) \
                                and top.parent.this is top:
                            top = top.parent
                        if top.find_ancestor(exp.Select, exp.Subquery) is not None and top.find_ancestor(exp.Subquery) is not None:
                            continue  # a column of a subquery in the condition
                        text = top.sql(dialect="snowflake")
                        if text not in seen and len(seen) < 8:
                            seen.add(text)
                            finding.columns.append((text, self.sample(text, relation, upto)))
                    for sub in test.find_all(exp.Subquery):
                        try:
                            finding.columns.append((sub.sql(dialect="snowflake")[:200], [r[0] for r in self.rows(
                                self.prefix(upto) + sub.this.sql(dialect="snowflake") + " limit 5")]))
                        except Exception:  # noqa: BLE001, S110
                            pass
                return finding
            base = filtered
        for key, kind in (("group", "group"), ("having", "having"), ("qualify", "qualify")):
            if query.args.get(key) is None:
                continue
            step = query.copy()
            for later in {"group": ("having", "qualify"), "having": ("qualify",), "qualify": ()}[key]:
                step.set(later, None)
            for clause in ("order", "limit"):
                step.set(clause, None)
            if rows_of(step) == 0 and key != "group":
                return Finding(scope, kind, query.args[key].sql(dialect="snowflake")[:600], inputs=inputs)
        limit = query.args.get("limit")
        if limit is not None and limit.expression is not None and limit.expression.name == "0":
            return Finding(scope, "where", "LIMIT 0", inputs=inputs)
        return Finding(scope, "unknown", detail="every clause keeps rows on its own", inputs=inputs)


class Twice(Why):
    """Where a key that should be unique comes to appear more than once: the join that pairs a row with
    several, the UNION whose branches share keys, or the relation that already holds it twice."""

    def __init__(self, cursor, sql: str, key: str) -> None:  # noqa: ANN001
        super().__init__(cursor, sql)
        self.key = key

    def both(self, body: str, upto: int | None, key: str | None = None) -> tuple[int, int] | None:
        """(rows, distinct keys) of a query, or None when it does not have the key (or does not run)."""
        try:
            found = self.rows(f"{self.prefix(upto)}select count(*), count(distinct {key or self.key}) from ({body}) as _fx_why "
                              f"where {key or self.key} is not null")[0]
            return int(found[0]), int(found[1])
        except Exception:  # noqa: BLE001
            return None

    def find(self) -> Finding:
        for i, cte in enumerate(self.ctes):
            seen = self.both(f"select * from {cte.alias}", i + 1)
            if seen and seen[0] > seen[1]:
                return self.inside_twice(cte.this, cte.alias, i)
        body = self.tree.copy()
        body.set("with_", None)
        return self.inside_twice(body, "the final select", len(self.ctes))

    def inside_twice(self, query: exp.Expression, scope: str, upto: int) -> Finding:
        query = query.unnest()
        if isinstance(query, exp.SetOperation):
            branches, node = [], query
            while isinstance(node, exp.SetOperation):
                branches.append(node.right.unnest())
                node = node.left.unnest()
            branches.append(node)
            for branch in reversed(branches):
                seen = self.both(branch.sql(dialect="snowflake"), upto)
                if seen and seen[0] > seen[1]:
                    return self.inside_twice(branch, f"{scope}, one side of its UNION", upto)
            return Finding(scope, "union", detail=f"each side holds {self.key} once; the sides share values", clause=query.sql(dialect="snowflake")[:300])
        if not isinstance(query, exp.Select) or query.args.get("from_") is None:
            return Finding(scope, "unknown", query.sql(dialect="snowflake")[:300])
        # what the key is, written in terms of the relations read (the SELECT may only rename it)
        written = next((p.unalias() for p in query.expressions if p.alias_or_name.upper() == self.key.upper()), None)
        key = written.sql(dialect="snowflake") if written is not None and not written.find(exp.AggFunc, exp.Window) else self.key
        source = query.args["from_"].this
        base = exp.Select(expressions=[exp.Star()])
        base.set("from_", query.args["from_"].copy())
        inputs: dict[str, int] = {}
        def counted(select: exp.Select) -> tuple[int, int] | None:
            """Rows and distinct keys of FROM + JOINs, with the key as the SELECT writes it (`o.order_id`)."""
            relation = self._relation(select)
            try:
                found = self.rows(f"{self.prefix(upto)}select count(*), count(distinct {key}) from {relation} where {key} is not null")[0]
                return int(found[0]), int(found[1])
            except Exception:  # noqa: BLE001
                return None

        before = counted(base)
        name = source.sql(dialect="snowflake")[:120]
        if before:
            inputs[name] = before[0]
            if before[0] > before[1]:
                return Finding(scope, "input", detail=f"{name} already holds {before[0] - before[1]} repeated value(s) of {key}", inputs=inputs)
        def total(select: exp.Select) -> int | None:
            try:
                return int(self.rows(f"{self.prefix(upto)}select count(*) from {self._relation(select)}")[0][0])
            except Exception:  # noqa: BLE001
                return None

        rows = total(base)
        grew: list[tuple[int, int, int, exp.Join]] = []  # joins after which there are more rows than before
        for join in query.args.get("joins") or []:
            base.append("joins", join.copy())
            now = total(base)
            if rows is not None and now is not None and now > rows:
                grew.append((now - rows, rows, now, join))
            rows = now if now is not None else rows
        base.set("joins", [])
        # the join that added rows; when the key is made of columns of several relations, the one that added most
        for join in query.args.get("joins") or []:
            base.append("joins", join.copy())
            after = counted(base)
            culprit = join if after and before and after[0] > after[1] and after[0] > before[0] else None
            if culprit is None and join is (query.args.get("joins") or [None])[-1] and grew and (after is None or after[0] > after[1]):
                _, was, became, culprit = max(grew, key=lambda g: g[0])
                before, after = (was, was), (became, became)
            if culprit is not None:
                join = culprit
                finding = Finding(scope, "join", join.sql(dialect="snowflake")[:600], inputs=inputs,
                                  detail=f"{before[0]} row(s) before it, {after[0]} after: a row meets several on the other side")
                on = join.args.get("on")
                alias = (join.this.alias_or_name or "").upper()
                mine = [c for test in (_conjuncts(on) if on is not None else []) for c in test.find_all(exp.Column) if c.table.upper() == alias]
                if mine:
                    columns = ", ".join(dict.fromkeys(c.sql(dialect="snowflake") for c in mine))
                    relation = join.this.sql(dialect="snowflake")
                    try:
                        repeated = self.rows(f"{self.prefix(upto)}select {columns}, count(*) from {relation} group by all having count(*) > 1 limit 3")
                    except Exception:  # noqa: BLE001
                        repeated = []
                    finding.columns.append((f"{columns} (value, times) in {join.this.alias_or_name}", [tuple(r) for r in repeated]))
                return finding
            before = after or before
        return Finding(scope, "unknown", detail=f"no join adds rows; {self.key} repeats after the WHERE / GROUP BY of this step", inputs=inputs)


def explain_duplicates(cursor, sql: str, key: str) -> Finding:  # noqa: ANN001
    try:
        found = Twice(cursor, sql, key).find()
        found.said = f"{key} repeats from"
        return found
    except Exception as e:  # noqa: BLE001
        return Finding("the model", "error", detail=str(e)[:300])


def explain(cursor, sql: str) -> Finding:  # noqa: ANN001
    try:
        return Why(cursor, sql).find()
    except Exception as e:  # noqa: BLE001
        return Finding("the model", "error", detail=str(e)[:300])
