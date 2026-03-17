#!/usr/bin/env python3

import sqlite3
from pathlib import Path


BASE_DIR = Path("/home/patrickli/Space/clarity_lab/jac_tpch")
DATA_DIR = BASE_DIR / "tpch_small"
ANSWERS_DIR = DATA_DIR / "answers"


QUERY_COLUMNS = {
    1: ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order"],
    2: ["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"],
    3: ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"],
    4: ["o_orderpriority", "order_count"],
    5: ["n_name", "revenue"],
    6: ["revenue"],
    7: ["supp_nation", "cust_nation", "l_year", "revenue"],
    8: ["o_year", "mkt_share"],
    9: ["nation", "o_year", "sum_profit"],
    10: ["c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment"],
    11: ["ps_partkey", "value"],
    12: ["l_shipmode", "high_line_count", "low_line_count"],
    13: ["c_count", "custdist"],
    14: ["promo_revenue"],
    15: ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"],
    16: ["p_brand", "p_type", "p_size", "supplier_cnt"],
    17: ["avg_yearly"],
    18: ["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum_quantity"],
    19: ["revenue"],
    20: ["s_name", "s_address"],
    21: ["s_name", "numwait"],
    22: ["cntrycode", "numcust", "totacctbal"],
}


SCHEMA = {
    "region": (
        """
        CREATE TABLE region (
            r_regionkey INTEGER,
            r_name TEXT,
            r_comment TEXT
        )
        """,
        "region.tbl",
        ["r_regionkey", "r_name", "r_comment"],
    ),
    "nation": (
        """
        CREATE TABLE nation (
            n_nationkey INTEGER,
            n_name TEXT,
            n_regionkey INTEGER,
            n_comment TEXT
        )
        """,
        "nation.tbl",
        ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    ),
    "part": (
        """
        CREATE TABLE part (
            p_partkey INTEGER,
            p_name TEXT,
            p_mfgr TEXT,
            p_brand TEXT,
            p_type TEXT,
            p_size INTEGER,
            p_container TEXT,
            p_retailprice REAL,
            p_comment TEXT
        )
        """,
        "part.tbl",
        ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"],
    ),
    "supplier": (
        """
        CREATE TABLE supplier (
            s_suppkey INTEGER,
            s_name TEXT,
            s_address TEXT,
            s_nationkey INTEGER,
            s_phone TEXT,
            s_acctbal REAL,
            s_comment TEXT
        )
        """,
        "supplier.tbl",
        ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
    ),
    "partsupp": (
        """
        CREATE TABLE partsupp (
            ps_partkey INTEGER,
            ps_suppkey INTEGER,
            ps_availqty INTEGER,
            ps_supplycost REAL,
            ps_comment TEXT
        )
        """,
        "partsupp.tbl",
        ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
    ),
    "customer": (
        """
        CREATE TABLE customer (
            c_custkey INTEGER,
            c_name TEXT,
            c_address TEXT,
            c_nationkey INTEGER,
            c_phone TEXT,
            c_acctbal REAL,
            c_mktsegment TEXT,
            c_comment TEXT
        )
        """,
        "customer.tbl",
        ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
    ),
    "orders": (
        """
        CREATE TABLE orders (
            o_orderkey INTEGER,
            o_custkey INTEGER,
            o_orderstatus TEXT,
            o_totalprice REAL,
            o_orderdate TEXT,
            o_orderpriority TEXT,
            o_clerk TEXT,
            o_shippriority INTEGER,
            o_comment TEXT
        )
        """,
        "orders.tbl",
        ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
    ),
    "lineitem": (
        """
        CREATE TABLE lineitem (
            l_orderkey INTEGER,
            l_partkey INTEGER,
            l_suppkey INTEGER,
            l_linenumber INTEGER,
            l_quantity REAL,
            l_extendedprice REAL,
            l_discount REAL,
            l_tax REAL,
            l_returnflag TEXT,
            l_linestatus TEXT,
            l_shipdate TEXT,
            l_commitdate TEXT,
            l_receiptdate TEXT,
            l_shipinstruct TEXT,
            l_shipmode TEXT,
            l_comment TEXT
        )
        """,
        "lineitem.tbl",
        ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"],
    ),
}


def load_tbl(conn: sqlite3.Connection, table: str, filename: str, columns: list[str]) -> None:
    file_path = DATA_DIR / filename
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

    rows: list[list[str]] = []
    with file_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            fields = line.split("|")
            if fields and fields[-1] == "":
                fields = fields[:-1]
            rows.append(fields)

    conn.executemany(insert_sql, rows)


def build_queries() -> dict[int, tuple[str, tuple]]:
    return {
        1: (
            """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) AS sum_qty,
                SUM(l_extendedprice) AS sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                AVG(l_quantity) AS avg_qty,
                AVG(l_extendedprice) AS avg_price,
                AVG(l_discount) AS avg_disc,
                COUNT(*) AS count_order
            FROM lineitem
            WHERE l_shipdate <= date('1998-12-01', '-90 day')
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
            """,
            (),
        ),
        2: (
            """
            SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            FROM part, supplier, partsupp, nation, region
            WHERE p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND p_size = ?
              AND p_type LIKE '%' || ?
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = ?
              AND ps_supplycost = (
                  SELECT MIN(ps_supplycost)
                  FROM partsupp, supplier, nation, region
                  WHERE p_partkey = ps_partkey
                    AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_name = ?
              )
            ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
            LIMIT 100
            """,
            (15, "BRASS", "EUROPE", "EUROPE"),
        ),
        3: (
            """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM customer, orders, lineitem
            WHERE c_mktsegment = ?
              AND c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate < ?
              AND l_shipdate > ?
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10
            """,
            ("BUILDING", "1995-03-15", "1995-03-15"),
        ),
        4: (
            """
            SELECT
                o_orderpriority,
                COUNT(*) AS order_count
            FROM orders
            WHERE o_orderdate >= ?
              AND o_orderdate < date(?, '+3 month')
              AND EXISTS (
                  SELECT 1
                  FROM lineitem
                  WHERE l_orderkey = o_orderkey
                    AND l_commitdate < l_receiptdate
              )
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority
            """,
            ("1993-07-01", "1993-07-01"),
        ),
        5: (
            """
            SELECT
                n_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM customer, orders, lineitem, supplier, nation, region
            WHERE c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND l_suppkey = s_suppkey
              AND c_nationkey = s_nationkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = ?
              AND o_orderdate >= ?
              AND o_orderdate < date(?, '+1 year')
            GROUP BY n_name
            ORDER BY revenue DESC
            """,
            ("ASIA", "1994-01-01", "1994-01-01"),
        ),
        6: (
            """
            SELECT
                SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE l_shipdate >= ?
              AND l_shipdate < date(?, '+1 year')
              AND l_discount BETWEEN ? - 0.01 AND ? + 0.01
              AND l_quantity < ?
            """,
            ("1994-01-01", "1994-01-01", 0.06, 0.06, 24),
        ),
        7: (
            """
            SELECT
                supp_nation,
                cust_nation,
                l_year,
                SUM(volume) AS revenue
            FROM (
                SELECT
                    n1.n_name AS supp_nation,
                    n2.n_name AS cust_nation,
                    CAST(strftime('%Y', l_shipdate) AS INTEGER) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume
                FROM supplier, lineitem, orders, customer, nation AS n1, nation AS n2
                WHERE s_suppkey = l_suppkey
                  AND o_orderkey = l_orderkey
                  AND c_custkey = o_custkey
                  AND s_nationkey = n1.n_nationkey
                  AND c_nationkey = n2.n_nationkey
                  AND ((n1.n_name = ? AND n2.n_name = ?) OR (n1.n_name = ? AND n2.n_name = ?))
                  AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
            ) AS shipping
            GROUP BY supp_nation, cust_nation, l_year
            ORDER BY supp_nation, cust_nation, l_year
            """,
            ("FRANCE", "GERMANY", "GERMANY", "FRANCE"),
        ),
        8: (
            """
            SELECT
                o_year,
                SUM(CASE WHEN nation = ? THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
            FROM (
                SELECT
                    CAST(strftime('%Y', o_orderdate) AS INTEGER) AS o_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    n2.n_name AS nation
                FROM part, supplier, lineitem, orders, customer, nation AS n1, nation AS n2, region
                WHERE p_partkey = l_partkey
                  AND s_suppkey = l_suppkey
                  AND l_orderkey = o_orderkey
                  AND o_custkey = c_custkey
                  AND c_nationkey = n1.n_nationkey
                  AND n1.n_regionkey = r_regionkey
                  AND r_name = ?
                  AND s_nationkey = n2.n_nationkey
                  AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
                  AND p_type = ?
            ) AS all_nations
            GROUP BY o_year
            ORDER BY o_year
            """,
            ("BRAZIL", "AMERICA", "ECONOMY ANODIZED STEEL"),
        ),
        9: (
            """
            SELECT
                nation,
                o_year,
                SUM(amount) AS sum_profit
            FROM (
                SELECT
                    n_name AS nation,
                    CAST(strftime('%Y', o_orderdate) AS INTEGER) AS o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                FROM part, supplier, lineitem, partsupp, orders, nation
                WHERE s_suppkey = l_suppkey
                  AND ps_suppkey = l_suppkey
                  AND ps_partkey = l_partkey
                  AND p_partkey = l_partkey
                  AND o_orderkey = l_orderkey
                  AND s_nationkey = n_nationkey
                  AND p_name LIKE '%' || ? || '%'
            ) AS profit
            GROUP BY nation, o_year
            ORDER BY nation, o_year DESC
            """,
            ("green",),
        ),
        10: (
            """
            SELECT
                c_custkey,
                c_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            FROM customer, orders, lineitem, nation
            WHERE c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate >= ?
              AND o_orderdate < date(?, '+3 month')
              AND l_returnflag = 'R'
              AND c_nationkey = n_nationkey
            GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
            ORDER BY revenue DESC
            LIMIT 20
            """,
            ("1993-10-01", "1993-10-01"),
        ),
        11: (
            """
            SELECT
                ps_partkey,
                SUM(ps_supplycost * ps_availqty) AS value
            FROM partsupp, supplier, nation
            WHERE ps_suppkey = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = ?
            GROUP BY ps_partkey
            HAVING SUM(ps_supplycost * ps_availqty) > (
                SELECT SUM(ps_supplycost * ps_availqty) * ?
                FROM partsupp, supplier, nation
                WHERE ps_suppkey = s_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_name = ?
            )
            ORDER BY value DESC
            """,
            ("GERMANY", 0.0001, "GERMANY"),
        ),
        12: (
            """
            SELECT
                l_shipmode,
                SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
                SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
            FROM orders, lineitem
            WHERE o_orderkey = l_orderkey
              AND l_shipmode IN (?, ?)
              AND l_commitdate < l_receiptdate
              AND l_shipdate < l_commitdate
              AND l_receiptdate >= ?
              AND l_receiptdate < date(?, '+1 year')
            GROUP BY l_shipmode
            ORDER BY l_shipmode
            """,
            ("MAIL", "SHIP", "1994-01-01", "1994-01-01"),
        ),
        13: (
            """
            SELECT
                c_count,
                COUNT(*) AS custdist
            FROM (
                SELECT
                    c_custkey,
                    COUNT(o_orderkey) AS c_count
                FROM customer
                LEFT OUTER JOIN orders
                  ON c_custkey = o_custkey
                 AND o_comment NOT LIKE '%' || ? || '%' || ? || '%'
                GROUP BY c_custkey
            ) AS c_orders
            GROUP BY c_count
            ORDER BY custdist DESC, c_count DESC
            """,
            ("special", "requests"),
        ),
        14: (
            """
            SELECT
                100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM lineitem, part
            WHERE l_partkey = p_partkey
              AND l_shipdate >= ?
              AND l_shipdate < date(?, '+1 month')
            """,
            ("1995-09-01", "1995-09-01"),
        ),
        15: (
            """
            WITH revenue AS (
                SELECT
                    l_suppkey AS supplier_no,
                    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM lineitem
                WHERE l_shipdate >= ?
                  AND l_shipdate < date(?, '+3 month')
                GROUP BY l_suppkey
            )
            SELECT
                s_suppkey,
                s_name,
                s_address,
                s_phone,
                total_revenue
            FROM supplier, revenue
            WHERE s_suppkey = supplier_no
              AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
            ORDER BY s_suppkey
            """,
            ("1996-01-01", "1996-01-01"),
        ),
        16: (
            """
            SELECT
                p_brand,
                p_type,
                p_size,
                COUNT(DISTINCT ps_suppkey) AS supplier_cnt
            FROM partsupp, part
            WHERE p_partkey = ps_partkey
              AND p_brand <> ?
              AND p_type NOT LIKE ? || '%'
              AND p_size IN (?, ?, ?, ?, ?, ?, ?, ?)
              AND ps_suppkey NOT IN (
                  SELECT s_suppkey
                  FROM supplier
                  WHERE s_comment LIKE '%Customer%Complaints%'
              )
            GROUP BY p_brand, p_type, p_size
            ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
            """,
            ("Brand#45", "MEDIUM POLISHED", 49, 14, 23, 45, 19, 3, 36, 9),
        ),
        17: (
            """
            SELECT
                SUM(l_extendedprice) / 7.0 AS avg_yearly
            FROM lineitem, part
            WHERE p_partkey = l_partkey
              AND p_brand = ?
              AND p_container = ?
              AND l_quantity < (
                  SELECT 0.2 * AVG(l_quantity)
                  FROM lineitem
                  WHERE l_partkey = p_partkey
              )
            """,
            ("Brand#23", "MED BOX"),
        ),
        18: (
            """
            SELECT
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice,
                SUM(l_quantity) AS sum_quantity
            FROM customer, orders, lineitem
            WHERE o_orderkey IN (
                SELECT l_orderkey
                FROM lineitem
                GROUP BY l_orderkey
                HAVING SUM(l_quantity) > ?
            )
              AND c_custkey = o_custkey
              AND o_orderkey = l_orderkey
            GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
            ORDER BY o_totalprice DESC, o_orderdate
            LIMIT 100
            """,
            (300,),
        ),
        19: (
            """
            SELECT
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM lineitem, part
            WHERE (
                    p_partkey = l_partkey
                AND p_brand = ?
                AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                AND l_quantity >= ?
                AND l_quantity <= ? + 10
                AND p_size BETWEEN 1 AND 5
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            ) OR (
                    p_partkey = l_partkey
                AND p_brand = ?
                AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= ?
                AND l_quantity <= ? + 10
                AND p_size BETWEEN 1 AND 10
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            ) OR (
                    p_partkey = l_partkey
                AND p_brand = ?
                AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= ?
                AND l_quantity <= ? + 10
                AND p_size BETWEEN 1 AND 15
                AND l_shipmode IN ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            )
            """,
            ("Brand#12", 1, 1, "Brand#23", 10, 10, "Brand#34", 20, 20),
        ),
        20: (
            """
            SELECT
                s_name,
                s_address
            FROM supplier, nation
            WHERE s_suppkey IN (
                SELECT ps_suppkey
                FROM partsupp
                WHERE ps_partkey IN (
                    SELECT p_partkey
                    FROM part
                    WHERE p_name LIKE ? || '%'
                )
                AND ps_availqty > (
                    SELECT 0.5 * SUM(l_quantity)
                    FROM lineitem
                    WHERE l_partkey = ps_partkey
                      AND l_suppkey = ps_suppkey
                      AND l_shipdate >= ?
                      AND l_shipdate < date(?, '+1 year')
                )
            )
              AND s_nationkey = n_nationkey
              AND n_name = ?
            ORDER BY s_name
            """,
            ("forest", "1994-01-01", "1994-01-01", "CANADA"),
        ),
        21: (
            """
            SELECT
                s_name,
                COUNT(*) AS numwait
            FROM supplier, lineitem AS l1, orders, nation
            WHERE s_suppkey = l1.l_suppkey
              AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F'
              AND l1.l_receiptdate > l1.l_commitdate
              AND EXISTS (
                  SELECT 1
                  FROM lineitem AS l2
                  WHERE l2.l_orderkey = l1.l_orderkey
                    AND l2.l_suppkey <> l1.l_suppkey
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM lineitem AS l3
                  WHERE l3.l_orderkey = l1.l_orderkey
                    AND l3.l_suppkey <> l1.l_suppkey
                    AND l3.l_receiptdate > l3.l_commitdate
              )
              AND s_nationkey = n_nationkey
              AND n_name = ?
            GROUP BY s_name
            ORDER BY numwait DESC, s_name
            LIMIT 100
            """,
            ("SAUDI ARABIA",),
        ),
        22: (
            """
            SELECT
                cntrycode,
                COUNT(*) AS numcust,
                SUM(c_acctbal) AS totacctbal
            FROM (
                SELECT
                    substr(c_phone, 1, 2) AS cntrycode,
                    c_acctbal
                FROM customer
                WHERE substr(c_phone, 1, 2) IN (?, ?, ?, ?, ?, ?, ?)
                  AND c_acctbal > (
                      SELECT AVG(c_acctbal)
                      FROM customer
                      WHERE c_acctbal > 0.00
                        AND substr(c_phone, 1, 2) IN (?, ?, ?, ?, ?, ?, ?)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM orders
                      WHERE o_custkey = c_custkey
                  )
            ) AS custsale
            GROUP BY cntrycode
            ORDER BY cntrycode
            """,
            (
                "13", "31", "23", "29", "30", "18", "17",
                "13", "31", "23", "29", "30", "18", "17",
            ),
        ),
    }


def format_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def create_indexes(conn: sqlite3.Connection) -> None:
    index_statements = [
        "CREATE INDEX idx_region_name ON region(r_name)",
        "CREATE INDEX idx_nation_region ON nation(n_regionkey)",
        "CREATE INDEX idx_nation_name ON nation(n_name)",
        "CREATE INDEX idx_supplier_nation ON supplier(s_nationkey)",
        "CREATE INDEX idx_partsupp_part_supp ON partsupp(ps_partkey, ps_suppkey)",
        "CREATE INDEX idx_partsupp_supp ON partsupp(ps_suppkey)",
        "CREATE INDEX idx_customer_nation ON customer(c_nationkey)",
        "CREATE INDEX idx_customer_segment ON customer(c_mktsegment)",
        "CREATE INDEX idx_customer_phone ON customer(c_phone)",
        "CREATE INDEX idx_orders_cust ON orders(o_custkey)",
        "CREATE INDEX idx_orders_date ON orders(o_orderdate)",
        "CREATE INDEX idx_orders_status ON orders(o_orderstatus)",
        "CREATE INDEX idx_lineitem_order ON lineitem(l_orderkey)",
        "CREATE INDEX idx_lineitem_part ON lineitem(l_partkey)",
        "CREATE INDEX idx_lineitem_supp ON lineitem(l_suppkey)",
        "CREATE INDEX idx_lineitem_shipdate ON lineitem(l_shipdate)",
        "CREATE INDEX idx_lineitem_receipt ON lineitem(l_receiptdate)",
        "CREATE INDEX idx_lineitem_commit ON lineitem(l_commitdate)",
        "CREATE INDEX idx_lineitem_mode ON lineitem(l_shipmode)",
        "CREATE INDEX idx_lineitem_instr ON lineitem(l_shipinstruct)",
        "CREATE INDEX idx_lineitem_order_supp_dates ON lineitem(l_orderkey, l_suppkey, l_receiptdate, l_commitdate)",
        "CREATE INDEX idx_part_type ON part(p_type)",
        "CREATE INDEX idx_part_size ON part(p_size)",
        "CREATE INDEX idx_part_brand ON part(p_brand)",
        "CREATE INDEX idx_part_name ON part(p_name)",
    ]
    for stmt in index_statements:
        conn.execute(stmt)


def write_answer_file(query_num: int, rows: list[tuple]) -> None:
    ANSWERS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANSWERS_DIR / f"q{query_num}.out"
    headers = QUERY_COLUMNS[query_num]

    with out_path.open("w", encoding="utf-8") as f:
        f.write("|".join(headers) + "\n")
        for row in rows:
            f.write("|".join(format_value(v) for v in row) + "\n")


def main() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")

    try:
        for _, (ddl, _, _) in SCHEMA.items():
            conn.execute(ddl)

        for table, (_, filename, columns) in SCHEMA.items():
            load_tbl(conn, table, filename, columns)

        create_indexes(conn)
        conn.commit()

        queries = build_queries()
        for query_num in range(1, 23):
            sql, params = queries[query_num]
            cur = conn.execute(sql, params)
            rows = cur.fetchall()
            write_answer_file(query_num, rows)

        print(f"Generated q1.out through q22.out in {ANSWERS_DIR}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
