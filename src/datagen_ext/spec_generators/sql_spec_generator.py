from typing import Optional, Dict, List
from datagen_ext.models import ColumnDefinition, DatagenSpec, TableDefinition, KeyDefinition
from datagen_ext.spec_generators import AbstractSpecGenerator
from datagen_ext.utils.sql_query_analyzer import SQLQueryAnalyzer
import re

class SQLSpecGenerator(AbstractSpecGenerator):
    def __init__(self, query: str):
        self.query = query
        self.analyzer = SQLQueryAnalyzer()



    def __convert_schema_to_datagen_spec(self,
            schema_json: dict,
            *,
            default_rows: int = 10_000,
            key_type_overrides: Optional[Dict[str, str]] = None,
    ) -> DatagenSpec:

        key_type_overrides = key_type_overrides or {}
        relationships = schema_json.get("relationships", [])
        raw_tables = schema_json.get("tables", {})

        # dimension tables appear only on right side
        l_tbls = {r["left"] for r in relationships}
        dim_tbls = {r["right"] for r in relationships if r["right"] not in l_tbls}

        # --- build KeyDefinition map & column→key mapping ------------
        keys: Dict[str, KeyDefinition] = {}
        col2kinfo: Dict[tuple[str,str], dict] = {}

        for rel in relationships:
            ltbl, rtbl = rel["left"], rel["right"]
            lkey, rkey = rel["left_key"], rel["right_key"]

            base = re.sub(r"^[a-z0-9]+_", "", rkey)
            key_ref = f"{base}_ref"

            if key_ref not in keys:
                keys[key_ref] = KeyDefinition(type=key_type_overrides.get(key_ref, "long"))

            col2kinfo[(rtbl, rkey)] = {"key_ref": key_ref, "is_pk": rtbl in dim_tbls}
            col2kinfo[(ltbl, lkey)] = {"key_ref": key_ref, "is_pk": False}

        # --- build TableDefinitions ----------------------------------
        tables: Dict[str, TableDefinition] = {}
        for tbl, cols in raw_tables.items():
            cols_def: List[ColumnDefinition] = []
            for c in sorted(cols):
                info = col2kinfo.get((tbl, c))
                if info:
                    cols_def.append(
                        ColumnDefinition(
                            name=c,
                            type=keys[info["key_ref"]].type,
                            key_ref=info["key_ref"],
                            is_primary=info["is_pk"],
                            nullable=not info["is_pk"]
                        )
                    )
                else:
                    cols_def.append(ColumnDefinition(name=c))
            tables[tbl] = TableDefinition(number_of_rows=default_rows, columns=cols_def)

        # --- set cardinalities once per PK ---------------------------
        for (tbl, _), info in col2kinfo.items():
            if info["is_pk"]:
                k = keys[info["key_ref"]]
                if k.cardinality is None:
                    k.cardinality = tables[tbl].number_of_rows

        return DatagenSpec(
            tables=tables,
            keys=keys
        )

    def generate_spec(self) -> DatagenSpec:
        schema_json = self.analyzer.analyze_query(self.query)
        return self.__convert_schema_to_datagen_spec(schema_json)

# query = """
# SELECT
#     i_item_desc,
#     w_warehouse_name,
#     d1.d_week_seq,
#     -- Calculate the number of promotions
#     SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) AS promo_count,
#     -- Calculate the number of non-promotional sales
#     SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo_count,
#     COUNT(*) AS total_sales_count
# FROM catalog_sales cs
# JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk
# JOIN warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk
# JOIN item i ON cs.cs_item_sk = i.i_item_sk
# JOIN customer_demographics cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
# JOIN household_demographics hd ON cs.cs_bill_hdemo_sk = hd.hd_demo_sk
# JOIN date_dim d1 ON cs.cs_sold_date_sk = d1.d_date_sk
# JOIN date_dim d2 ON inv.inv_date_sk = d2.d_date_sk
# JOIN date_dim d3 ON cs.cs_ship_date_sk = d3.d_date_sk
# LEFT OUTER JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
# LEFT OUTER JOIN catalog_returns cr ON cs.cs_item_sk = cr.cr_item_sk AND cs.cs_order_number = cr.cr_order_number
# WHERE
#     d1.d_week_seq = d2.d_week_seq
#     AND inv_quantity_on_hand < cs_quantity
#     AND d3.d_date > (CAST(d1.d_date AS DATE) + INTERVAL '5' DAY)
#     AND hd_buy_potential = '>10000'
#     AND d1.d_year = 1999
#     AND cd_marital_status = 'D'
# GROUP BY
#     i_item_desc, w_warehouse_name, d1.d_week_seq
# ORDER BY
#     total_sales_count DESC, i_item_desc, w_warehouse_name, d1.d_week_seq;
# """
#
# # ssg = SQLSpecGenerator()
# # rels = ssg.analyze_query(query)
# # print(rels)