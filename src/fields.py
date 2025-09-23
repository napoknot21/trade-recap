from __future__ import annotations

import json, ast
import polars as pl

from typing import Any, Dict, List


def parse_list_of_dicts (s : str) -> list[dict] :
    """
    
    """
    if s is None :
        return []
    
    s = s.strip()
    
    if s == "" or s == "[]" :
        return []
    
    try :
        v = ast.literal_eval(s)
    
    except Exception :
        
        try :
            v = json.loads(s.replace("'", '"'))
        
        except Exception :
            return []
        
    out = []

    if isinstance(v, list) :

        for item in v :

            if isinstance(item, dict) :

                out.append(
                    {
                        "code" : "" if item.get("code") is None else str(item.get("code")),
                        "value" : "" if item.get("value") is None else str(item.get("value")),
                    }
                )
    
    return out


def manage_list_type_column_from_df (df : pl.DataFrame, column : str = "fields") -> pl.DataFrame :
    """

    """
    if df is None or df.is_empty() or column not in df.columns :
        return df

    col_dtype = dict(df.schema).get(column)

    if isinstance(col_dtype, pl.List) and isinstance(col_dtype.inner, pl.Struct) :

        if df.select(pl.col(column).list.len().sum()).item() == 0 :
            return df
        
        base = df.with_row_index("_rowid").explode(column).drop_nulls(column)

        extracted = base.select(
            "_rowid",
            pl.col(column).struct.field("code").cast(pl.Utf8).alias("__code"),
            pl.col(column).struct.field("value").cast(pl.Utf8).alias("__value"),
        )

        wide = extracted.pivot(
            values="__value", index="_rowid", on="__code", aggregate_function="first"
        )

        out = df.with_row_index("_rowid").join(wide, on="_rowid", how="left").drop("_rowid")
        created = [c for c in out.columns if c not in df.columns]

        return out.rename({c: f"{column}.{c}" for c in created})

    if col_dtype == pl.Utf8 :

        target = pl.List(pl.Struct([pl.Field("code", pl.Utf8), pl.Field("value", pl.Utf8)]))
        
        df2 = df.with_columns(

            pl.when(pl.col(column).is_null())
              .then(pl.lit([], dtype=target))
              .otherwise(pl.col(column).map_elements(parse_list_of_dicts, return_dtype=target))
              .alias(column)
        
        )
        
        return manage_list_type_column_from_df(df2, column)

    return df