from __future__ import annotations

import fnmatch
import polars as pl

from collections import deque
from typing import Any, Dict, List, Optional, Tuple, Iterable

from src.utils import sanitize_key, py_to_jsonish, looks_jsonish_expr
from src.config import SEP as DEFAULT_SEP


def resolve_list_policy (
        
        path: str,
        routes: Optional[List[Dict[str, Any]]],
        default_strategy: str,
        default_list_max: int,
        default_join_delim: str,

    ) -> Tuple[str, int, str]:
    """
    
    """
    if routes :

        for r in routes :
        
            pat = r.get("pattern")
            
            if pat and fnmatch.fnmatchcase(path, pat) :
                
                return (
                    r.get("strategy", default_strategy),
                    int(r.get("list_max", default_list_max)),
                    r.get("join_delim", default_join_delim),
                )
            
    return default_strategy, default_list_max, default_join_delim


def flatten_struct_like_columns_routed(
        
        df : pl.DataFrame,
        columns : Iterable[str],
        *,
        sep : str = DEFAULT_SEP,
        parse_strings : bool = True,
        sample_rows : int = 8,
        infer_json_rows : Optional[int] = None,
        drop_source : bool = False,
        max_depth : int = 100,
        default_list_strategy : str = "index",
        default_list_max : int = 30,
        default_join_delim : str = "; ",
        routes : Optional[List[Dict[str, Any]]] = None,
    
    ) -> pl.DataFrame :
    """
    
    """
    if df is None or df.is_empty() :
        return df

    out = df

    if parse_strings :
        
        sniff_exprs, names = [], []
        
        for col in columns :

            if col in out.columns and out.schema.get(col) == pl.Utf8 :

                sniff_exprs.append(looks_jsonish_expr(pl.col(col)).alias(col))
                names.append(col)
        
        if sniff_exprs :

            sniff = out.select(sniff_exprs).head(sample_rows)
            to_decode = [col for col in names if sniff[col].any()]
            
            if to_decode :

                out = out.with_columns(
                    [
                        pl.col(c).map_elements(py_to_jsonish, return_dtype=pl.Utf8)
                                 .str.json_decode(infer_schema_length=infer_json_rows)
                                 .alias(c)
                        
                        for c in to_decode
                    ]
                )

    queue = deque([c for c in columns if isinstance(out.schema.get(c), (pl.Struct, pl.List))])
    depth = 0

    while queue and depth < max_depth :

        cur = queue.popleft()
        dt = out.schema.get(cur)
        
        if dt is None :
            continue
        
        # Case for pl.Struct[n], with n:= integer
        if isinstance(dt, pl.Struct) :

            fields = dt.fields
            
            if fields :
            
                exprs = [
                    pl.col(cur).struct.field(f.name).alias(f"{cur}{sep}{sanitize_key(f.name, sep)}")
                    for f in fields
                ]

                out = out.with_columns(exprs)
                
                if drop_source and cur in out.columns :
                    out = out.drop(cur)
                
                sch = out.schema

                for f in fields :

                    child = f"{cur}{sep}{sanitize_key(f.name, sep)}"
                    cdt = sch.get(child)
                    
                    if isinstance(cdt, (pl.Struct, pl.List)) :
                        queue.append(child)
        
        # Case for Lists
        elif isinstance(dt, pl.List) :

            inner = dt.inner
            strategy, list_max, join_delim = resolve_list_policy(cur, routes, default_list_strategy, default_list_max, default_join_delim)
            
            if strategy == "ignore" :
                pass

            elif strategy == "join" and not isinstance(inner, (pl.Struct, pl.List)) :
                out = out.with_columns(pl.col(cur).list.join(join_delim).alias(cur))

            elif strategy in ("index", "first") :

                obs_max = out.select(pl.col(cur).list.len().max()).item() or 0
                limit = 1 if strategy == "first" else min(list_max, int(obs_max))
                
                if limit > 0 :

                    exprs = [pl.col(cur).list.get(i).alias(f"{cur}{sep}{i}") for i in range(limit)]
                    
                    out = out.with_columns(exprs)
                    sch = out.schema
                    
                    for i in range(limit) :

                        child = f"{cur}{sep}{i}"
                        cdt = sch.get(child)
                        
                        if isinstance(cdt, (pl.Struct, pl.List)) :
                            queue.append(child)

            elif strategy == "explode" :

                out = out.explode(cur)
                new_dt = out.schema.get(cur)
                
                if isinstance(new_dt, (pl.Struct, pl.List)) :
                    queue.append(cur)

        depth += 1

    return out


def manage_struct_like_column_from_df (df : pl.DataFrame, column : str, **kwargs) -> pl.DataFrame :
    """
    
    """
    flatten = flatten_all_struct_like_columns_routed(df, [column], **kwargs)
    
    return flatten


def flatten_all_struct_like_columns_routed(
        
        df : pl.DataFrame,
        *,
        include_cols : Optional[List[str]] = None,
        exclude_cols : Optional[List[str]] = None,
        **kwargs,

    ) -> pl.DataFrame :
    """
    
    """
    if df is None or df.is_empty() :
        return df
    
    cols = include_cols or df.columns
    
    if exclude_cols :

        ex = set(exclude_cols)
        cols = [c for c in cols if c not in ex]
    
    return flatten_struct_like_columns_routed(df, cols, **kwargs)
