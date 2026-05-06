"""
FNS V2 runtime helpers for custom profile expressions.

Design goals:
- Keep v1 evaluator logic untouched.
- Provide a namespaced, dedupe-aware helper surface for profile mode.
- Keep state under meta._fns_v2 to avoid collisions with v1 state.
"""

from __future__ import annotations

import math
from bisect import insort
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


_TRUE_SET = {"1", "true", "yes", "on", "y"}
_FALSE_SET = {"0", "false", "no", "off", "n"}


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUE_SET:
            return True
        if text in _FALSE_SET:
            return False
    return bool(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
        if math.isnan(out):
            return float(default)
        return out
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _split_csv(values: Any) -> List[str]:
    if isinstance(values, str):
        return [part.strip() for part in values.split(",") if part.strip()]
    if isinstance(values, (list, tuple, set)):
        out: List[str] = []
        for item in values:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return out
    return []


def _path_get(data: Any, path: str, default: Any = None) -> Any:
    if not isinstance(path, str) or not path.strip():
        return default
    current = data
    for token in [part.strip() for part in path.split(".") if part.strip()]:
        if isinstance(current, dict):
            if token not in current:
                return default
            current = current[token]
            continue
        return default
    return current


def _path_set(data: Dict[str, Any], path: str, value: Any) -> None:
    tokens = [part.strip() for part in str(path or "").split(".") if part.strip()]
    if not tokens:
        return
    current: Dict[str, Any] = data
    for token in tokens[:-1]:
        nxt = current.get(token)
        if not isinstance(nxt, dict):
            nxt = {}
            current[token] = nxt
        current = nxt
    current[tokens[-1]] = value


def _normalise_unique_key(value: Any, mode: Any) -> Any:
    if value is None:
        return None
    text_mode = str(mode or "trim").strip().lower().replace("-", "_")
    if text_mode == "upper_trim":
        text_mode = "trim_upper"
    elif text_mode == "lower_trim":
        text_mode = "trim_lower"
    if not isinstance(value, str):
        return value
    out = value
    if text_mode in {"trim", "trim_upper", "trim_lower", "upper", "lower", "auto"}:
        out = out.strip()
    if text_mode in {"upper", "trim_upper", "auto"}:
        out = out.upper()
    elif text_mode in {"lower", "trim_lower"}:
        out = out.lower()
    return out


def _format_time(dt: Optional[datetime]) -> Optional[str]:
    if not isinstance(dt, datetime):
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse_window_seconds(value: str) -> Tuple[str, int]:
    text = str(value or "").strip().lower()
    if not text:
        return ("w_1d", 86400)

    if text.endswith("m"):
        minutes = max(1.0, _safe_float(text[:-1], 1.0))
        return (f"w_{int(minutes)}m", int(minutes * 60.0))
    if text.endswith("h"):
        hours = max(1.0, _safe_float(text[:-1], 1.0))
        return (f"w_{int(hours)}h", int(hours * 3600.0))
    if text.endswith("d"):
        days = max(1.0, _safe_float(text[:-1], 1.0))
        if float(days).is_integer():
            return (f"w_{int(days)}d", int(days * 86400.0))
        label = str(days).replace(".", "_")
        return (f"w_{label}d", int(days * 86400.0))

    # Numeric values are treated as days for compatibility.
    num_days = max(1e-6, _safe_float(text, 1.0))
    if float(num_days).is_integer():
        return (f"w_{int(num_days)}d", int(num_days * 86400.0))
    label = str(num_days).replace(".", "_")
    return (f"w_{label}d", int(num_days * 86400.0))


def _parse_duration_seconds(value: Any, default_seconds: int = 0, numeric_unit: str = "seconds") -> int:
    if value is None:
        return max(0, _safe_int(default_seconds, 0))
    if isinstance(value, (int, float)):
        unit = str(numeric_unit or "seconds").strip().lower()
        raw = max(0.0, _safe_float(value, 0.0))
        if unit in {"days", "day", "d"}:
            return int(raw * 86400.0)
        if unit in {"hours", "hour", "h"}:
            return int(raw * 3600.0)
        if unit in {"minutes", "minute", "min", "m"}:
            return int(raw * 60.0)
        return int(raw)

    text = str(value or "").strip().lower()
    if not text:
        return max(0, _safe_int(default_seconds, 0))
    if text.endswith("s"):
        return max(0, int(max(0.0, _safe_float(text[:-1], 0.0))))
    if text.endswith("m"):
        return max(0, int(max(0.0, _safe_float(text[:-1], 0.0)) * 60.0))
    if text.endswith("h"):
        return max(0, int(max(0.0, _safe_float(text[:-1], 0.0)) * 3600.0))
    if text.endswith("d"):
        return max(0, int(max(0.0, _safe_float(text[:-1], 0.0)) * 86400.0))
    return max(0, int(max(0.0, _safe_float(text, float(default_seconds)))))


class _FnsV2Runtime:
    def __init__(
        self,
        *,
        profile_root: str,
        working_doc: Dict[str, Any],
        working_meta: Dict[str, Any],
        custom_values: Dict[str, Any],
        resolve_profile_path: Callable[[Any], str],
        profile_prev: Callable[[Any, Any], Any],
        stable_json_token: Callable[[Any], str],
        to_number: Callable[[Any], Optional[float]],
        parse_event_time: Callable[[Any], Optional[datetime]],
        current_event_dt: datetime,
        current_event_time: str,
        entity_key: Any,
        json_safe_value: Callable[[Any], Any],
        v1_inc: Callable[[Any, Any, Any], Any],
        v1_map_inc: Callable[[Any, Any, Any, Any], Dict[str, Any]],
        v1_append_unique: Callable[[Any, Any, Any, Any], List[Any]],
        v1_rolling_update: Callable[[Any, Any, Any, Any, Any], Dict[str, Any]],
        default_dedupe_options: Optional[Dict[str, Any]] = None,
        row_dedupe_gate_enabled: bool = False,
    ):
        self.profile_root = str(profile_root or "").strip()
        self.working_doc = working_doc
        self.working_meta = working_meta
        self.custom_values = custom_values
        self.resolve_profile_path = resolve_profile_path
        self.profile_prev = profile_prev
        self.stable_json_token = stable_json_token
        self.to_number = to_number
        self.parse_event_time = parse_event_time
        self.current_event_dt = current_event_dt
        self.current_event_time = current_event_time
        self.entity_key = entity_key
        self.json_safe_value = json_safe_value

        self.v1_inc = v1_inc
        self.v1_map_inc = v1_map_inc
        self.v1_append_unique = v1_append_unique
        self.v1_rolling_update = v1_rolling_update
        self.default_dedupe_options = (
            dict(default_dedupe_options)
            if isinstance(default_dedupe_options, dict)
            else {}
        )
        self.row_dedupe_gate_enabled = bool(row_dedupe_gate_enabled)
        self._row_dedupe_gate_ready = False
        self._row_dedupe_gate_value: Optional[Tuple[bool, bool, str]] = None

        self.meta_root = self._ensure_meta_root()

    def _ensure_meta_root(self) -> Dict[str, Any]:
        root = self.working_meta.get("_fns_v2")
        if not isinstance(root, dict):
            root = {}
            self.working_meta["_fns_v2"] = root
        for bucket_name in ("dedupe", "metrics", "groups", "signals", "row_dedupe"):
            bucket = root.get(bucket_name)
            if not isinstance(bucket, dict):
                root[bucket_name] = {}
        return root

    def _resolve_metric_key(self, path: Any) -> str:
        text = str(path or "").strip()
        if not text:
            return "metric"
        return text

    def _get_metric_state(self, path: Any) -> Dict[str, Any]:
        key = self._resolve_metric_key(path)
        metrics = self.meta_root["metrics"]
        state = metrics.get(key)
        if not isinstance(state, dict):
            state = {}
            metrics[key] = state
        return state

    def _get_group_state(self, path: Any) -> Dict[str, Any]:
        key = self._resolve_metric_key(path)
        groups = self.meta_root["groups"]
        state = groups.get(key)
        if not isinstance(state, dict):
            state = {}
            groups[key] = state
        return state

    def _lookup_value(self, path_or_value: Any, default: Any = None) -> Any:
        if isinstance(path_or_value, str):
            path_text = str(path_or_value or "").strip()
            if path_text in self.custom_values:
                return self.custom_values.get(path_text)
            return self.profile_prev(path_text, default)
        return path_or_value if path_or_value is not None else default

    def _resolve_dedupe_controls(
        self,
        unique_key: Any,
        options: Any = None,
    ) -> Tuple[Any, str, str, str]:
        opts: Dict[str, Any] = {}
        if isinstance(self.default_dedupe_options, dict):
            opts.update(self.default_dedupe_options)
        if isinstance(options, dict):
            opts.update(options)
        effective_unique_key = opts.get("dedupe_key", opts.get("unique_key", unique_key))

        normalize = str(opts.get("normalize", "trim_upper") or "trim_upper").strip().lower().replace("-", "_")
        if normalize == "upper_trim":
            normalize = "trim_upper"
        elif normalize == "lower_trim":
            normalize = "trim_lower"
        elif normalize in {"trim", "upper", "lower", "trim_upper", "trim_lower", "auto"}:
            pass
        else:
            normalize = "trim_upper"

        on_duplicate = str(opts.get("on_duplicate", "ignore") or "ignore").strip().lower()
        if on_duplicate in {"skip", "drop", "ignore"}:
            on_duplicate = "ignore"
        elif on_duplicate in {"update", "overwrite", "replace", "keep_latest"}:
            on_duplicate = "update"
        else:
            on_duplicate = "ignore"

        invalid_raw = opts.get("invalid_key", opts.get("on_invalid_key", "ignore"))
        invalid_mode = str(invalid_raw or "ignore").strip().lower().replace("-", "_")
        if invalid_mode in {"skip", "drop", "ignore"}:
            invalid_mode = "ignore"
        elif invalid_mode in {"allow", "count", "use_null", "null"}:
            invalid_mode = "allow"
        elif invalid_mode in {"raise", "error", "strict"}:
            invalid_mode = "raise"
        else:
            invalid_mode = "ignore"

        return effective_unique_key, normalize, on_duplicate, invalid_mode

    def _dedupe_gate_with_options(
        self,
        metric_path: Any,
        unique_key: Any,
        options: Any = None,
    ) -> Tuple[bool, bool, str]:
        if (
            self.row_dedupe_gate_enabled
            and not self._options_override_dedupe_controls(options)
            and self._default_has_row_dedupe_key()
        ):
            row_gate = self._row_dedupe_gate()
            if row_gate is not None:
                return row_gate
        effective_unique_key, normalize, on_duplicate, invalid_mode = self._resolve_dedupe_controls(
            unique_key,
            options,
        )
        return self._dedupe_gate(
            metric_path,
            effective_unique_key,
            normalize=normalize,
            on_duplicate=on_duplicate,
            on_invalid_key=invalid_mode,
        )

    def _options_override_dedupe_controls(self, options: Any) -> bool:
        if not isinstance(options, dict):
            return False
        for key in (
            "dedupe_key",
            "unique_key",
            "normalize",
            "on_duplicate",
            "invalid_key",
            "on_invalid_key",
        ):
            if key in options:
                return True
        return False

    def _default_has_row_dedupe_key(self) -> bool:
        if not isinstance(self.default_dedupe_options, dict):
            return False
        key = self.default_dedupe_options.get("dedupe_key", self.default_dedupe_options.get("unique_key"))
        if key is None:
            return False
        if isinstance(key, str):
            return key.strip() != ""
        return True

    def _row_dedupe_gate(self) -> Optional[Tuple[bool, bool, str]]:
        if self._row_dedupe_gate_ready:
            return self._row_dedupe_gate_value

        row_root = self.meta_root.get("row_dedupe")
        if not isinstance(row_root, dict):
            row_root = {}
            self.meta_root["row_dedupe"] = row_root

        defaults = self.default_dedupe_options if isinstance(self.default_dedupe_options, dict) else {}
        effective_unique_key, normalize, on_duplicate, invalid_mode = self._resolve_dedupe_controls(
            defaults.get("dedupe_key", defaults.get("unique_key")),
            defaults,
        )
        scope_key = "global"
        state = row_root.get(scope_key)
        if not isinstance(state, dict):
            state = {
                "seen": {},
                "accepted": 0,
                "duplicates": 0,
                "invalid": 0,
                "last_key": None,
            }
            row_root[scope_key] = state

        raw_key = _normalise_unique_key(effective_unique_key, normalize)
        invalid = raw_key is None or (isinstance(raw_key, str) and raw_key == "")
        if invalid:
            state["invalid"] = _safe_int(state.get("invalid"), 0) + 1
            if invalid_mode in {"allow", "count", "use_null"}:
                token = "__invalid__"
            elif invalid_mode in {"raise", "error"}:
                raise RuntimeError("FNS V2: invalid unique key")
            else:
                out = (False, False, "")
                self._row_dedupe_gate_value = out
                self._row_dedupe_gate_ready = True
                return out
        else:
            token = self.stable_json_token(raw_key)

        seen = state.get("seen")
        if not isinstance(seen, dict):
            seen = {}
            state["seen"] = seen

        is_dup = token in seen
        if is_dup:
            state["duplicates"] = _safe_int(state.get("duplicates"), 0) + 1
            if on_duplicate in {"update", "overwrite", "replace"}:
                state["last_key"] = token
                out = (True, True, token)
                self._row_dedupe_gate_value = out
                self._row_dedupe_gate_ready = True
                return out
            out = (False, True, token)
            self._row_dedupe_gate_value = out
            self._row_dedupe_gate_ready = True
            return out

        seen[token] = 1
        state["accepted"] = _safe_int(state.get("accepted"), 0) + 1
        state["last_key"] = token
        out = (True, False, token)
        self._row_dedupe_gate_value = out
        self._row_dedupe_gate_ready = True
        return out

    def _dedupe_gate(
        self,
        metric_path: Any,
        unique_key: Any,
        *,
        normalize: Any = "trim_upper",
        on_duplicate: Any = "ignore",
        on_invalid_key: Any = "ignore",
    ) -> Tuple[bool, bool, str]:
        dedupe_key = self._resolve_metric_key(metric_path)
        dedupe_root = self.meta_root["dedupe"]
        state = dedupe_root.get(dedupe_key)
        if not isinstance(state, dict):
            state = {
                "seen": {},
                "accepted": 0,
                "duplicates": 0,
                "invalid": 0,
                "last_key": None,
            }
            dedupe_root[dedupe_key] = state

        raw_key = _normalise_unique_key(unique_key, normalize)
        invalid = raw_key is None or (isinstance(raw_key, str) and raw_key == "")
        if invalid:
            state["invalid"] = _safe_int(state.get("invalid"), 0) + 1
            invalid_mode = str(on_invalid_key or "ignore").strip().lower()
            if invalid_mode in {"allow", "count", "use_null"}:
                token = "__invalid__"
            elif invalid_mode in {"raise", "error"}:
                raise RuntimeError("FNS V2: invalid unique key")
            else:
                return (False, False, "")
        else:
            token = self.stable_json_token(raw_key)

        seen = state.get("seen")
        if not isinstance(seen, dict):
            seen = {}
            state["seen"] = seen

        is_dup = token in seen
        if is_dup:
            state["duplicates"] = _safe_int(state.get("duplicates"), 0) + 1
            dup_mode = str(on_duplicate or "ignore").strip().lower()
            if dup_mode in {"update", "overwrite", "replace"}:
                state["last_key"] = token
                return (True, True, token)
            return (False, True, token)

        seen[token] = 1
        state["accepted"] = _safe_int(state.get("accepted"), 0) + 1
        state["last_key"] = token
        return (True, False, token)

    def _update_stats_state(self, state: Dict[str, Any], value: Any) -> None:
        n = self.to_number(value)
        if n is None:
            return
        x = float(n)

        count = _safe_int(state.get("count"), 0) + 1
        total = _safe_float(state.get("sum"), 0.0) + x

        min_v = state.get("min")
        max_v = state.get("max")
        if min_v is None or x < _safe_float(min_v, x):
            min_v = x
        if max_v is None or x > _safe_float(max_v, x):
            max_v = x

        prev_mean = _safe_float(state.get("mean"), x)
        prev_m2 = _safe_float(state.get("m2"), 0.0)
        delta = x - prev_mean
        mean = prev_mean + (delta / float(count))
        delta2 = x - mean
        m2 = prev_m2 + (delta * delta2)

        state["count"] = count
        state["sum"] = total
        state["min"] = min_v
        state["max"] = max_v
        state["mean"] = mean
        state["m2"] = m2
        state["last"] = x
        state["last_time"] = self.current_event_time

    def _stats_output(self, state: Dict[str, Any]) -> Dict[str, Any]:
        count = _safe_int(state.get("count"), 0)
        total = _safe_float(state.get("sum"), 0.0)
        mean = (total / float(count)) if count > 0 else None
        m2 = _safe_float(state.get("m2"), 0.0)
        var = (m2 / float(count)) if count > 0 else None
        std = math.sqrt(var) if isinstance(var, (int, float)) and var is not None and var >= 0 else None
        return {
            "count": count,
            "sum": total,
            "avg": mean,
            "min": state.get("min"),
            "max": state.get("max"),
            "var": var,
            "std": std,
            "last": state.get("last"),
            "last_time": state.get("last_time"),
        }

    def _group_metric_value(self, item: Dict[str, Any], metric_text: str) -> Any:
        metric_name = str(metric_text or "count").strip().lower()
        if metric_name == "count":
            return _safe_int(item.get("count"), 0)
        if metric_name == "sum":
            return _safe_float(item.get("sum"), 0.0)
        if metric_name == "avg":
            stats = self._stats_output(item)
            return stats.get("avg")
        if metric_name in {"std", "stddev", "variance", "var", "zscore", "percentile"}:
            stats = self._stats_output(item)
            if metric_name in {"std", "stddev"}:
                return stats.get("std")
            if metric_name in {"variance", "var"}:
                return stats.get("var")
            if metric_name == "percentile":
                return item.get("percentile")
            return item.get("zscore")
        return self._stats_output(item)

    def _safe_time(self, value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        parsed = self.parse_event_time(value)
        if isinstance(parsed, datetime):
            return parsed
        return None

    def _resolve_allowed_lateness_seconds(
        self,
        payload_obj: Optional[Dict[str, Any]] = None,
        options: Any = None,
        *,
        default_seconds: int = 0,
    ) -> int:
        raw: Any = None
        if isinstance(payload_obj, dict):
            raw = payload_obj.get("allowed_lateness")
        if raw is None and isinstance(options, dict):
            raw = options.get("allowed_lateness")
        return _parse_duration_seconds(raw, default_seconds, "seconds")

    def _partition_value_text(self, value: Any) -> str:
        if value is None:
            return "UNKNOWN"
        if isinstance(value, datetime):
            return _format_time(value) or "UNKNOWN"
        if isinstance(value, str):
            text = value.strip()
            return text if text else "UNKNOWN"
        if isinstance(value, (int, float, bool)):
            return str(value)
        return self.stable_json_token(value)

    def _compose_partition_key(self, partition_by: Any = None, partition_key: Any = None) -> Tuple[str, List[str]]:
        values: List[Any] = []
        if isinstance(partition_by, dict):
            for key in sorted(partition_by.keys()):
                values.append(f"{key}={self._partition_value_text(partition_by.get(key))}")
        elif isinstance(partition_by, (list, tuple, set)):
            values.extend(list(partition_by))
        elif partition_by is not None:
            values.append(partition_by)
        elif partition_key is not None:
            values.append(partition_key)

        if not values:
            values = ["GLOBAL"]

        tokens = [self._partition_value_text(item) for item in values]
        if not tokens:
            tokens = ["GLOBAL"]
        return "|".join(tokens), tokens

    def _metric_get(self, path: Any, metric: Any, default: Any = None) -> Any:
        raw = self._lookup_value(path, None)
        if raw is None and isinstance(path, str):
            # also allow direct access to meta state by metric key
            raw = self.meta_root.get("metrics", {}).get(self._resolve_metric_key(path))
        metric_key = str(metric or "").strip()
        if raw is None:
            return default
        if not metric_key:
            return raw
        if isinstance(raw, dict):
            hit = _path_get(raw, metric_key, None)
            if hit is not None:
                return hit
            return raw.get(metric_key, default)
        if metric_key == "value":
            return raw
        return default

    # ---- Canonical functions ----
    def unique_metrics(
        self,
        profile_path: Any,
        unique_key: Any,
        metric: Any = "stats",
        value: Any = None,
        options: Any = None,
    ) -> Any:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )

        state = self._get_metric_state(profile_path)
        if accepted:
            self._update_stats_state(state, value)

        out = self._stats_output(state)
        metric_name = str(metric or "stats").strip().lower()
        if metric_name in {"stats", "all", ""}:
            return out
        if metric_name == "value":
            return out.get("sum")
        if metric_name in out:
            return out.get(metric_name)
        return out

    def unique_group_aggregate(
        self,
        profile_path: Any,
        unique_key: Any,
        partition_key: Any,
        metric: Any = "count",
        value: Any = 1,
        options: Any = None,
    ) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )

        partition_text = str(partition_key if partition_key is not None else "UNKNOWN")
        state = self._get_group_state(profile_path)
        buckets = state.get("buckets")
        if not isinstance(buckets, dict):
            buckets = {}
            state["buckets"] = buckets

        bucket = buckets.get(partition_text)
        if not isinstance(bucket, dict):
            bucket = {}
            buckets[partition_text] = bucket

        metric_text = str(metric or "count").strip().lower()
        if accepted:
            if metric_text in {"count", "sum"}:
                cur = _safe_float(bucket.get(metric_text), 0.0)
                cur += _safe_float(value, 0.0 if metric_text == "sum" else 1.0)
                bucket[metric_text] = int(cur) if float(cur).is_integer() else cur
            else:
                self._update_stats_state(bucket, value)

        cache_root = state.get("out_cache_by_metric")
        if not isinstance(cache_root, dict):
            cache_root = {}
            state["out_cache_by_metric"] = cache_root
        cached_out = cache_root.get(metric_text)
        if not isinstance(cached_out, dict):
            rebuilt: Dict[str, Any] = {}
            for key, item in buckets.items():
                if not isinstance(item, dict):
                    continue
                rebuilt[key] = self._group_metric_value(item, metric_text)
            cache_root[metric_text] = rebuilt
            return rebuilt

        if accepted:
            out = dict(cached_out)
            out[partition_text] = self._group_metric_value(bucket, metric_text)
            cache_root[metric_text] = out
            return out
        return cached_out

    def unique_signal(
        self,
        profile_path: Any,
        unique_key: Any,
        signal_type: Any,
        payload: Any = None,
        options: Any = None,
    ) -> Any:
        signal = str(signal_type or "").strip().lower()
        if signal in {"interval", "unique_interval"}:
            return self.unique_interval(profile_path, unique_key, payload, options)
        if signal in {"velocity", "unique_velocity"}:
            return self.unique_velocity(profile_path, unique_key, payload, options)
        if signal in {"rolling", "unique_rolling"}:
            return self.unique_rolling(profile_path, unique_key, payload, options)
        if signal in {"sequence", "repeated_sequence"}:
            if isinstance(payload, dict):
                return self.repeated_sequence(
                    profile_path,
                    unique_key,
                    payload.get("event"),
                    payload.get("txn_ids_path"),
                    payload.get("tail_size", 20),
                    options,
                )
            return self.repeated_sequence(profile_path, unique_key, payload, None, 20, options)
        if signal in {"anomaly", "anomaly_detection", "anamoly_detection", "outlier"}:
            if isinstance(payload, dict):
                return self.anomaly_detection(
                    profile_path,
                    unique_key,
                    payload.get("value"),
                    payload.get("time"),
                    payload,
                    options,
                )
            return self.anomaly_detection(profile_path, unique_key, payload, None, None, options)
        if signal in {"anomaly_group_aggregate", "anamoly_group_aggregate", "outlier_group"}:
            if isinstance(payload, dict):
                return self.anomaly_group_aggregate(
                    profile_path,
                    unique_key,
                    payload.get("value"),
                    payload.get("time"),
                    payload,
                    options,
                )
            return self.anomaly_group_aggregate(profile_path, unique_key, payload, None, None, options)
        return self._lookup_value(profile_path, None)

    # ---- Wrappers ----
    def unique_count(self, profile_path: Any, unique_key: Any, increment: Any = 1, options: Any = None) -> Any:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        current = _safe_float(self._lookup_value(profile_path, 0), 0.0)
        if accepted:
            current += _safe_float(increment, 1.0)
        return int(current) if float(current).is_integer() else current

    def unique_sum(self, profile_path: Any, unique_key: Any, value: Any, options: Any = None) -> Any:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        current = _safe_float(self._lookup_value(profile_path, 0), 0.0)
        if accepted:
            current += _safe_float(value, 0.0)
        return int(current) if float(current).is_integer() else current

    def unique_avg(self, profile_path: Any, unique_key: Any, value: Any, options: Any = None) -> Any:
        out = self.unique_metrics(profile_path, unique_key, "stats", value, options)
        if isinstance(out, dict):
            return out.get("avg")
        return out

    def unique_min(self, profile_path: Any, unique_key: Any, value: Any, options: Any = None) -> Any:
        out = self.unique_metrics(profile_path, unique_key, "stats", value, options)
        if isinstance(out, dict):
            return out.get("min")
        return out

    def unique_max(self, profile_path: Any, unique_key: Any, value: Any, options: Any = None) -> Any:
        out = self.unique_metrics(profile_path, unique_key, "stats", value, options)
        if isinstance(out, dict):
            return out.get("max")
        return out

    def unique_stats(self, profile_path: Any, unique_key: Any, value: Any, metric: Any = "stats", options: Any = None) -> Any:
        return self.unique_metrics(profile_path, unique_key, metric, value, options)

    def metric_get(self, profile_path: Any, metric: Any = "value", default: Any = None) -> Any:
        return self._metric_get(profile_path, metric, default)

    def unique_metric(self, profile_path: Any, unique_key: Any, metric: Any = "stats", value: Any = None, options: Any = None) -> Any:
        return self.unique_metrics(profile_path, unique_key, metric, value, options)

    def unique_rate(self, profile_path: Any, unique_key: Any, numerator_value: Any, options: Any = None) -> Any:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        state = self._get_metric_state(profile_path)
        if accepted:
            state["den"] = _safe_int(state.get("den"), 0) + 1
            if _coerce_bool(numerator_value, False):
                state["num"] = _safe_int(state.get("num"), 0) + 1
        den = _safe_int(state.get("den"), 0)
        num = _safe_int(state.get("num"), 0)
        return (float(num) / float(den)) if den > 0 else 0.0

    def unique_ratio(self, profile_path: Any, unique_key: Any, numerator: Any, denominator: Any, options: Any = None) -> Any:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        state = self._get_metric_state(profile_path)
        if accepted:
            state["num"] = _safe_float(state.get("num"), 0.0) + _safe_float(numerator, 0.0)
            state["den"] = _safe_float(state.get("den"), 0.0) + _safe_float(denominator, 0.0)
        den = _safe_float(state.get("den"), 0.0)
        num = _safe_float(state.get("num"), 0.0)
        if den == 0:
            return 0.0
        return float(num) / float(den)

    def unique_flag(self, profile_path: Any, unique_key: Any, flag_value: Any, options: Any = None) -> Any:
        return self.unique_rate(profile_path, unique_key, flag_value, options)

    def unique_map(self, profile_path: Any, unique_key: Any, map_key: Any, amount: Any = 1, options: Any = None) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        current = self._lookup_value(profile_path, {})
        out = dict(current) if isinstance(current, dict) else {}
        if accepted:
            key_text = str(map_key if map_key is not None else "UNKNOWN")
            out[key_text] = _safe_float(out.get(key_text), 0.0) + _safe_float(amount, 1.0)
            if float(out[key_text]).is_integer():
                out[key_text] = int(out[key_text])
        return out

    def top_key(self, profile_path_or_map: Any) -> Optional[str]:
        current = self._lookup_value(profile_path_or_map, None)
        if not isinstance(current, dict) or not current:
            return None
        best_key = None
        best_value = None
        for key, val in current.items():
            n = self.to_number(val)
            if n is None:
                continue
            if best_value is None or float(n) > float(best_value):
                best_value = float(n)
                best_key = str(key)
        return best_key

    def unique_interval(self, profile_path: Any, unique_key: Any, txn_time: Any, options: Any = None) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        state = self._get_metric_state(profile_path)

        event_dt = self._safe_time(txn_time) or self.current_event_dt
        prev_dt = self._safe_time(state.get("last_ts"))
        last_sec = state.get("last_sec")
        if accepted and isinstance(prev_dt, datetime) and event_dt >= prev_dt:
            delta = max(0.0, (event_dt - prev_dt).total_seconds())
            state["sum_sec"] = _safe_float(state.get("sum_sec"), 0.0) + delta
            state["count"] = _safe_int(state.get("count"), 0) + 1
            state["last_sec"] = delta
            last_sec = delta

        if accepted and (prev_dt is None or event_dt >= prev_dt):
            state["last_ts"] = _format_time(event_dt)

        count = _safe_int(state.get("count"), 0)
        avg_sec = (_safe_float(state.get("sum_sec"), 0.0) / float(count)) if count > 0 else None
        return {
            "last_sec": last_sec,
            "avg_sec": avg_sec,
            "avg_min": (avg_sec / 60.0) if isinstance(avg_sec, (int, float)) else None,
            "count": count,
        }

    def unique_velocity(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )

        payload_obj = payload if isinstance(payload, dict) else {"time": payload}
        event_dt = self._safe_time(payload_obj.get("time")) or self.current_event_dt
        window_min = max(1, _safe_int(payload_obj.get("window_min"), 5))
        retention_sec = max(window_min * 60, _safe_int(payload_obj.get("retention_sec"), window_min * 120))

        state = self._get_metric_state(profile_path)
        events = state.get("events")
        if not isinstance(events, list):
            events = []

        if accepted:
            events.append(_format_time(event_dt))

        cutoff = event_dt - timedelta(seconds=retention_sec)
        pruned: List[str] = []
        for item in events:
            dt = self._safe_time(item)
            if isinstance(dt, datetime) and dt >= cutoff:
                pruned.append(_format_time(dt) or "")
        state["events"] = pruned

        active_cutoff = event_dt - timedelta(minutes=window_min)
        active_count = 0
        for item in pruned:
            dt = self._safe_time(item)
            if isinstance(dt, datetime) and dt >= active_cutoff:
                active_count += 1

        per_min = float(active_count) / float(window_min)
        return {
            "window_min": window_min,
            "count": active_count,
            "per_min": per_min,
        }

    def burst_flag(self, profile_path: Any, payload: Any) -> bool:
        payload_obj = payload if isinstance(payload, dict) else {}
        curr_ref = str(payload_obj.get("curr") or "").strip()
        base_ref = str(payload_obj.get("base") or "").strip()
        factor = _safe_float(payload_obj.get("factor"), 2.0)

        curr_value = self._metric_get(curr_ref, "value", None) if curr_ref else None
        if curr_value is None and curr_ref:
            curr_value = self._lookup_value(curr_ref, None)
            if isinstance(curr_value, dict):
                curr_value = _path_get(curr_value, "per_min", None)

        base_value = self._metric_get(base_ref, "value", None) if base_ref else None
        if base_value is None and base_ref:
            base_value = self._lookup_value(base_ref, None)

        curr_num = self.to_number(curr_value)
        base_num = self.to_number(base_value)
        if curr_num is None or base_num is None or float(base_num) <= 0:
            return False
        return float(curr_num) >= float(base_num) * max(1.0, factor)

    def entropy_update(self, profile_path: Any, unique_key: Any, value: Any, options: Any = None) -> float:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        state = self._get_metric_state(profile_path)
        counts = state.get("counts")
        if not isinstance(counts, dict):
            counts = {}
            state["counts"] = counts
        total = _safe_int(state.get("total"), 0)

        if accepted:
            key = str(value if value is not None else "UNKNOWN")
            counts[key] = _safe_int(counts.get(key), 0) + 1
            total += 1
            state["total"] = total

        if total <= 0:
            return 0.0
        entropy = 0.0
        for count in counts.values():
            c = _safe_int(count, 0)
            if c <= 0:
                continue
            p = float(c) / float(total)
            entropy -= p * math.log(max(p, 1e-12), 2)
        return entropy

    def min_update(self, profile_path: Any, value: Any) -> Any:
        current = self._lookup_value(profile_path, None)
        candidate = value
        if current is None:
            return candidate
        current_dt = self._safe_time(current)
        candidate_dt = self._safe_time(candidate)
        if isinstance(current_dt, datetime) and isinstance(candidate_dt, datetime):
            return candidate if candidate_dt < current_dt else current
        return candidate if candidate < current else current

    def max_update(self, profile_path: Any, value: Any) -> Any:
        current = self._lookup_value(profile_path, None)
        candidate = value
        if current is None:
            return candidate
        current_dt = self._safe_time(current)
        candidate_dt = self._safe_time(candidate)
        if isinstance(current_dt, datetime) and isinstance(candidate_dt, datetime):
            return candidate if candidate_dt > current_dt else current
        return candidate if candidate > current else current

    def repetition_rate(self, profile_path: Any, unique_key: Any, value: Any, options: Any = None) -> float:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        state = self._get_metric_state(profile_path)
        counts = state.get("counts")
        if not isinstance(counts, dict):
            counts = {}
            state["counts"] = counts
        total = _safe_int(state.get("total"), 0)
        if accepted:
            key = self.stable_json_token(value)
            counts[key] = _safe_int(counts.get(key), 0) + 1
            total += 1
            state["total"] = total

        if total <= 0:
            return 0.0
        repeated = 0
        for count in counts.values():
            c = _safe_int(count, 0)
            if c > 1:
                repeated += (c - 1)
        return float(repeated) / float(total)

    def change_rate(self, profile_path: Any, unique_key: Any, value: Any, options: Any = None) -> float:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        state = self._get_metric_state(profile_path)
        if accepted:
            total = _safe_int(state.get("total"), 0)
            changes = _safe_int(state.get("changes"), 0)
            last_val = state.get("last")
            if total > 0 and last_val != value:
                changes += 1
            total += 1
            state["total"] = total
            state["changes"] = changes
            state["last"] = value
        total = _safe_int(state.get("total"), 0)
        changes = _safe_int(state.get("changes"), 0)
        denom = max(1, total - 1)
        return float(changes) / float(denom)

    def geo_variance(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        payload_obj = payload if isinstance(payload, dict) else {}
        lat = self.to_number(payload_obj.get("lat"))
        lon = self.to_number(payload_obj.get("lon"))
        if lat is None or lon is None:
            prev = self._lookup_value(profile_path, None)
            return prev if isinstance(prev, dict) else {"variance_km2": 0.0, "mean_distance_km": 0.0}

        state = self._get_metric_state(profile_path)
        points = state.get("points")
        if not isinstance(points, list):
            points = []
        if accepted:
            points.append({"lat": float(lat), "lon": float(lon), "ts": self.current_event_time})

        retention = max(50, _safe_int(payload_obj.get("retention_points"), 1000))
        if len(points) > retention:
            points = points[-retention:]
        state["points"] = points

        if not points:
            return {"variance_km2": 0.0, "mean_distance_km": 0.0}

        mean_lat = sum([_safe_float(p.get("lat"), 0.0) for p in points]) / float(len(points))
        mean_lon = sum([_safe_float(p.get("lon"), 0.0) for p in points]) / float(len(points))

        # Equirectangular approximation in KM.
        distances: List[float] = []
        for p in points:
            dlat = (_safe_float(p.get("lat"), mean_lat) - mean_lat) * 111.32
            dlon = (_safe_float(p.get("lon"), mean_lon) - mean_lon) * 111.32 * math.cos(math.radians(mean_lat))
            distances.append(math.sqrt((dlat * dlat) + (dlon * dlon)))

        mean_distance = sum(distances) / float(len(distances))
        var_distance = sum([(d - mean_distance) ** 2 for d in distances]) / float(len(distances))
        return {
            "variance_km2": var_distance,
            "mean_distance_km": mean_distance,
        }

    def geo_conflict_flag(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> bool:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )

        payload_obj = payload if isinstance(payload, dict) else {}
        customer = str(payload_obj.get("customer") or self.entity_key or "")
        lat = self.to_number(payload_obj.get("lat"))
        lon = self.to_number(payload_obj.get("lon"))
        event_dt = self._safe_time(payload_obj.get("time")) or self.current_event_dt
        if lat is None or lon is None or not customer:
            return bool(self._lookup_value(profile_path, False))

        threshold_km = max(1.0, _safe_float(payload_obj.get("distance_km"), 25.0))
        bucket_seconds = max(1, _safe_int(payload_obj.get("time_bucket_seconds"), 60))
        bucket_key = int(event_dt.timestamp()) // bucket_seconds

        state = self._get_metric_state(profile_path)
        positions = state.get("positions")
        if not isinstance(positions, dict):
            positions = {}

        key = f"{customer}|{bucket_key}"
        prior = positions.get(key)
        conflict = bool(state.get("flag"))

        if accepted:
            if isinstance(prior, dict):
                mean_lat = _safe_float(prior.get("lat"), float(lat))
                mean_lon = _safe_float(prior.get("lon"), float(lon))
                dlat = (float(lat) - mean_lat) * 111.32
                dlon = (float(lon) - mean_lon) * 111.32 * math.cos(math.radians(mean_lat))
                distance = math.sqrt((dlat * dlat) + (dlon * dlon))
                if distance >= threshold_km:
                    conflict = True
            positions[key] = {"lat": float(lat), "lon": float(lon), "ts": self.current_event_time}

        state["positions"] = positions
        state["flag"] = conflict
        return conflict

    def _rolling_normalize_events_state(
        self,
        state: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
        raw_events = state.get("events")
        normalized: List[Dict[str, Any]] = []
        if isinstance(raw_events, list):
            for item in raw_events:
                if not isinstance(item, dict):
                    continue
                raw_ts = item.get("ts")
                ts_value: Optional[float] = None
                if isinstance(raw_ts, (int, float)):
                    ts_value = float(raw_ts)
                else:
                    dt = self._safe_time(raw_ts)
                    if isinstance(dt, datetime):
                        ts_value = float(dt.timestamp())
                if ts_value is None:
                    continue
                normalized.append({
                    "ts": ts_value,
                    "value": _safe_float(item.get("value"), 0.0),
                })
        normalized.sort(key=lambda entry: _safe_float(entry.get("ts"), 0.0))
        state["events"] = normalized
        head = max(0, min(_safe_int(state.get("events_head"), 0), len(normalized)))
        state["events_head"] = head
        window_state = state.get("window_state")
        if not isinstance(window_state, dict):
            window_state = {}
            state["window_state"] = window_state
        state["events_fast_v2"] = True
        return normalized, head, window_state

    def _rolling_compact_prefix(
        self,
        state: Dict[str, Any],
        events: List[Dict[str, Any]],
        events_head: int,
        window_state: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], int]:
        shift = max(0, events_head)
        if shift <= 0:
            return events, events_head
        compacted = events[shift:]

        for ws in window_state.values():
            if not isinstance(ws, dict):
                continue
            ws["start_idx"] = max(0, _safe_int(ws.get("start_idx"), 0) - shift)
            for q_key, h_key in (("minq", "minq_head"), ("maxq", "maxq_head")):
                raw_q = ws.get(q_key)
                q = list(raw_q) if isinstance(raw_q, list) else []
                head = max(0, min(_safe_int(ws.get(h_key), 0), len(q)))
                if head > 0:
                    q = q[head:]
                adj: List[int] = []
                for raw_idx in q:
                    idx = _safe_int(raw_idx, -1)
                    if idx >= shift:
                        adj.append(idx - shift)
                ws[q_key] = adj
                ws[h_key] = 0

        state["events"] = compacted
        state["events_head"] = 0
        return compacted, 0

    def _rolling_rebuild_window_state(
        self,
        events: List[Dict[str, Any]],
        events_head: int,
        event_ts: float,
        retention_cutoff_ts: float,
        seconds: int,
    ) -> Dict[str, Any]:
        effective_cutoff = max(retention_cutoff_ts, event_ts - float(seconds))
        start_idx = max(0, min(events_head, len(events)))
        while start_idx < len(events):
            if _safe_float(events[start_idx].get("ts"), 0.0) >= effective_cutoff:
                break
            start_idx += 1

        count = 0
        total = 0.0
        minq: List[int] = []
        maxq: List[int] = []
        for idx in range(start_idx, len(events)):
            val = _safe_float(events[idx].get("value"), 0.0)
            count += 1
            total += val
            while minq and _safe_float(events[minq[-1]].get("value"), 0.0) >= val:
                minq.pop()
            minq.append(idx)
            while maxq and _safe_float(events[maxq[-1]].get("value"), 0.0) <= val:
                maxq.pop()
            maxq.append(idx)

        return {
            "seconds": int(seconds),
            "start_idx": int(start_idx),
            "count": int(count),
            "sum": float(total),
            "last": _safe_float(events[-1].get("value"), 0.0) if count > 0 else None,
            "minq": minq,
            "minq_head": 0,
            "maxq": maxq,
            "maxq_head": 0,
        }

    def _rolling_advance_window_state(
        self,
        ws: Dict[str, Any],
        events: List[Dict[str, Any]],
        effective_cutoff: float,
        new_event_idx: Optional[int],
    ) -> Dict[str, Any]:
        start_idx = max(0, min(_safe_int(ws.get("start_idx"), 0), len(events)))
        count = max(0, _safe_int(ws.get("count"), 0))
        total = _safe_float(ws.get("sum"), 0.0)

        minq = list(ws.get("minq")) if isinstance(ws.get("minq"), list) else []
        minq_head = max(0, min(_safe_int(ws.get("minq_head"), 0), len(minq)))
        maxq = list(ws.get("maxq")) if isinstance(ws.get("maxq"), list) else []
        maxq_head = max(0, min(_safe_int(ws.get("maxq_head"), 0), len(maxq)))

        if isinstance(new_event_idx, int) and 0 <= new_event_idx < len(events) and new_event_idx >= start_idx:
            new_val = _safe_float(events[new_event_idx].get("value"), 0.0)
            count += 1
            total += new_val
            while len(minq) > minq_head and _safe_float(events[minq[-1]].get("value"), 0.0) >= new_val:
                minq.pop()
            minq.append(new_event_idx)
            while len(maxq) > maxq_head and _safe_float(events[maxq[-1]].get("value"), 0.0) <= new_val:
                maxq.pop()
            maxq.append(new_event_idx)

        while start_idx < len(events) and _safe_float(events[start_idx].get("ts"), 0.0) < effective_cutoff:
            old_idx = start_idx
            old_val = _safe_float(events[old_idx].get("value"), 0.0)
            if count > 0:
                count -= 1
                total -= old_val
            if minq_head < len(minq) and _safe_int(minq[minq_head], -1) == old_idx:
                minq_head += 1
            if maxq_head < len(maxq) and _safe_int(maxq[maxq_head], -1) == old_idx:
                maxq_head += 1
            start_idx += 1

        while minq_head < len(minq) and _safe_int(minq[minq_head], -1) < start_idx:
            minq_head += 1
        while maxq_head < len(maxq) and _safe_int(maxq[maxq_head], -1) < start_idx:
            maxq_head += 1

        if minq_head > 0 and (minq_head > 1024 or minq_head * 2 > len(minq)):
            minq = minq[minq_head:]
            minq_head = 0
        if maxq_head > 0 and (maxq_head > 1024 or maxq_head * 2 > len(maxq)):
            maxq = maxq[maxq_head:]
            maxq_head = 0

        ws["start_idx"] = int(start_idx)
        ws["count"] = int(max(0, count))
        ws["sum"] = float(total if abs(total) > 1e-12 else 0.0)
        ws["last"] = (
            _safe_float(events[-1].get("value"), 0.0)
            if ws["count"] > 0 and len(events) > 0 and _safe_float(events[-1].get("ts"), 0.0) >= effective_cutoff
            else None
        )
        ws["minq"] = minq
        ws["minq_head"] = minq_head
        ws["maxq"] = maxq
        ws["maxq_head"] = maxq_head
        return ws

    def unique_rolling(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        payload_obj = payload if isinstance(payload, dict) else {"value": payload}
        value = _safe_float(payload_obj.get("value"), 0.0)
        event_dt = self._safe_time(payload_obj.get("time")) or self.current_event_dt
        allowed_lateness_sec = self._resolve_allowed_lateness_seconds(payload_obj, options, default_seconds=0)

        windows_raw = payload_obj.get("windows")
        windows = _split_csv(windows_raw if windows_raw is not None else "1d,7d,30d")
        if not windows:
            windows = ["1d", "7d", "30d"]
        parsed_windows = [_parse_window_seconds(win) for win in windows]

        retention_days = max(1, _safe_int(payload_obj.get("retention"), 45))
        event_ts = float(event_dt.timestamp())
        retention_cutoff_ts = event_ts - float(retention_days * 86400)

        state = self._get_metric_state(profile_path)
        if not bool(state.get("events_fast_v2", False)):
            events, events_head, window_state = self._rolling_normalize_events_state(state)
        else:
            events = state.get("events")
            if not isinstance(events, list):
                events = []
            events_head = max(0, min(_safe_int(state.get("events_head"), 0), len(events)))
            window_state = state.get("window_state")
            if not isinstance(window_state, dict):
                window_state = {}
                state["window_state"] = window_state

        last_event_ts_raw = state.get("last_event_ts")
        last_event_ts = _safe_float(last_event_ts_raw, event_ts) if last_event_ts_raw is not None else None
        force_rebuild = False
        new_event_idx: Optional[int] = None
        accepted_event = bool(accepted)
        if accepted_event and last_event_ts is not None and event_ts < (last_event_ts - float(allowed_lateness_sec)):
            accepted_event = False
            state["late_dropped"] = _safe_int(state.get("late_dropped"), 0) + 1

        if accepted_event:
            events.append({"ts": event_ts, "value": value})
            new_event_idx = len(events) - 1
            if last_event_ts is not None and event_ts < last_event_ts:
                force_rebuild = True
            if last_event_ts is None or event_ts > last_event_ts:
                state["last_event_ts"] = event_ts
        elif last_event_ts is None or event_ts > last_event_ts:
            state["last_event_ts"] = event_ts

        if force_rebuild:
            events.sort(key=lambda entry: _safe_float(entry.get("ts"), 0.0))
            state["window_state"] = {}
            window_state = state["window_state"]
            if accepted_event:
                for idx, item in enumerate(events):
                    if abs(_safe_float(item.get("ts"), 0.0) - event_ts) < 1e-9 and abs(_safe_float(item.get("value"), 0.0) - value) < 1e-9:
                        new_event_idx = idx
                        break
            if events:
                state["last_event_ts"] = _safe_float(events[-1].get("ts"), event_ts)

        while events_head < len(events) and _safe_float(events[events_head].get("ts"), 0.0) < retention_cutoff_ts:
            events_head += 1

        if events_head > 0 and (events_head > 2048 and events_head * 2 > len(events)):
            compact_shift = events_head
            events, events_head = self._rolling_compact_prefix(state, events, events_head, window_state)
            if isinstance(new_event_idx, int):
                new_event_idx = max(0, new_event_idx - compact_shift)

        state["events"] = events
        state["events_head"] = events_head
        state["events_fast_v2"] = True

        out: Dict[str, Any] = {}
        for label, seconds in parsed_windows:
            sec = max(1, int(seconds))
            ws = window_state.get(label)
            rebuild_needed = (
                force_rebuild
                or not isinstance(ws, dict)
                or _safe_int((ws or {}).get("seconds"), -1) != sec
            )
            if rebuild_needed:
                ws = self._rolling_rebuild_window_state(
                    events=events,
                    events_head=events_head,
                    event_ts=event_ts,
                    retention_cutoff_ts=retention_cutoff_ts,
                    seconds=sec,
                )
            else:
                effective_cutoff = max(retention_cutoff_ts, event_ts - float(sec))
                ws = self._rolling_advance_window_state(
                    ws,
                    events,
                    effective_cutoff,
                    new_event_idx if accepted_event else None,
                )

            window_state[label] = ws
            count = _safe_int(ws.get("count"), 0)
            total = _safe_float(ws.get("sum"), 0.0)
            avg = (total / float(count)) if count > 0 else None

            start_idx = _safe_int(ws.get("start_idx"), 0)
            effective_cutoff = max(retention_cutoff_ts, event_ts - float(sec))
            if count > 0 and 0 <= start_idx < len(events):
                start_ts = _safe_float(events[start_idx].get("ts"), effective_cutoff)
            else:
                start_ts = effective_cutoff

            min_value = None
            max_value = None
            minq = ws.get("minq")
            minq_head = _safe_int(ws.get("minq_head"), 0)
            if isinstance(minq, list) and minq_head < len(minq):
                min_idx = _safe_int(minq[minq_head], -1)
                if 0 <= min_idx < len(events):
                    min_value = _safe_float(events[min_idx].get("value"), 0.0)

            maxq = ws.get("maxq")
            maxq_head = _safe_int(ws.get("maxq_head"), 0)
            if isinstance(maxq, list) and maxq_head < len(maxq):
                max_idx = _safe_int(maxq[maxq_head], -1)
                if 0 <= max_idx < len(events):
                    max_value = _safe_float(events[max_idx].get("value"), 0.0)

            out[label] = {
                "count": count,
                "sum": total,
                "avg": avg,
                "min": min_value,
                "max": max_value,
                "last": ws.get("last"),
                "start_time": _format_time(datetime.fromtimestamp(start_ts)),
                "end_time": _format_time(event_dt),
            }
        state["window_state"] = window_state
        return out

    def unique_window(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> Dict[str, Any]:
        return self.unique_rolling(profile_path, unique_key, payload, options)

    def window_sum(self, profile_path: Any, window: Any = "7d") -> Any:
        rolling = self._lookup_value(profile_path, None)
        label, _seconds = _parse_window_seconds(str(window or "7d"))
        if isinstance(rolling, dict):
            bucket = rolling.get(label)
            if isinstance(bucket, dict):
                return bucket.get("sum")
        state = self._get_metric_state(profile_path)
        events = state.get("events")
        if not isinstance(events, list):
            return None
        cutoff = self.current_event_dt - timedelta(seconds=_parse_window_seconds(str(window or "7d"))[1])
        total = 0.0
        for item in events:
            dt = self._safe_time(item.get("ts") if isinstance(item, dict) else None)
            if not isinstance(dt, datetime) or dt < cutoff:
                continue
            total += _safe_float(item.get("value") if isinstance(item, dict) else 0.0, 0.0)
        return total

    def window_avg(self, profile_path: Any, window: Any = "7d") -> Any:
        rolling = self._lookup_value(profile_path, None)
        label, _seconds = _parse_window_seconds(str(window or "7d"))
        if isinstance(rolling, dict):
            bucket = rolling.get(label)
            if isinstance(bucket, dict):
                return bucket.get("avg")
        state = self._get_metric_state(profile_path)
        events = state.get("events")
        if not isinstance(events, list):
            return None
        cutoff = self.current_event_dt - timedelta(seconds=_parse_window_seconds(str(window or "7d"))[1])
        values: List[float] = []
        for item in events:
            dt = self._safe_time(item.get("ts") if isinstance(item, dict) else None)
            if not isinstance(dt, datetime) or dt < cutoff:
                continue
            values.append(_safe_float(item.get("value") if isinstance(item, dict) else 0.0, 0.0))
        if not values:
            return None
        return sum(values) / float(len(values))

    def trend_flag(self, profile_path: Any, payload: Any) -> bool:
        payload_obj = payload if isinstance(payload, dict) else {}
        short_win = payload_obj.get("short", "1d")
        long_win = payload_obj.get("long", "7d")
        metric = str(payload_obj.get("metric") or "avg").strip().lower()
        source = payload_obj.get("source", profile_path)

        short_val = self.window_avg(source, short_win) if metric == "avg" else self.window_sum(source, short_win)
        long_val = self.window_avg(source, long_win) if metric == "avg" else self.window_sum(source, long_win)
        short_num = self.to_number(short_val)
        long_num = self.to_number(long_val)
        if short_num is None or long_num is None:
            return False
        return float(short_num) > float(long_num)

    def anomaly_detection(
        self,
        profile_path: Any,
        unique_key: Any,
        value: Any,
        txn_time: Any = None,
        payload: Any = None,
        options: Any = None,
    ) -> Dict[str, Any]:
        accepted, is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )

        payload_obj = payload if isinstance(payload, dict) else {}
        event_dt = self._safe_time(txn_time if txn_time is not None else payload_obj.get("time")) or self.current_event_dt
        event_ts = float(event_dt.timestamp())
        amount_num = self.to_number(value)

        partition_by = payload_obj.get("partition_by", payload_obj.get("group_by"))
        partition_key_arg = payload_obj.get("partition_key", payload_obj.get("partition"))
        partition_key, partition_values = self._compose_partition_key(partition_by, partition_key_arg)

        window_label, window_sec = _parse_window_seconds(str(payload_obj.get("window") or "30d"))
        allowed_lateness_sec = self._resolve_allowed_lateness_seconds(payload_obj, options, default_seconds=0)
        min_count = max(1, _safe_int(payload_obj.get("min_count"), 5))
        z_threshold = max(0.0, _safe_float(payload_obj.get("z_threshold"), 2.0))
        very_high_z_threshold = max(z_threshold, _safe_float(payload_obj.get("very_high_z_threshold"), 3.0))
        method = str(payload_obj.get("method") or "zscore").strip().lower()
        if method not in {"zscore", "stddev"}:
            method = "zscore"

        state = self._get_metric_state(profile_path)
        partitions = state.get("partitions")
        if not isinstance(partitions, dict):
            partitions = {}
            state["partitions"] = partitions

        part_state = partitions.get(partition_key)
        if not isinstance(part_state, dict):
            part_state = {
                "events": [],
                "max_event_ts": None,
                "late_dropped": 0,
                "last": None,
            }
            partitions[partition_key] = part_state

        raw_events = part_state.get("events")
        events = raw_events if isinstance(raw_events, list) else []
        max_event_ts_raw = part_state.get("max_event_ts")
        max_event_ts = _safe_float(max_event_ts_raw, event_ts) if max_event_ts_raw is not None else None

        is_new_txn = False
        late_event_ignored = False
        if accepted and amount_num is not None:
            if max_event_ts is not None and event_ts < (max_event_ts - float(allowed_lateness_sec)):
                late_event_ignored = True
                part_state["late_dropped"] = _safe_int(part_state.get("late_dropped"), 0) + 1
            else:
                is_new_txn = True
                events.append({"ts": event_ts, "value": float(amount_num)})
                if max_event_ts is None or event_ts > max_event_ts:
                    max_event_ts = event_ts
                elif event_ts < max_event_ts:
                    events.sort(key=lambda entry: _safe_float((entry or {}).get("ts"), 0.0))

        if max_event_ts is None:
            if events:
                max_event_ts = _safe_float((events[-1] or {}).get("ts"), event_ts)
            else:
                max_event_ts = event_ts

        window_end_ts = max(max_event_ts, event_ts)
        window_cutoff = window_end_ts - float(window_sec)

        compacted: List[Dict[str, Any]] = []
        for item in events:
            if not isinstance(item, dict):
                continue
            ts_val = _safe_float(item.get("ts"), 0.0)
            if ts_val >= window_cutoff:
                compacted.append({"ts": ts_val, "value": _safe_float(item.get("value"), 0.0)})
        events = compacted

        part_state["events"] = events
        part_state["max_event_ts"] = max_event_ts

        values = [_safe_float(item.get("value"), 0.0) for item in events if isinstance(item, dict)]
        count = len(values)
        total = float(sum(values)) if values else 0.0
        avg = (total / float(count)) if count > 0 else None

        stddev = None
        if count > 1 and avg is not None:
            # Sample stddev to align with SQL-style STDDEV behavior.
            variance = sum([(val - avg) ** 2 for val in values]) / float(max(1, count - 1))
            stddev = math.sqrt(max(0.0, variance))
        elif count == 1:
            stddev = 0.0

        z_score = None
        if amount_num is not None and avg is not None and stddev is not None and stddev > 0:
            z_score = (float(amount_num) - float(avg)) / float(stddev)
        abs_z_score = abs(float(z_score)) if z_score is not None else None

        if count < min_count:
            flag = "INSUFFICIENT_DATA"
        elif stddev is None or stddev <= 0:
            flag = "NO_VARIATION"
        elif abs_z_score is None:
            flag = "NO_VALUE"
        elif abs_z_score >= very_high_z_threshold:
            flag = "VERY_HIGH_OUTLIER"
        elif abs_z_score >= z_threshold:
            flag = "OUTLIER"
        else:
            flag = "NORMAL"

        out = {
            "partition_key": partition_key,
            "partition_values": partition_values,
            "window": str(payload_obj.get("window") or "30d"),
            "window_label": window_label,
            "window_seconds": int(window_sec),
            "allowed_lateness_seconds": int(allowed_lateness_sec),
            "count": int(count),
            "sum": total,
            "avg": avg,
            "stddev": stddev,
            "z_score": z_score,
            "abs_z_score": abs_z_score,
            "is_outlier": flag in {"OUTLIER", "VERY_HIGH_OUTLIER"},
            "flag": flag,
            "min_count": int(min_count),
            "z_threshold": float(z_threshold),
            "very_high_z_threshold": float(very_high_z_threshold),
            "method": method,
            "window_start_time": _format_time(datetime.fromtimestamp(window_cutoff)),
            "window_end_time": _format_time(datetime.fromtimestamp(window_end_ts)),
            "is_new_txn": bool(is_new_txn),
            "is_duplicate": bool(is_dup),
            "late_event_ignored": bool(late_event_ignored),
            "late_dropped": _safe_int(part_state.get("late_dropped"), 0),
            "value": float(amount_num) if amount_num is not None else None,
        }
        if bool(is_new_txn):
            part_state["new_txn_count"] = _safe_int(part_state.get("new_txn_count"), 0) + 1
            if flag in {"OUTLIER", "VERY_HIGH_OUTLIER"}:
                part_state["outlier_count"] = _safe_int(part_state.get("outlier_count"), 0) + 1
            if flag == "VERY_HIGH_OUTLIER":
                part_state["very_high_outlier_count"] = _safe_int(part_state.get("very_high_outlier_count"), 0) + 1
        part_state["last"] = out
        return out

    def anomaly_group_aggregate(
        self,
        profile_path: Any,
        unique_key: Any,
        value: Any,
        txn_time: Any = None,
        payload: Any = None,
        options: Any = None,
    ) -> Dict[str, Any]:
        payload_obj = payload if isinstance(payload, dict) else {}
        current = self.anomaly_detection(
            profile_path,
            unique_key,
            value,
            txn_time,
            payload_obj,
            options,
        )

        state = self._get_metric_state(profile_path)
        partitions_state = state.get("partitions")
        if not isinstance(partitions_state, dict):
            partitions_state = {}

        include_partitions = _coerce_bool(payload_obj.get("include_partitions"), True)
        include_current = _coerce_bool(payload_obj.get("include_current"), True)
        include_summary = _coerce_bool(payload_obj.get("include_summary"), True)
        limit = max(0, _safe_int(payload_obj.get("limit"), 0))
        sort_by = str(payload_obj.get("sort_by") or "partition_key").strip().lower()

        rows: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = []
        for part_key, part_state in partitions_state.items():
            if not isinstance(part_state, dict):
                continue
            last = part_state.get("last")
            if not isinstance(last, dict):
                continue
            row = {
                "count": _safe_int(last.get("count"), 0),
                "sum": _safe_float(last.get("sum"), 0.0),
                "avg": last.get("avg"),
                "stddev": last.get("stddev"),
                "last_value": last.get("value"),
                "last_z_score": last.get("z_score"),
                "last_abs_z_score": last.get("abs_z_score"),
                "last_flag": last.get("flag"),
                "is_outlier": _coerce_bool(last.get("is_outlier"), False),
                "outlier_count": _safe_int(part_state.get("outlier_count"), 0),
                "very_high_outlier_count": _safe_int(part_state.get("very_high_outlier_count"), 0),
                "new_txn_count": _safe_int(part_state.get("new_txn_count"), 0),
                "late_dropped": _safe_int(part_state.get("late_dropped"), 0),
                "window_start_time": last.get("window_start_time"),
                "window_end_time": last.get("window_end_time"),
                "partition_values": last.get("partition_values"),
            }
            rows.append((str(part_key), row, last))

        def _severity_rank(flag_text: Any) -> int:
            text = str(flag_text or "").strip().upper()
            if text == "VERY_HIGH_OUTLIER":
                return 3
            if text == "OUTLIER":
                return 2
            if text == "NO_VARIATION":
                return 1
            return 0

        if sort_by in {"severity", "outlier"}:
            rows.sort(
                key=lambda item: (
                    -_severity_rank((item[2] or {}).get("flag")),
                    -_safe_float((item[2] or {}).get("abs_z_score"), 0.0),
                    item[0],
                )
            )
        elif sort_by in {"abs_z", "zscore", "z_score"}:
            rows.sort(
                key=lambda item: (
                    -_safe_float((item[2] or {}).get("abs_z_score"), 0.0),
                    item[0],
                )
            )
        else:
            rows.sort(key=lambda item: item[0])

        if limit > 0:
            rows = rows[:limit]

        partition_out: Dict[str, Any] = {}
        if include_partitions:
            for key, row, _last in rows:
                partition_out[key] = row

        summary = {
            "partition_count": len(rows),
            "outlier_partitions": 0,
            "very_high_outlier_partitions": 0,
            "insufficient_data_partitions": 0,
            "no_variation_partitions": 0,
            "late_dropped_total": 0,
            "new_txn_total": 0,
            "window": current.get("window"),
            "generated_at": self.current_event_time,
        }
        for _key, row, last in rows:
            flag = str((last or {}).get("flag") or "").strip().upper()
            if flag in {"OUTLIER", "VERY_HIGH_OUTLIER"}:
                summary["outlier_partitions"] = _safe_int(summary.get("outlier_partitions"), 0) + 1
            if flag == "VERY_HIGH_OUTLIER":
                summary["very_high_outlier_partitions"] = _safe_int(summary.get("very_high_outlier_partitions"), 0) + 1
            if flag == "INSUFFICIENT_DATA":
                summary["insufficient_data_partitions"] = _safe_int(summary.get("insufficient_data_partitions"), 0) + 1
            if flag == "NO_VARIATION":
                summary["no_variation_partitions"] = _safe_int(summary.get("no_variation_partitions"), 0) + 1
            summary["late_dropped_total"] = _safe_int(summary.get("late_dropped_total"), 0) + _safe_int(row.get("late_dropped"), 0)
            summary["new_txn_total"] = _safe_int(summary.get("new_txn_total"), 0) + _safe_int(row.get("new_txn_count"), 0)

        out: Dict[str, Any] = {}
        if include_summary:
            out["summary"] = summary
        if include_partitions:
            out["partitions"] = partition_out
        if include_current:
            out["current"] = current
        return out

    def sequence_update(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )

        payload_obj = payload if isinstance(payload, dict) else {"event": payload}
        event = str(payload_obj.get("event") or "").strip()
        pattern_text = str(payload_obj.get("pattern") or "").strip()
        tail_size = max(3, _safe_int(payload_obj.get("tail_size"), 20))

        state = self._get_metric_state(profile_path)
        tail = state.get("tail")
        if not isinstance(tail, list):
            tail = []

        if accepted and event:
            tail.append(event)
            if len(tail) > tail_size:
                tail = tail[-tail_size:]

        state["tail"] = tail
        pattern_parts = [part.strip() for part in pattern_text.split(">") if part.strip()]
        hit = False
        if pattern_parts and len(tail) >= len(pattern_parts):
            hit = tail[-len(pattern_parts):] == pattern_parts
        if hit:
            state["hit_count"] = _safe_int(state.get("hit_count"), 0) + 1

        return {
            "last_events": tail[-tail_size:],
            "pattern_hit": hit,
            "hit_count": _safe_int(state.get("hit_count"), 0),
        }

    def loop_flag(self, profile_path: Any, sequence_ref: Any) -> bool:
        seq_obj = self._lookup_value(sequence_ref, None)
        if isinstance(seq_obj, dict):
            if _coerce_bool(seq_obj.get("pattern_hit"), False):
                return True
            return _safe_int(seq_obj.get("hit_count"), 0) > 1
        return _coerce_bool(seq_obj, False)

    def repeated_sequence(
        self,
        profile_path: Any,
        unique_key: Any,
        event_value: Any,
        txn_ids_path: Any = None,
        tail_size: Any = 20,
        options: Any = None,
    ) -> Dict[str, Any]:
        tail_cap = max(5, _safe_int(tail_size, 20))
        event = str(event_value if event_value is not None else "UNKNOWN")
        effective_unique_key, normalize_mode, _on_dup, _on_invalid = self._resolve_dedupe_controls(
            unique_key,
            options,
        )

        accepted = True
        is_dup = False
        token = ""
        if isinstance(txn_ids_path, str) and txn_ids_path.strip():
            token = self.stable_json_token(_normalise_unique_key(effective_unique_key, normalize_mode))
            txn_ids_value = self.profile_prev(txn_ids_path, [])
            txn_ids = txn_ids_value if isinstance(txn_ids_value, list) else []
            dedupe_set = {
                self.stable_json_token(_normalise_unique_key(item, normalize_mode))
                for item in txn_ids
            }
            if token in dedupe_set:
                accepted = False
                is_dup = True
        else:
            accepted, is_dup, token = self._dedupe_gate_with_options(
                profile_path,
                unique_key,
                options,
            )

        state = self._get_metric_state(profile_path)
        service_counts = state.get("service_counts")
        if not isinstance(service_counts, dict):
            service_counts = {}

        sequence_tail = state.get("sequence_tail")
        if not isinstance(sequence_tail, list):
            sequence_tail = []

        repeat_events = _safe_int(state.get("repeat_events"), 0)
        total = _safe_int(state.get("total_txn_count"), 0)
        last_service = state.get("last_service")
        current_streak_service = state.get("current_streak_service")
        current_streak_count = _safe_int(state.get("current_streak_count"), 0)
        max_streak_count = _safe_int(state.get("max_streak_count"), 0)

        if accepted:
            total += 1
            service_counts[event] = _safe_int(service_counts.get(event), 0) + 1
            if str(last_service) == event:
                repeat_events += 1

            if str(current_streak_service) == event:
                current_streak_count += 1
            else:
                current_streak_service = event
                current_streak_count = 1
            max_streak_count = max(max_streak_count, current_streak_count)
            last_service = event

            sequence_tail.append(event)
            if len(sequence_tail) > tail_cap:
                sequence_tail = sequence_tail[-tail_cap:]

        dominant_service = None
        dominant_count = -1
        for key, count in service_counts.items():
            c = _safe_int(count, 0)
            if c > dominant_count:
                dominant_service = key
                dominant_count = c

        state["service_counts"] = service_counts
        state["sequence_tail"] = sequence_tail
        state["total_txn_count"] = total
        state["count_total"] = total
        state["repeat_events"] = repeat_events
        state["last_service"] = last_service
        state["current_streak_service"] = current_streak_service
        state["current_streak_count"] = current_streak_count
        state["max_streak_count"] = max_streak_count
        state["last_txn_id"] = effective_unique_key

        repeat_rate = (float(repeat_events) / float(total)) if total > 0 else 0.0
        return {
            "is_new_txn": bool(accepted),
            "last_txn_id": effective_unique_key,
            "total_txn_count": total,
            "count_total": total,
            "repeat_events": repeat_events,
            "repeat_rate": repeat_rate,
            "last_service": last_service,
            "current_streak_service": current_streak_service,
            "current_streak_count": current_streak_count,
            "max_streak_count": max_streak_count,
            "service_counts": service_counts,
            "dominant_service": dominant_service,
            "dominant_service_count": max(0, dominant_count),
            "sequence_tail": sequence_tail[-tail_cap:],
        }

    def unique_sequence(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> Dict[str, Any]:
        return self.sequence_update(profile_path, unique_key, payload, options)

    def unique_geo(self, profile_path: Any, unique_key: Any, payload: Any, options: Any = None) -> Dict[str, Any]:
        return self.geo_variance(profile_path, unique_key, payload, options)

    # Partition wrappers.
    def partition_count(self, profile_path: Any, unique_key: Any, partition_key: Any, value: Any = 1, options: Any = None) -> Dict[str, Any]:
        return self.unique_group_aggregate(profile_path, unique_key, partition_key, "count", value, options)

    def partition_avg(self, profile_path: Any, unique_key: Any, partition_key: Any, value: Any, options: Any = None) -> Dict[str, Any]:
        return self.unique_group_aggregate(profile_path, unique_key, partition_key, "avg", value, options)

    def partition_stddev(self, profile_path: Any, unique_key: Any, partition_key: Any, value: Any, options: Any = None) -> Dict[str, Any]:
        return self.unique_group_aggregate(profile_path, unique_key, partition_key, "std", value, options)

    def partition_percentile(
        self,
        profile_path: Any,
        unique_key: Any,
        partition_key: Any,
        value: Any,
        percentile: Any = 95,
        options: Any = None,
    ) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        group_state = self._get_group_state(profile_path)
        values_map = group_state.get("values_sorted")
        if not isinstance(values_map, dict):
            values_map = {}
            # Legacy migration path: if previous unsorted values exist, normalize once.
            legacy_values = group_state.get("values")
            if isinstance(legacy_values, dict):
                for lk, lv in legacy_values.items():
                    if not isinstance(lv, list):
                        continue
                    normalized = sorted([_safe_float(x, 0.0) for x in lv])
                    values_map[str(lk)] = normalized
            group_state["values_sorted"] = values_map

        p_key = str(partition_key if partition_key is not None else "UNKNOWN")
        vals = values_map.get(p_key)
        if not isinstance(vals, list):
            vals = []
        if accepted:
            n = self.to_number(value)
            if n is not None:
                insort(vals, float(n))
        values_map[p_key] = vals

        p = max(0.0, min(100.0, _safe_float(percentile, 95.0)))
        cached_p = _safe_float(group_state.get("percentile_p"), 95.0)
        cached_out = group_state.get("percentile_out")
        p_changed = abs(cached_p - p) > 1e-12

        if not isinstance(cached_out, dict) or p_changed:
            rebuilt: Dict[str, Any] = {}
            for key, arr in values_map.items():
                if not isinstance(arr, list) or not arr:
                    rebuilt[key] = None
                    continue
                idx = int(round((p / 100.0) * (len(arr) - 1)))
                rebuilt[key] = arr[max(0, min(idx, len(arr) - 1))]
            group_state["percentile_p"] = p
            group_state["percentile_out"] = rebuilt
            return rebuilt

        out_map = dict(cached_out)
        if accepted and isinstance(vals, list) and vals:
            idx = int(round((p / 100.0) * (len(vals) - 1)))
            out_map[p_key] = vals[max(0, min(idx, len(vals) - 1))]
            group_state["percentile_out"] = out_map
            return out_map
        return cached_out

    def partition_zscore(self, profile_path: Any, unique_key: Any, partition_key: Any, value: Any, options: Any = None) -> Dict[str, Any]:
        accepted, _is_dup, _token = self._dedupe_gate_with_options(
            profile_path,
            unique_key,
            options,
        )
        group_state = self._get_group_state(profile_path)
        buckets = group_state.get("buckets")
        if not isinstance(buckets, dict):
            buckets = {}
            group_state["buckets"] = buckets

        current_partition = str(partition_key if partition_key is not None else "UNKNOWN")
        bucket = buckets.get(current_partition)
        if not isinstance(bucket, dict):
            bucket = {}
            buckets[current_partition] = bucket

        if accepted:
            self._update_stats_state(bucket, value)

        out_cache = group_state.get("zscore_out")
        out_map: Dict[str, Any] = dict(out_cache) if isinstance(out_cache, dict) else {}
        current_value = self.to_number(value)
        stats = self._stats_output(bucket)
        mean = stats.get("avg")
        std = stats.get("std")
        if current_value is None or mean is None or std in (None, 0):
            out_map[current_partition] = None
        else:
            out_map[current_partition] = (float(current_value) - float(mean)) / float(std)
        out_map["value"] = out_map.get(current_partition)
        out_map["partition"] = current_partition
        group_state["zscore_out"] = out_map
        return out_map

    def sigma_threshold(self, profile_path: Any, value: Any, mean: Any, std: Any, sigma: Any = 3) -> bool:
        v = self.to_number(value)
        m = self.to_number(mean)
        s = self.to_number(std)
        k = max(0.1, _safe_float(sigma, 3.0))
        if v is None or m is None or s is None or float(s) <= 0:
            return False
        return abs(float(v) - float(m)) >= (k * float(s))

    def threshold_reason(self, profile_path: Any, value: Any, thresholds: Any) -> str:
        n = self.to_number(value)
        if n is None:
            return "NO_VALUE"
        if isinstance(thresholds, dict):
            high = self.to_number(thresholds.get("high"))
            low = self.to_number(thresholds.get("low"))
            if high is not None and float(n) >= float(high):
                return "HIGH"
            if low is not None and float(n) <= float(low):
                return "LOW"
            return "NORMAL"
        t = self.to_number(thresholds)
        if t is None:
            return "NORMAL"
        return "ABOVE_THRESHOLD" if float(n) >= float(t) else "NORMAL"

    def zscore_threshold_flag(self, profile_path: Any, zscore_value: Any, threshold: Any = 3) -> bool:
        z = self.to_number(zscore_value)
        t = max(0.1, _safe_float(threshold, 3.0))
        if z is None:
            return False
        return abs(float(z)) >= t

    # v2 overloads for v1 helpers (backward-compatible).
    def inc(self, path_or_value: Any, amount: Any = 1, default: Any = 0, dedupe_key: Any = None, options: Any = None) -> Any:
        if dedupe_key is None and options is None:
            return self.v1_inc(path_or_value, amount, default)
        if not isinstance(path_or_value, str):
            return self.v1_inc(path_or_value, amount, default)
        return self.unique_sum(path_or_value, dedupe_key, amount, options)

    def map_inc(self, path_or_map: Any, key: Any, amount: Any = 1, default: Any = 0, dedupe_key: Any = None, options: Any = None) -> Dict[str, Any]:
        if dedupe_key is None and options is None:
            return self.v1_map_inc(path_or_map, key, amount, default)
        path = path_or_map if isinstance(path_or_map, str) else "map"
        return self.unique_map(path, dedupe_key, key, amount, options)

    def append_unique(self, path_or_values: Any, value: Any = None, max_items: Any = None, normalize: Any = True) -> List[Any]:
        return self.v1_append_unique(path_or_values, value, max_items, normalize)

    def rolling_update(self, name: Any, value: Any, txn_time: Any = None, windows: Any = None, retention_days: Any = None) -> Dict[str, Any]:
        return self.v1_rolling_update(name, value, txn_time, windows, retention_days)

    def build_registry(self) -> Dict[str, Any]:
        registry = {
            # Canonical.
            "unique_metrics": self.unique_metrics,
            "unique_group_aggregate": self.unique_group_aggregate,
            "unique_signal": self.unique_signal,
            # Main helper surface.
            "unique_count": self.unique_count,
            "unique_sum": self.unique_sum,
            "unique_avg": self.unique_avg,
            "unique_min": self.unique_min,
            "unique_max": self.unique_max,
            "unique_stats": self.unique_stats,
            "metric_get": self.metric_get,
            "unique_rate": self.unique_rate,
            "unique_ratio": self.unique_ratio,
            "unique_flag": self.unique_flag,
            "unique_map": self.unique_map,
            "top_key": self.top_key,
            "unique_interval": self.unique_interval,
            "unique_velocity": self.unique_velocity,
            "burst_flag": self.burst_flag,
            "entropy_update": self.entropy_update,
            "min_update": self.min_update,
            "max_update": self.max_update,
            "repetition_rate": self.repetition_rate,
            "change_rate": self.change_rate,
            "geo_variance": self.geo_variance,
            "geo_conflict_flag": self.geo_conflict_flag,
            "unique_rolling": self.unique_rolling,
            "unique_window": self.unique_window,
            "window_sum": self.window_sum,
            "window_avg": self.window_avg,
            "trend_flag": self.trend_flag,
            "anomaly_detection": self.anomaly_detection,
            "anamoly_detection": self.anomaly_detection,
            "anomaly_group_aggregate": self.anomaly_group_aggregate,
            "anamoly_group_aggregate": self.anomaly_group_aggregate,
            "sequence_update": self.sequence_update,
            "loop_flag": self.loop_flag,
            "repeated_sequence": self.repeated_sequence,
            "unique_sequence": self.unique_sequence,
            "unique_geo": self.unique_geo,
            "unique_metric": self.unique_metric,
            # Partition / threshold helpers.
            "partition_count": self.partition_count,
            "partition_avg": self.partition_avg,
            "partition_stddev": self.partition_stddev,
            "partition_percentile": self.partition_percentile,
            "partition_zscore": self.partition_zscore,
            "sigma_threshold": self.sigma_threshold,
            "threshold_reason": self.threshold_reason,
            "zscore_threshold_flag": self.zscore_threshold_flag,
            # Backward-compatible overloaded helpers.
            "inc": self.inc,
            "map_inc": self.map_inc,
            "append_unique": self.append_unique,
            "rolling_update": self.rolling_update,
        }
        return registry


def build_fns_v2_registry(
    *,
    profile_root: str,
    working_doc: Dict[str, Any],
    working_meta: Dict[str, Any],
    custom_values: Dict[str, Any],
    resolve_profile_path: Callable[[Any], str],
    profile_prev: Callable[[Any, Any], Any],
    stable_json_token: Callable[[Any], str],
    to_number: Callable[[Any], Optional[float]],
    parse_event_time: Callable[[Any], Optional[datetime]],
    current_event_dt: datetime,
    current_event_time: str,
    entity_key: Any,
    json_safe_value: Callable[[Any], Any],
    v1_inc: Callable[[Any, Any, Any], Any],
    v1_map_inc: Callable[[Any, Any, Any, Any], Dict[str, Any]],
    v1_append_unique: Callable[[Any, Any, Any, Any], List[Any]],
    v1_rolling_update: Callable[[Any, Any, Any, Any, Any], Dict[str, Any]],
    default_dedupe_options: Optional[Dict[str, Any]] = None,
    row_dedupe_gate_enabled: bool = False,
) -> Dict[str, Any]:
    runtime = _FnsV2Runtime(
        profile_root=profile_root,
        working_doc=working_doc,
        working_meta=working_meta,
        custom_values=custom_values,
        resolve_profile_path=resolve_profile_path,
        profile_prev=profile_prev,
        stable_json_token=stable_json_token,
        to_number=to_number,
        parse_event_time=parse_event_time,
        current_event_dt=current_event_dt,
        current_event_time=current_event_time,
        entity_key=entity_key,
        json_safe_value=json_safe_value,
        v1_inc=v1_inc,
        v1_map_inc=v1_map_inc,
        v1_append_unique=v1_append_unique,
        v1_rolling_update=v1_rolling_update,
        default_dedupe_options=default_dedupe_options,
        row_dedupe_gate_enabled=row_dedupe_gate_enabled,
    )
    return runtime.build_registry()
