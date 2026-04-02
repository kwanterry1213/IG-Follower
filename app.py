import time
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlencode

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from zoneinfo import ZoneInfo


TARGET_INSTAGRAM_URL = "https://www.instagram.com/jk_macau/"
SCAN_LOG_COLUMNS = ["timestamp", "staff_id", "device_id"]
STAFF_DIRECTORY_FILE = "staff_directory.csv"
HK_TZ = ZoneInfo("Asia/Hong_Kong")  # 澳門/香港時區


def _get_query_params() -> Tuple[Optional[str], Optional[str]]:
    """
    Streamlit query param extraction with fallbacks across versions.
    """
    sid: Optional[str] = None
    fid: Optional[str] = None

    # Newer Streamlit exposes `st.query_params` (dict-like).
    if hasattr(st, "query_params"):
        try:
            params = st.query_params
            sid_raw = params.get("sid")
            fid_raw = params.get("fid")
            sid = sid_raw if isinstance(sid_raw, str) else (sid_raw[0] if sid_raw else None)
            fid = fid_raw if isinstance(fid_raw, str) else (fid_raw[0] if fid_raw else None)
            return sid, fid
        except Exception:
            # Fall through to legacy API.
            pass

    # Legacy Streamlit API.
    try:
        params = st.experimental_get_query_params()
        sid_list = params.get("sid", [])
        fid_list = params.get("fid", [])
        sid = sid_list[0] if sid_list else None
        fid = fid_list[0] if fid_list else None
    except Exception:
        sid, fid = None, None

    return sid, fid


def _ensure_csv_initialized(csv_path: Path) -> None:
    if csv_path.exists():
        return
    df = pd.DataFrame(columns=SCAN_LOG_COLUMNS)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def _load_scan_log(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        _ensure_csv_initialized(csv_path)
    # Use dtype=str for stable parsing of ids; parse timestamp separately.
    try:
        df = pd.read_csv(csv_path, dtype=str)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame(columns=SCAN_LOG_COLUMNS)

    if df.empty:
        for col in SCAN_LOG_COLUMNS:
            if col not in df.columns:
                df[col] = pd.Series(dtype=str)
        return df[SCAN_LOG_COLUMNS]

    # Ensure required columns exist.
    for col in SCAN_LOG_COLUMNS:
        if col not in df.columns:
            df[col] = pd.Series(dtype=str)
    return df[SCAN_LOG_COLUMNS]


def _parse_utc_timestamp(series: pd.Series) -> pd.Series:
    # Parse into timezone-aware UTC datetimes (or NaT).
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt


def _is_duplicate_scan(
    df: pd.DataFrame, *, staff_id: str, device_id: str, now_utc: datetime, window_hours: int = 24
) -> bool:
    cutoff = now_utc - timedelta(hours=window_hours)
    ts = _parse_utc_timestamp(df["timestamp"])
    mask = (df["staff_id"].astype(str) == str(staff_id)) & (df["device_id"].astype(str) == str(device_id))
    if ts.notna().any():
        mask = mask & (ts >= cutoff)
    return bool(mask.any())


def _append_scan(csv_path: Path, *, staff_id: str, device_id: str) -> None:
    now_utc = datetime.now(timezone.utc)
    df = _load_scan_log(csv_path)

    device_id = str(device_id)
    staff_id = str(staff_id)

    if _is_duplicate_scan(df, staff_id=staff_id, device_id=device_id, now_utc=now_utc, window_hours=24):
        return

    new_row = {
        "timestamp": now_utc.isoformat(),
        "staff_id": staff_id,
        "device_id": device_id,
    }
    df_to_write = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df_to_write.to_csv(csv_path, index=False, encoding="utf-8-sig")


def _redirect_via_meta_refresh(url: str) -> None:
    # Immediately redirect on the client.
    st.markdown(
        f'<meta http-equiv="refresh" content="0;url={url}">',
        unsafe_allow_html=True,
    )
    st.stop()


def _inject_fingerprintjs_redirect(sid: str) -> None:
    """
    When `sid` exists but `fid` is missing, load FingerprintJS and redirect with &fid=<visitorId>.
    """
    sid_json = json.dumps(str(sid), ensure_ascii=False)
    html = r"""
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
  </head>
  <body>
    <script type="module">
      import * as FP from 'https://openfpcdn.io/fingerprintjs/v4';

      const FingerprintJS = FP.default ?? FP;

      try {
        const run = async () => {
          const fp = await FingerprintJS.load();
          const result = await fp.get();
          const visitorId = result.visitorId;

          // Streamlit components run in a sandboxed iframe, so top navigation is blocked.
          // Open the target URL in a new tab; then the server will handle sid+fid.
          const sidVal = __SID_JSON__;
          const target = '/?sid=' + encodeURIComponent(String(sidVal)) + '&fid=' + encodeURIComponent(visitorId);
          window.open(target, '_blank');
        };
        run();
      } catch (e) {
        console.error('FingerprintJS error:', e);
      }
    </script>
  </body>
</html>
"""
    html = html.replace("__SID_JSON__", sid_json)
    components.html(html, height=0)
    st.stop()


def _compute_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute today's leaderboard using HK/Macau local date (Asia/Hong_Kong).
    """
    if df.empty:
        return pd.DataFrame(columns=["staff_id", "valid_scan_count"])

    # Count valid scans (rows) per staff_id for today's local day.
    df["staff_id"] = df["staff_id"].astype(str)

    now_local = datetime.now(HK_TZ)
    start_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=HK_TZ)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    ts_utc = _parse_utc_timestamp(df["timestamp"])
    mask = ts_utc.notna() & (ts_utc >= start_utc) & (ts_utc < end_utc)
    df_today = df.loc[mask]

    counts = df_today.groupby("staff_id").size().reset_index(name="valid_scan_count")
    counts = counts.sort_values("valid_scan_count", ascending=False).reset_index(drop=True)
    return counts


def _load_staff_directory(staff_dir_path: Path) -> pd.DataFrame:
    """
    staff_directory.csv schema (UTF-8):
      - staff_id, staff_name
    or（兼容）：
      - ID, 員工名稱
    """
    if not staff_dir_path.exists():
        return pd.DataFrame(columns=["staff_id", "staff_name"])
    try:
        staff_df = pd.read_csv(staff_dir_path, dtype=str, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=["staff_id", "staff_name"])

    if staff_df.empty:
        return pd.DataFrame(columns=["staff_id", "staff_name"])

    # Normalize column names to staff_id / staff_name.
    if "staff_id" not in staff_df.columns and "ID" in staff_df.columns:
        staff_df["staff_id"] = staff_df["ID"]
    if "staff_name" not in staff_df.columns and "員工名稱" in staff_df.columns:
        staff_df["staff_name"] = staff_df["員工名稱"]

    if "staff_id" not in staff_df.columns:
        staff_df["staff_id"] = pd.Series(dtype=str)
    if "staff_name" not in staff_df.columns:
        staff_df["staff_name"] = pd.Series(dtype=str)

    return staff_df[["staff_id", "staff_name"]]


st.set_page_config(
    page_title="Staff Promotion Leaderboard",
    page_icon="🏆",
    layout="wide",
)

csv_path = Path(__file__).resolve().parent / "scan_log.csv"
_ensure_csv_initialized(csv_path)
staff_dir_path = Path(__file__).resolve().parent / STAFF_DIRECTORY_FILE

sid, fid = _get_query_params()

if sid and not fid:
    _inject_fingerprintjs_redirect(sid)

if sid and fid:
    # Record scan (with 24h device_id duplicate protection), then redirect to Instagram.
    try:
        _append_scan(csv_path, staff_id=sid, device_id=fid)
    finally:
        _redirect_via_meta_refresh(TARGET_INSTAGRAM_URL)

# Public leaderboard view
df = _load_scan_log(csv_path)
leaderboard_placeholder = st.empty()

while True:
    with leaderboard_placeholder.container():
        st.header("Staff Promotion Leaderboard")
        st.subheader("今日排行榜（澳門/香港時區）")
        if df.empty:
            st.write("目前沒有任何有效掃描紀錄。")
        else:
            # Always reload the latest data each loop.
            df = _load_scan_log(csv_path)
            leaderboard_df = _compute_leaderboard(df)

            if leaderboard_df.empty:
                st.write("目前今日沒有任何有效掃描紀錄。")
            else:
                staff_df = _load_staff_directory(staff_dir_path)
                if staff_df.empty:
                    display_df = leaderboard_df.copy()
                    display_df["員工名稱"] = display_df["staff_id"]
                    display_df = display_df.rename(
                        columns={"staff_id": "ID", "valid_scan_count": "當日次數總數"}
                    )
                else:
                    merged = leaderboard_df.merge(staff_df, how="left", on="staff_id")
                    merged["員工名稱"] = merged["staff_name"].fillna(merged["staff_id"])
                    display_df = merged.rename(
                        columns={"staff_id": "ID", "valid_scan_count": "當日次數總數"}
                    )

                display_df = (
                    display_df[["ID", "員工名稱", "當日次數總數"]]
                    .sort_values("當日次數總數", ascending=False)
                    .reset_index(drop=True)
                )

                # DataFrame with styling highlight for the top performer.
                top_count = display_df.loc[0, "當日次數總數"]

                def highlight_top(row: pd.Series) -> list:
                    if int(row.name) == 0:
                        return ["background-color: #fff2cc; font-weight: 700;"] * len(row)
                    return [""] * len(row)

                styled = display_df.style.apply(highlight_top, axis=1)
                st.dataframe(styled, use_container_width=True)

                # Bar chart: employee name vs today's total scans.
                chart_series = (
                    display_df.groupby("員工名稱")["當日次數總數"].sum().sort_values(ascending=False)
                )
                st.bar_chart(chart_series)

                st.caption(
                    f"Top performer: {display_df.loc[0, '員工名稱']}（{top_count} 次/今日）"
                )

    time.sleep(10)
    st.rerun()

