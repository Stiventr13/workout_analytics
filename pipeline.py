import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

LB_TO_KG = 0.453592

def standardize_text(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
    )

def build_fact_workout_sets(raw_logs: pd.DataFrame, dim_exercises: pd.DataFrame) -> pd.DataFrame:
    raw = raw_logs.copy()
    dim = dim_exercises.copy()

    raw.columns = (
        raw.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    dim.columns = (
        dim.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    rename_map = {
        "perceived_effort_(rpe)": "perceived_effort_rpe",
        "perceived_technique_quality": "perceived_technique_quality"
    }
    raw = raw.rename(columns=rename_map)

    if "exercise" in raw.columns:
        raw["exercise"] = standardize_text(raw["exercise"])
    if "exercise_name" in dim.columns:
        dim["exercise_name"] = standardize_text(dim["exercise_name"])

    if "routine_day" in raw.columns:
        raw["routine_day"] = raw["routine_day"].astype(str).str.strip()

    if "weight_unit" in raw.columns:
        raw["weight_unit"] = standardize_text(raw["weight_unit"]).replace({
            "kgs": "kg",
            "kilograms": "kg",
            "kilogram": "kg",
            "lbs": "lb",
            "pounds": "lb",
            "pound": "lb"
        })
    else:
        raw["weight_unit"] = pd.NA

    numeric_cols = [
        "set_number", "weight", "reps", "rir", "rest_seconds", "set_seconds",
        "perceived_technique_quality", "perceived_effort_rpe", "fatigue_level"
    ]

    for col in numeric_cols:
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors="coerce")

    if "session_date" in raw.columns:
        raw["session_date"] = pd.to_datetime(raw["session_date"], errors="coerce")

    fact = raw.merge(
        dim,
        how="left",
        left_on="exercise",
        right_on="exercise_name",
        validate="many_to_one"
    )

    if "default_unit" in fact.columns:
        fact["default_unit"] = standardize_text(fact["default_unit"]).replace({
            "kgs": "kg",
            "kilograms": "kg",
            "kilogram": "kg",
            "lbs": "lb",
            "pounds": "lb",
            "pound": "lb"
        })
        fact["weight_unit"] = fact["weight_unit"].fillna(fact["default_unit"])

    fact["weight_unit"] = fact["weight_unit"].fillna("kg")

    fact["weight_kg"] = fact["weight"]
    fact.loc[fact["weight_unit"] == "lb", "weight_kg"] = (
        fact.loc[fact["weight_unit"] == "lb", "weight"] * LB_TO_KG
    )
    fact["weight_unit_standard"] = "kg"

    fact["volume"] = fact["weight_kg"] * fact["reps"]
    fact["intensity"] = 10 - fact["rir"]
    fact["est_1rm"] = fact["weight_kg"] * (1 + (fact["reps"] / 30))

    iso_calendar = fact["session_date"].dt.isocalendar()
    fact["year"] = iso_calendar.year.astype("Int64")
    fact["week"] = iso_calendar.week.astype("Int64")
    fact["year_week"] = (
        fact["year"].astype("string") + "-W" +
        fact["week"].astype("string").str.zfill(2)
    )
    fact["week_start"] = (
        fact["session_date"] -
        pd.to_timedelta(fact["session_date"].dt.weekday, unit="d")
    )

    fact["session_date_str"] = fact["session_date"].dt.strftime("%Y-%m-%d")
    fact["session_id"] = fact["session_date_str"].astype(str) + "_" + fact["routine_day"].astype(str)
    fact["set_id"] = (
        fact["session_id"].astype(str) + "_" +
        fact["exercise_id"].astype(str) + "_set" +
        fact["set_number"].fillna(0).astype("Int64").astype(str)
    )

    final_cols = [
        "set_id", "session_id", "session_date", "week_start", "year", "week", "year_week",
        "routine_day", "exercise_id", "exercise_name", "muscle_group", "exercise_type",
        "laterality", "equipment", "set_number", "weight", "weight_unit", "weight_kg",
        "weight_unit_standard", "reps", "rir", "rest_seconds", "set_seconds", "notes",
        "perceived_technique_quality", "perceived_effort_rpe", "fatigue_level",
        "working_side", "volume", "intensity", "est_1rm"
    ]

    final_cols_existing = [col for col in final_cols if col in fact.columns]
    return fact[final_cols_existing].copy()

def make_serializable(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df.fillna("")

def main():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    spreadsheet_name = os.environ["SPREADSHEET_NAME"]

    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)

    sh = gc.open(spreadsheet_name)

    raw_ws = sh.worksheet("workout_logs_raw")
    dim_ws = sh.worksheet("dim_exercises")

    raw_logs = pd.DataFrame(raw_ws.get_all_records())
    dim_exercises = pd.DataFrame(dim_ws.get_all_records())

    fact_workout_sets = build_fact_workout_sets(raw_logs, dim_exercises)
    fact_workout_sets_export = make_serializable(fact_workout_sets)

    output_sheet_name = "fact_workout_sets"

    try:
        out_ws = sh.worksheet(output_sheet_name)
        out_ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        out_ws = sh.add_worksheet(title=output_sheet_name, rows=2000, cols=40)

    data_to_write = [fact_workout_sets_export.columns.tolist()] + fact_workout_sets_export.values.tolist()
    out_ws.resize(rows=len(data_to_write), cols=len(data_to_write[0]))
    out_ws.update("A1", data_to_write)

if __name__ == "__main__":
    main()
