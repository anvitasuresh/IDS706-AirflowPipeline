from __future__ import annotations
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil


OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = "netflix_user_watch"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# Helper for column-name variations
def _pick(df, candidates, required=True):
    for c in candidates:
        if c in df.columns:
            return c
    if required:
        raise KeyError(
            f"Expected one of {candidates} in columns: {list(df.columns)[:15]} ..."
        )
    return None


with DAG(
    dag_id="netflix_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    # 1) USERS

    @task()
    def fetch_users(output_dir: str = OUTPUT_DIR) -> str:
        import pandas as pd

        path = "/opt/airflow/data/users.csv"
        df = pd.read_csv(path)

        user_id = _pick(df, ["user_id", "userid", "userId"])
        age_col = _pick(df, ["age"], required=False)
        plan_col = _pick(df, ["subscription_plan", "plan", "tier"], required=False)
        spend_col = _pick(
            df, ["monthly_spend", "monthly_spending", "spend"], required=False
        )
        region_col = _pick(df, ["region", "country", "location"], required=False)
        active_col = _pick(df, ["is_active", "active", "churn_label"], required=False)

        keep_cols = [
            c
            for c in [user_id, age_col, plan_col, spend_col, region_col, active_col]
            if c
        ]
        df = df[keep_cols].drop_duplicates(subset=[user_id], keep="last")

        # Basic cleaning
        if age_col:
            df[age_col] = pd.to_numeric(df[age_col], errors="coerce")
            df.loc[(df[age_col] < 5) | (df[age_col] > 100), age_col] = None
        if spend_col:
            df[spend_col] = pd.to_numeric(df[spend_col], errors="coerce")
            df.loc[(df[spend_col] < 0) | (df[spend_col] > 1000), spend_col] = None
        if active_col:
            df[active_col] = (
                pd.to_numeric(df[active_col], errors="coerce").round().astype("Int64")
            )

        out = os.path.join(output_dir, "users_clean.csv")
        rename_map = {}
        if age_col:
            rename_map[age_col] = "age"
        if plan_col:
            rename_map[plan_col] = "subscription_plan"
        if spend_col:
            rename_map[spend_col] = "monthly_spend"
        if region_col:
            rename_map[region_col] = "region"
        if active_col:
            rename_map[active_col] = "is_active"
        rename_map[user_id] = "user_id"

        df = df.rename(columns=rename_map)
        df.to_csv(out, index=False)
        print(f"Users saved to {out} (rows={len(df)})")
        return out

    # 2) WATCH HISTORY

    @task()
    def fetch_watch_history(output_dir: str = OUTPUT_DIR) -> str:
        import pandas as pd

        path = "/opt/airflow/data/watch_history.csv"
        df = pd.read_csv(path)

        user_id = _pick(df, ["user_id", "userid", "userId"])
        dur_col = _pick(
            df,
            [
                "watch_duration_minutes",
                "duration_minutes",
                "minutes_watched",
                "watch_minutes",
                "duration",
            ],
        )
        comp_col = _pick(
            df, ["completion_rate", "progress", "percent_completed"], required=False
        )

        df[dur_col] = pd.to_numeric(df[dur_col], errors="coerce")
        df = df.dropna(subset=[dur_col])
        # Minutes -> hours
        df["watch_hours"] = df[dur_col] / 60.0
        # Cut extreme binge sessions
        df = df[(df["watch_hours"] >= 0) & (df["watch_hours"] <= 13)]

        if comp_col:
            df[comp_col] = pd.to_numeric(df[comp_col], errors="coerce")
            # Convert to 0-1
            if df[comp_col].dropna().median() > 1.5:
                df["completion_rate"] = df[comp_col] / 100.0
            else:
                df["completion_rate"] = df[comp_col]
        else:
            df["completion_rate"] = None

        # Aggregate to user-level engagement (sum hours, mean completion)
        agg = df.groupby(user_id, as_index=False).agg(
            watch_hours=("watch_hours", "sum"),
            completion_rate=("completion_rate", "mean"),
        )

        out = os.path.join(output_dir, "watch_history_clean.csv")
        agg = agg.rename(columns={user_id: "user_id"})
        agg.to_csv(out, index=False)
        print(f"Watch history saved to {out} (rows={len(agg)})")
        return out

    # 3) MERGE

    @task()
    def merge_csvs(
        users_path: str, history_path: str, output_dir: str = OUTPUT_DIR
    ) -> str:
        import pandas as pd

        u = pd.read_csv(users_path)
        h = pd.read_csv(history_path)

        merged = pd.merge(u, h, on="user_id", how="inner")

        # Make sure numeric types are numeric
        for col in [
            "age",
            "monthly_spend",
            "watch_hours",
            "completion_rate",
            "is_active",
        ]:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")

        merged_path = os.path.join(output_dir, "merged_data.csv")
        merged.to_csv(merged_path, index=False)
        print(f"Merged data saved to {merged_path} (rows={len(merged)})")
        return merged_path

    # 4) LOAD TO POSTGRES

    @task()
    def load_csv_to_pg(
        conn_id: str, csv_path: str, table: str = TARGET_TABLE, append: bool = False
    ) -> int:
        import pandas as pd

        df = pd.read_csv(csv_path)
        if df.empty:
            print("No rows to insert.")
            return 0

        # Make sure numeric types are numeric
        for col in [
            "age",
            "monthly_spend",
            "watch_hours",
            "completion_rate",
            "is_active",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        schema = "assignment"
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"

        # SQL types
        columns_sql = []
        for col in df.columns:
            if col in ["age", "monthly_spend", "watch_hours", "completion_rate"]:
                columns_sql.append(f'"{col}" DOUBLE PRECISION')
            elif col in ["is_active"]:
                columns_sql.append(f'"{col}" INTEGER')
            else:
                columns_sql.append(f'"{col}" TEXT')

        create_table = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join(columns_sql)}
            );
        """
        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None

        insert_sql = f"""
            INSERT INTO {schema}.{table} ({', '.join([f'"{c}"' for c in df.columns])})
            VALUES ({', '.join(['%s' for _ in df.columns])});
        """
        rows = [tuple(r) for r in df.to_numpy()]

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                if delete_rows:
                    cur.execute(delete_rows)
                cur.executemany(insert_sql, rows)
                conn.commit()
            print(f"Inserted {len(rows)} rows into {schema}.{table}")
            return len(rows)
        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    # 5) SIMPLE ML MODEL

    @task()
    def train_simple_model(conn_id: str, table: str = "netflix_merged") -> str:
        import pandas as pd
        from sklearn.model_selection import train_test_split
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score
        import joblib
        import numpy as np
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=conn_id)
        df = hook.get_pandas_df(
            f'SELECT "is_active", "watch_hours" FROM assignment."{table}";'
        )

        # Clean types
        df["is_active"] = pd.to_numeric(df["is_active"], errors="coerce")
        df["watch_hours"] = pd.to_numeric(df["watch_hours"], errors="coerce")

        # Drop NaNs
        df = df.replace([np.inf, -np.inf], np.nan).dropna(
            subset=["is_active", "watch_hours"]
        )

        # Ensure binary target (1 = active, 0 = churned)
        y = (df["is_active"] > 0).astype(int)
        X = df[["watch_hours"]]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        model = LogisticRegression()
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        acc = accuracy_score(y_test, preds)

        model_path = "/opt/airflow/data/simple_model.pkl"
        joblib.dump(model, model_path)

        print(f"Model trained with accuracy: {acc:.2f}")
        return f"Model accuracy: {acc:.2f}"

    # 6) VISUALIZATION

    @task()
    def perform_visualization(conn_id: str, table: str = TARGET_TABLE) -> str:
        import pandas as pd
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=conn_id)

        q = f"""
            SELECT "subscription_plan","watch_hours"
            FROM assignment."{table}";
        """

        df = hook.get_pandas_df(q)
        if df.empty:
            print("No data to plot.")
            return ""

        df["watch_hours"] = pd.to_numeric(df["watch_hours"], errors="coerce")
        df = df.dropna(subset=["watch_hours"])

        # Group + sort plans by mean watch hours
        summary = (
            df.groupby("subscription_plan", as_index=False)["watch_hours"]
            .agg(["mean", "count"])
            .reset_index()
            .sort_values("mean")
        )

        plt.figure(figsize=(10, 6))
        bars = plt.bar(summary["subscription_plan"], summary["mean"], color="steelblue")

        # Add count labels above each bar
        for bar, n in zip(bars, summary["count"]):
            plt.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.1,
                f"n={n}",
                ha="center",
                fontsize=10,
            )

        plt.title("Average Watch Hours by Subscription Plan", fontsize=16)
        plt.xlabel("Subscription Plan", fontsize=12)
        plt.ylabel("Avg Watch Hours", fontsize=12)
        plt.xticks(rotation=20)
        plt.tight_layout()

        img_path = "/opt/airflow/data/analysis_plot.png"
        plt.savefig(img_path)
        plt.close()

        print(f"Visualization saved to {img_path}")
        return img_path

    # 7) CLEANUP

    @task()
    def clear_folder(folder_path: str = "/opt/airflow/data") -> None:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                # Keep visualization & model files
                if "analysis_plot.png" in file_path or "simple_model.pkl" in file_path:
                    continue
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    if "raw" not in file_path:
                        shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
        print("Clean process completed!")

    # Task Orchestration

    users_file = fetch_users()
    history_file = fetch_watch_history()
    merged_file = merge_csvs(users_file, history_file)

    load_to_database = load_csv_to_pg(
        conn_id="Postgres", csv_path=merged_file, table=TARGET_TABLE
    )
    train_model = train_simple_model(conn_id="Postgres", table=TARGET_TABLE)
    visualization = perform_visualization(conn_id="Postgres", table=TARGET_TABLE)
    clean_folder = clear_folder(folder_path=OUTPUT_DIR)

    load_to_database >> [train_model, visualization] >> clean_folder
