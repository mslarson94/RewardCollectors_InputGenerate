# path: tools/mark_review_tk.py
# Replace your current Tkinter file with this version.

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk


DEFAULT_TIME_COL = "mLT_orig"
DEFAULT_EVENT_COL = "lo_eventType"
DEFAULT_BLOCK_COL = "BlockNum"


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    return json.loads(text)


def seconds_between(a: pd.Timestamp, b: pd.Timestamp) -> float:
    return float((b - a).total_seconds())


def relink_cluster_df(cluster_df: pd.DataFrame) -> pd.DataFrame:
    if cluster_df.empty:
        return cluster_df

    cluster_df = cluster_df.sort_values(["start_time", "end_time"]).reset_index(drop=True).copy()
    cluster_df["prev_cluster_id"] = ""
    cluster_df["next_cluster_id"] = ""
    cluster_df["gap_before_seconds"] = pd.NA
    cluster_df["gap_after_seconds"] = pd.NA

    for i in range(len(cluster_df)):
        if i > 0:
            cluster_df.loc[i, "prev_cluster_id"] = cluster_df.loc[i - 1, "cluster_id"]
            cluster_df.loc[i, "gap_before_seconds"] = seconds_between(
                pd.to_datetime(cluster_df.loc[i - 1, "end_time"]),
                pd.to_datetime(cluster_df.loc[i, "start_time"]),
            )
        if i < len(cluster_df) - 1:
            cluster_df.loc[i, "next_cluster_id"] = cluster_df.loc[i + 1, "cluster_id"]
            cluster_df.loc[i, "gap_after_seconds"] = seconds_between(
                pd.to_datetime(cluster_df.loc[i, "end_time"]),
                pd.to_datetime(cluster_df.loc[i + 1, "start_time"]),
            )

    return cluster_df


def infer_auto_label(n_marks: int) -> tuple[str, float]:
    if n_marks <= 2:
        return "pause_like", 0.45
    if n_marks == 3:
        return "start_like", 0.50
    if n_marks >= 4:
        return "end_like", 0.50
    return "unknown", 0.0


def recompute_cluster_row_fields(row: pd.Series, mark_times: list[pd.Timestamp]) -> pd.Series:
    row = row.copy()
    row["n_marks"] = len(mark_times)
    row["start_time"] = min(mark_times).isoformat()
    row["end_time"] = max(mark_times).isoformat()
    row["duration_seconds"] = seconds_between(min(mark_times), max(mark_times))
    auto_label, auto_conf = infer_auto_label(len(mark_times))
    row["auto_label"] = auto_label
    row["auto_confidence"] = auto_conf
    row["mark_times"] = json.dumps([t.isoformat() for t in mark_times])
    return row


class MarkReviewApp:
    def __init__(
        self,
        root: tk.Tk,
        cluster_csv: str = "",
        context_csv: str = "",
        source_csv: str = "",
        review_csv: str = "review_edits.csv",
        time_col: str = DEFAULT_TIME_COL,
        event_col: str = DEFAULT_EVENT_COL,
        block_col: str = DEFAULT_BLOCK_COL,
        context_seconds: int = 300,
    ) -> None:
        self.root = root
        self.root.title("Mark Cluster Review")
        self.root.geometry("1800x1000")

        self.time_col = time_col
        self.event_col = event_col
        self.block_col = block_col
        self.context_seconds = context_seconds

        self.cluster_csv_var = tk.StringVar(value=cluster_csv)
        self.context_csv_var = tk.StringVar(value=context_csv)
        self.source_csv_var = tk.StringVar(value=source_csv)
        self.review_csv_var = tk.StringVar(value=review_csv)
        self.time_col_var = tk.StringVar(value=time_col)
        self.event_col_var = tk.StringVar(value=event_col)
        self.block_col_var = tk.StringVar(value=block_col)
        self.context_seconds_var = tk.StringVar(value=str(context_seconds))

        self.cluster_df = pd.DataFrame()
        self.context_df = pd.DataFrame()
        self.source_df = pd.DataFrame()
        self.review_df = pd.DataFrame()

        self.selected_cluster_id: str | None = None
        self.next_generated_cluster_num = 1

        self.manual_label_var = tk.StringVar(value="unknown")
        self.review_status_var = tk.StringVar(value="proposed")
        self.action_var = tk.StringVar(value="")
        self.split_after_var = tk.StringVar(value="")
        self.merge_with_next_var = tk.BooleanVar(value=False)

        self._build_ui()

        if cluster_csv:
            self.load_all_data()

    def _build_ui(self) -> None:
        self.root.rowconfigure(1, weight=1)
        self.root.columnconfigure(0, weight=1)

        top = ttk.Frame(self.root, padding=8)
        top.grid(row=0, column=0, sticky="nsew")
        for i in range(9):
            top.columnconfigure(i, weight=1)

        ttk.Label(top, text="Cluster CSV").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.cluster_csv_var).grid(row=0, column=1, columnspan=3, sticky="ew", padx=4)
        ttk.Button(top, text="Browse", command=lambda: self._browse_file(self.cluster_csv_var)).grid(row=0, column=4, sticky="ew")

        ttk.Label(top, text="Context CSV").grid(row=1, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.context_csv_var).grid(row=1, column=1, columnspan=3, sticky="ew", padx=4)
        ttk.Button(top, text="Browse", command=lambda: self._browse_file(self.context_csv_var)).grid(row=1, column=4, sticky="ew")

        ttk.Label(top, text="Source CSV").grid(row=2, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.source_csv_var).grid(row=2, column=1, columnspan=3, sticky="ew", padx=4)
        ttk.Button(top, text="Browse", command=lambda: self._browse_file(self.source_csv_var)).grid(row=2, column=4, sticky="ew")

        ttk.Label(top, text="Review CSV").grid(row=3, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.review_csv_var).grid(row=3, column=1, columnspan=3, sticky="ew", padx=4)
        ttk.Button(top, text="Browse", command=lambda: self._browse_save(self.review_csv_var)).grid(row=3, column=4, sticky="ew")

        ttk.Label(top, text="Time col").grid(row=0, column=5, sticky="w")
        ttk.Entry(top, textvariable=self.time_col_var, width=18).grid(row=0, column=6, sticky="ew", padx=4)

        ttk.Label(top, text="Event col").grid(row=1, column=5, sticky="w")
        ttk.Entry(top, textvariable=self.event_col_var, width=18).grid(row=1, column=6, sticky="ew", padx=4)

        ttk.Label(top, text="Block col").grid(row=2, column=5, sticky="w")
        ttk.Entry(top, textvariable=self.block_col_var, width=18).grid(row=2, column=6, sticky="ew", padx=4)

        ttk.Label(top, text="Window sec").grid(row=3, column=5, sticky="w")
        ttk.Entry(top, textvariable=self.context_seconds_var, width=18).grid(row=3, column=6, sticky="ew", padx=4)

        ttk.Button(top, text="Load / Refresh", command=self.load_all_data).grid(row=0, column=7, sticky="ew", padx=4)
        ttk.Button(top, text="Save Selected Edit", command=self.save_selected_edit).grid(row=1, column=7, sticky="ew", padx=4)
        ttk.Button(top, text="Apply Edits In App", command=self.apply_edits_in_app).grid(row=2, column=7, sticky="ew", padx=4)
        ttk.Button(top, text="Save Review CSV", command=self.save_review_csv).grid(row=3, column=7, sticky="ew", padx=4)

        main = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        main.grid(row=1, column=0, sticky="nsew")

        left = ttk.Frame(main, padding=8)
        center = ttk.Frame(main, padding=8)
        right = ttk.Frame(main, padding=8)

        main.add(left, weight=5)
        main.add(center, weight=4)
        main.add(right, weight=5)

        left.rowconfigure(1, weight=1)
        left.columnconfigure(0, weight=1)

        ttk.Label(left, text="Clusters").grid(row=0, column=0, sticky="w")

        cluster_cols = (
            "cluster_id",
            "start_time",
            "end_time",
            "duration_seconds",
            "n_marks",
            "gap_before_seconds",
            "gap_after_seconds",
            "auto_label",
            "manual_label",
            "review_status",
        )
        self.cluster_tree = ttk.Treeview(left, columns=cluster_cols, show="headings", height=24)
        for col in cluster_cols:
            self.cluster_tree.heading(col, text=col)
            self.cluster_tree.column(col, width=130, anchor="w")
        self.cluster_tree.grid(row=1, column=0, sticky="nsew")

        cluster_scroll_y = ttk.Scrollbar(left, orient="vertical", command=self.cluster_tree.yview)
        cluster_scroll_y.grid(row=1, column=1, sticky="ns")
        self.cluster_tree.configure(yscrollcommand=cluster_scroll_y.set)
        self.cluster_tree.bind("<<TreeviewSelect>>", self.on_cluster_select)

        center.rowconfigure(5, weight=1)
        center.columnconfigure(0, weight=1)

        ttk.Label(center, text="Selected Cluster").grid(row=0, column=0, sticky="w")
        self.cluster_detail_text = tk.Text(center, wrap="word", height=10)
        self.cluster_detail_text.grid(row=1, column=0, sticky="nsew")

        ttk.Label(center, text="Cluster Marks").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.cluster_marks_text = tk.Text(center, wrap="none", height=10)
        self.cluster_marks_text.grid(row=3, column=0, sticky="nsew")

        ttk.Label(center, text="Stored Context").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.context_text = tk.Text(center, wrap="word", height=12)
        self.context_text.grid(row=5, column=0, sticky="nsew")

        right.rowconfigure(5, weight=1)
        right.columnconfigure(0, weight=1)

        ttk.Label(right, text="Edit Selected Cluster").grid(row=0, column=0, sticky="w")

        form = ttk.Frame(right)
        form.grid(row=1, column=0, sticky="ew", pady=4)
        form.columnconfigure(1, weight=1)

        ttk.Label(form, text="Manual label").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            form,
            textvariable=self.manual_label_var,
            values=["unknown", "start", "end", "pause", "ignore"],
            state="readonly",
        ).grid(row=0, column=1, sticky="ew", padx=4, pady=2)

        ttk.Label(form, text="Review status").grid(row=1, column=0, sticky="w")
        ttk.Combobox(
            form,
            textvariable=self.review_status_var,
            values=["proposed", "confirmed", "edited"],
            state="readonly",
        ).grid(row=1, column=1, sticky="ew", padx=4, pady=2)

        ttk.Label(form, text="Action").grid(row=2, column=0, sticky="w")
        ttk.Combobox(
            form,
            textvariable=self.action_var,
            values=["", "split", "merge_next", "ignore"],
            state="readonly",
        ).grid(row=2, column=1, sticky="ew", padx=4, pady=2)

        ttk.Label(form, text="Split after mark index").grid(row=3, column=0, sticky="w")
        ttk.Entry(form, textvariable=self.split_after_var).grid(row=3, column=1, sticky="ew", padx=4, pady=2)

        ttk.Checkbutton(form, text="Merge with next", variable=self.merge_with_next_var).grid(row=4, column=1, sticky="w", padx=4, pady=2)

        ttk.Label(right, text="Notes").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.notes_text = tk.Text(right, wrap="word", height=8)
        self.notes_text.grid(row=3, column=0, sticky="nsew")

        ttk.Label(right, text="Local Event Window").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.local_text = tk.Text(right, wrap="none", height=18)
        self.local_text.grid(row=5, column=0, sticky="nsew")

    def _browse_file(self, var: tk.StringVar) -> None:
        path = filedialog.askopenfilename()
        if path:
            var.set(path)

    def _browse_save(self, var: tk.StringVar) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if path:
            var.set(path)

    def _set_next_generated_cluster_num(self) -> None:
        max_num = 0
        if "cluster_id" not in self.cluster_df.columns or self.cluster_df.empty:
            self.next_generated_cluster_num = 1
            return

        for cid in self.cluster_df["cluster_id"].astype(str).tolist():
            if cid.startswith("gui_c"):
                suffix = cid.replace("gui_c", "")
                if suffix.isdigit():
                    max_num = max(max_num, int(suffix))
        self.next_generated_cluster_num = max_num + 1

    def _new_cluster_id(self) -> str:
        cid = f"gui_c{self.next_generated_cluster_num}"
        self.next_generated_cluster_num += 1
        return cid

    def load_all_data(self) -> None:
        try:
            self.time_col = self.time_col_var.get().strip()
            self.event_col = self.event_col_var.get().strip()
            self.block_col = self.block_col_var.get().strip()
            self.context_seconds = int(self.context_seconds_var.get().strip())

            cluster_path = self.cluster_csv_var.get().strip()
            context_path = self.context_csv_var.get().strip()
            source_path = self.source_csv_var.get().strip()

            if not cluster_path:
                raise ValueError("Cluster CSV path is required.")

            self.cluster_df = pd.read_csv(cluster_path)

            review_only_cols = [
                "manual_label",
                "review_status",
                "action",
                "split_after_mark_index",
                "merge_with_next",
                "notes",
            ]
            self.cluster_df = self.cluster_df.drop(
                columns=[col for col in review_only_cols if col in self.cluster_df.columns],
                errors="ignore",
            )

            self.cluster_df["start_time"] = pd.to_datetime(self.cluster_df["start_time"], errors="coerce")
            self.cluster_df["end_time"] = pd.to_datetime(self.cluster_df["end_time"], errors="coerce")
            self.cluster_df = relink_cluster_df(self.cluster_df)

            self.context_df = pd.read_csv(context_path) if context_path and Path(context_path).exists() else pd.DataFrame()

            if source_path and Path(source_path).exists():
                self.source_df = pd.read_csv(source_path)
                if self.time_col in self.source_df.columns:
                    self.source_df[self.time_col] = pd.to_datetime(self.source_df[self.time_col], errors="coerce")
                    self.source_df = self.source_df.dropna(subset=[self.time_col]).sort_values(self.time_col).reset_index(drop=True)
            else:
                self.source_df = pd.DataFrame()

            self._init_or_merge_review_df()
            self._set_next_generated_cluster_num()
            self.refresh_cluster_table()
            self._clear_detail_views()
            messagebox.showinfo("Loaded", "Files loaded successfully.")
        except Exception as exc:
            messagebox.showerror("Load error", str(exc))

    def _init_or_merge_review_df(self) -> None:
        base = self.cluster_df[["cluster_id"]].copy()
        base["manual_label"] = "unknown"
        base["review_status"] = "proposed"
        base["action"] = ""
        base["split_after_mark_index"] = ""
        base["merge_with_next"] = ""
        base["notes"] = ""

        review_path = self.review_csv_var.get().strip()
        if review_path and Path(review_path).exists():
            saved = pd.read_csv(review_path, dtype=str).fillna("")
            merged = base.merge(saved, on="cluster_id", how="left", suffixes=("", "_saved"))
            for col in ["manual_label", "review_status", "action", "split_after_mark_index", "merge_with_next", "notes"]:
                saved_col = f"{col}_saved"
                if saved_col in merged.columns:
                    merged[col] = merged[saved_col].where(merged[saved_col] != "", merged[col])
                    merged = merged.drop(columns=[saved_col])
            self.review_df = merged[["cluster_id", "manual_label", "review_status", "action", "split_after_mark_index", "merge_with_next", "notes"]].copy()
        else:
            self.review_df = base

    def refresh_cluster_table(self) -> None:
        for item in self.cluster_tree.get_children():
            self.cluster_tree.delete(item)

        merged = self.cluster_df.merge(self.review_df, on="cluster_id", how="left", validate="one_to_one")
        merged = merged.sort_values(["start_time", "end_time"]).reset_index(drop=True)

        for _, row in merged.iterrows():
            values = (
                row.get("cluster_id", ""),
                row.get("start_time", ""),
                row.get("end_time", ""),
                row.get("duration_seconds", ""),
                row.get("n_marks", ""),
                row.get("gap_before_seconds", ""),
                row.get("gap_after_seconds", ""),
                row.get("auto_label", ""),
                row.get("manual_label", ""),
                row.get("review_status", ""),
            )
            self.cluster_tree.insert("", "end", iid=str(row["cluster_id"]), values=values)

    def _clear_detail_views(self) -> None:
        for widget in [self.cluster_detail_text, self.cluster_marks_text, self.context_text, self.local_text]:
            widget.delete("1.0", tk.END)
        self.notes_text.delete("1.0", tk.END)

    def on_cluster_select(self, event: Any = None) -> None:
        selection = self.cluster_tree.selection()
        if not selection:
            return

        self.selected_cluster_id = str(selection[0])

        cluster_row_df = self.cluster_df[self.cluster_df["cluster_id"].astype(str) == self.selected_cluster_id]
        if cluster_row_df.empty:
            return
        cluster_row = cluster_row_df.iloc[0]

        review_row_df = self.review_df[self.review_df["cluster_id"].astype(str) == self.selected_cluster_id]
        if review_row_df.empty:
            return
        review_row = review_row_df.iloc[0]

        self.manual_label_var.set(review_row["manual_label"] or "unknown")
        self.review_status_var.set(review_row["review_status"] or "proposed")
        self.action_var.set(review_row["action"] or "")
        self.split_after_var.set(review_row["split_after_mark_index"] or "")
        self.merge_with_next_var.set(str(review_row["merge_with_next"]).strip().lower() in {"1", "true", "yes", "y"})
        self.notes_text.delete("1.0", tk.END)
        self.notes_text.insert("1.0", review_row["notes"] or "")

        self._render_cluster_details(cluster_row, review_row)
        self._render_cluster_marks(cluster_row)
        self._render_context(cluster_row)
        self._render_local_window(cluster_row)

    def _render_cluster_details(self, cluster_row: pd.Series, review_row: pd.Series) -> None:
        payload = {
            "cluster_id": cluster_row.get("cluster_id", ""),
            "start_time": str(cluster_row.get("start_time", "")),
            "end_time": str(cluster_row.get("end_time", "")),
            "duration_seconds": cluster_row.get("duration_seconds", ""),
            "n_marks": cluster_row.get("n_marks", ""),
            "gap_before_seconds": cluster_row.get("gap_before_seconds", ""),
            "gap_after_seconds": cluster_row.get("gap_after_seconds", ""),
            "auto_label": cluster_row.get("auto_label", ""),
            "auto_confidence": cluster_row.get("auto_confidence", ""),
            "manual_label": review_row.get("manual_label", ""),
            "review_status": review_row.get("review_status", ""),
            "action": review_row.get("action", ""),
            "split_after_mark_index": review_row.get("split_after_mark_index", ""),
            "merge_with_next": review_row.get("merge_with_next", ""),
        }
        self.cluster_detail_text.delete("1.0", tk.END)
        self.cluster_detail_text.insert("1.0", json.dumps(payload, indent=2, default=str))

    def _render_cluster_marks(self, cluster_row: pd.Series) -> None:
        self.cluster_marks_text.delete("1.0", tk.END)
        mark_times = [pd.to_datetime(v) for v in parse_json_list(cluster_row.get("mark_times", "[]"))]
        source_rows = parse_json_list(cluster_row.get("source_row_indices", "[]"))

        if not mark_times:
            self.cluster_marks_text.insert("1.0", "No mark-level info available.")
            return

        lines = []
        for i, mark_time in enumerate(mark_times, start=1):
            row_val = source_rows[i - 1] if i - 1 < len(source_rows) else ""
            lines.append(f"[{i}] time={mark_time} | source_row={row_val}")
        lines.append("")
        lines.append("Use split_after_mark_index with the bracketed value above.")
        lines.append("Example: entering 2 splits after [2].")
        self.cluster_marks_text.insert("1.0", "\n".join(lines))

    def _render_context(self, cluster_row: pd.Series) -> None:
        self.context_text.delete("1.0", tk.END)
        if self.context_df.empty:
            return

        context_row_df = self.context_df[self.context_df["cluster_id"].astype(str) == str(cluster_row["cluster_id"])]
        if context_row_df.empty:
            self.context_text.insert("1.0", "No stored context row for this cluster.")
            return

        context_row = context_row_df.iloc[0].to_dict()
        self.context_text.insert("1.0", json.dumps(context_row, indent=2, default=str))

    def _render_local_window(self, cluster_row: pd.Series) -> None:
        self.local_text.delete("1.0", tk.END)
        if self.source_df.empty:
            return

        if self.time_col not in self.source_df.columns or self.event_col not in self.source_df.columns:
            return

        cluster_start = pd.to_datetime(cluster_row["start_time"])
        cluster_end = pd.to_datetime(cluster_row["end_time"])
        midpoint = cluster_start + (cluster_end - cluster_start) / 2
        window_start = midpoint - pd.Timedelta(seconds=self.context_seconds)
        window_end = midpoint + pd.Timedelta(seconds=self.context_seconds)

        local = self.source_df[(self.source_df[self.time_col] >= window_start) & (self.source_df[self.time_col] <= window_end)].copy()
        source_row_indices = set(parse_json_list(cluster_row.get("source_row_indices", "[]")))

        cluster_marks = {}
        mark_times = [pd.to_datetime(v) for v in parse_json_list(cluster_row.get("mark_times", "[]"))]
        cluster_source_rows = parse_json_list(cluster_row.get("source_row_indices", "[]"))
        for i, src in enumerate(cluster_source_rows, start=1):
            cluster_marks[src] = i

        lines: list[str] = []
        for _, row in local.iterrows():
            source_idx = row["_source_row_index"] if "_source_row_index" in row.index else ""
            mark_label = f"[{cluster_marks[source_idx]}]" if source_idx in cluster_marks else "   "
            marker = "*" if source_idx in source_row_indices else " "
            block_val = row[self.block_col] if self.block_col in row.index else ""
            lines.append(f"{marker} {mark_label} {row[self.time_col]} | {row[self.event_col]} | BlockNum={block_val} | row={source_idx}")

        self.local_text.insert("1.0", "\n".join(lines))

    def save_selected_edit(self) -> None:
        if not self.selected_cluster_id:
            messagebox.showwarning("No selection", "Select a cluster first.")
            return

        mask = self.review_df["cluster_id"].astype(str) == self.selected_cluster_id
        if not mask.any():
            messagebox.showerror("Missing row", "Could not find selected cluster in review sheet.")
            return

        self.review_df.loc[mask, "manual_label"] = self.manual_label_var.get().strip() or "unknown"
        self.review_df.loc[mask, "review_status"] = self.review_status_var.get().strip() or "proposed"
        self.review_df.loc[mask, "action"] = self.action_var.get().strip()
        self.review_df.loc[mask, "split_after_mark_index"] = self.split_after_var.get().strip()
        self.review_df.loc[mask, "merge_with_next"] = "true" if self.merge_with_next_var.get() else ""
        self.review_df.loc[mask, "notes"] = self.notes_text.get("1.0", tk.END).strip()

        self.refresh_cluster_table()
        messagebox.showinfo("Saved", f"Saved edits for {self.selected_cluster_id}. Click 'Apply Edits In App' to re-segment the table.")

    def apply_edits_in_app(self) -> None:
        try:
            before_n = len(self.cluster_df)

            self._apply_splits()
            self._apply_merges()
            self.cluster_df = relink_cluster_df(self.cluster_df)

            merged = self.cluster_df[["cluster_id"]].copy()
            merged["manual_label"] = "unknown"
            merged["review_status"] = "proposed"
            merged["action"] = ""
            merged["split_after_mark_index"] = ""
            merged["merge_with_next"] = ""
            merged["notes"] = ""
            merged = merged.merge(self.review_df, on="cluster_id", how="left", suffixes=("", "_old"))
            for col in ["manual_label", "review_status", "action", "split_after_mark_index", "merge_with_next", "notes"]:
                old = f"{col}_old"
                if old in merged.columns:
                    merged[col] = merged[old].where(merged[old].notna() & (merged[old] != ""), merged[col])
                    merged = merged.drop(columns=[old])
            self.review_df = merged[["cluster_id", "manual_label", "review_status", "action", "split_after_mark_index", "merge_with_next", "notes"]].copy()

            self.refresh_cluster_table()

            after_n = len(self.cluster_df)
            self.selected_cluster_id = None
            self._clear_detail_views()

            messagebox.showinfo("Applied", f"Edits applied in app.\nClusters before: {before_n}\nClusters after: {after_n}")
        except Exception as exc:
            messagebox.showerror("Apply error", str(exc))

    def _apply_splits(self) -> None:
        merged = self.cluster_df.merge(self.review_df, on="cluster_id", how="left")
        merged = merged.sort_values(["start_time", "end_time"]).reset_index(drop=True)

        new_rows: list[dict[str, Any]] = []

        for _, row in merged.iterrows():
            action = str(row.get("action", "")).strip()
            split_text = str(row.get("split_after_mark_index", "")).strip()

            if action != "split" or not split_text:
                new_rows.append(row[self.cluster_df.columns].to_dict())
                continue

            mark_times = [pd.to_datetime(v) for v in parse_json_list(row.get("mark_times", "[]"))]
            source_rows = parse_json_list(row.get("source_row_indices", "[]"))

            if len(mark_times) < 2:
                new_rows.append(row[self.cluster_df.columns].to_dict())
                continue

            split_idx = int(split_text)
            if split_idx < 1 or split_idx >= len(mark_times):
                raise ValueError(
                    f"Invalid split_after_mark_index={split_idx} for cluster {row['cluster_id']} with n_marks={len(mark_times)}"
                )

            left_times = mark_times[:split_idx]
            right_times = mark_times[split_idx:]
            left_sources = source_rows[:split_idx]
            right_sources = source_rows[split_idx:]

            left_row = row[self.cluster_df.columns].copy()
            left_row["cluster_id"] = self._new_cluster_id()
            left_row["source_row_indices"] = json.dumps(left_sources)
            left_row = recompute_cluster_row_fields(left_row, left_times)

            right_row = row[self.cluster_df.columns].copy()
            right_row["cluster_id"] = self._new_cluster_id()
            right_row["source_row_indices"] = json.dumps(right_sources)
            right_row = recompute_cluster_row_fields(right_row, right_times)

            new_rows.append(left_row.to_dict())
            new_rows.append(right_row.to_dict())

            self.review_df = self.review_df[self.review_df["cluster_id"].astype(str) != str(row["cluster_id"])]

            self.review_df = pd.concat(
                [
                    self.review_df,
                    pd.DataFrame(
                        [
                            {
                                "cluster_id": left_row["cluster_id"],
                                "manual_label": row.get("manual_label", "unknown"),
                                "review_status": "edited",
                                "action": "",
                                "split_after_mark_index": "",
                                "merge_with_next": "",
                                "notes": row.get("notes", ""),
                            },
                            {
                                "cluster_id": right_row["cluster_id"],
                                "manual_label": row.get("manual_label", "unknown"),
                                "review_status": "edited",
                                "action": "",
                                "split_after_mark_index": "",
                                "merge_with_next": "",
                                "notes": row.get("notes", ""),
                            },
                        ]
                    ),
                ],
                ignore_index=True,
            )

        self.cluster_df = pd.DataFrame(new_rows)
        self.cluster_df["start_time"] = pd.to_datetime(self.cluster_df["start_time"], errors="coerce")
        self.cluster_df["end_time"] = pd.to_datetime(self.cluster_df["end_time"], errors="coerce")
        print("---- SPLIT DEBUG ----")
        print("cluster_id:", row["cluster_id"])
        print("action:", action)
        print("split_text:", split_text)
        print("mark_times_raw:", row.get("mark_times", "[]"))
        print("source_row_indices:", row.get("source_row_indices", "[]"))
        print("---------------------")

    def _apply_merges(self) -> None:
        merged = self.cluster_df.merge(self.review_df, on="cluster_id", how="left")
        merged = merged.sort_values(["start_time", "end_time"]).reset_index(drop=True)

        new_rows: list[dict[str, Any]] = []
        used_ids: set[str] = set()
        i = 0

        while i < len(merged):
            row = merged.iloc[i]
            cid = str(row["cluster_id"])
            if cid in used_ids:
                i += 1
                continue

            merge_flag = str(row.get("merge_with_next", "")).strip().lower() in {"1", "true", "yes", "y"}
            action = str(row.get("action", "")).strip()

            if (merge_flag or action == "merge_next") and i < len(merged) - 1:
                nxt = merged.iloc[i + 1]
                next_cid = str(nxt["cluster_id"])

                left_times = [pd.to_datetime(v) for v in parse_json_list(row.get("mark_times", "[]"))]
                right_times = [pd.to_datetime(v) for v in parse_json_list(nxt.get("mark_times", "[]"))]
                left_sources = parse_json_list(row.get("source_row_indices", "[]"))
                right_sources = parse_json_list(nxt.get("source_row_indices", "[]"))

                merged_row = row[self.cluster_df.columns].copy()
                merged_row["cluster_id"] = self._new_cluster_id()
                merged_row["source_row_indices"] = json.dumps(left_sources + right_sources)
                merged_row = recompute_cluster_row_fields(merged_row, left_times + right_times)

                new_rows.append(merged_row.to_dict())
                used_ids.add(cid)
                used_ids.add(next_cid)

                self.review_df = self.review_df[~self.review_df["cluster_id"].astype(str).isin([cid, next_cid])]
                self.review_df = pd.concat(
                    [
                        self.review_df,
                        pd.DataFrame(
                            [
                                {
                                    "cluster_id": merged_row["cluster_id"],
                                    "manual_label": row.get("manual_label", "unknown"),
                                    "review_status": "edited",
                                    "action": "",
                                    "split_after_mark_index": "",
                                    "merge_with_next": "",
                                    "notes": " ".join(
                                        [str(row.get("notes", "")).strip(), str(nxt.get("notes", "")).strip()]
                                    ).strip(),
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

                i += 2
                continue

            new_rows.append(row[self.cluster_df.columns].to_dict())
            used_ids.add(cid)
            i += 1

        self.cluster_df = pd.DataFrame(new_rows)
        self.cluster_df["start_time"] = pd.to_datetime(self.cluster_df["start_time"], errors="coerce")
        self.cluster_df["end_time"] = pd.to_datetime(self.cluster_df["end_time"], errors="coerce")

    def save_review_csv(self) -> None:
        try:
            review_path = self.review_csv_var.get().strip()
            if not review_path:
                raise ValueError("Review CSV path is required.")
            Path(review_path).parent.mkdir(parents=True, exist_ok=True)
            self.review_df.to_csv(review_path, index=False)
            messagebox.showinfo("Saved", f"Review CSV saved to:\n{review_path}")
        except Exception as exc:
            messagebox.showerror("Save error", str(exc))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local Tkinter GUI for Mark cluster review.")
    parser.add_argument("--cluster-csv", default="", help="Path to mark_clusters.csv")
    parser.add_argument("--context-csv", default="", help="Path to mark_cluster_context.csv")
    parser.add_argument("--source-csv", default="", help="Path to source CSV")
    parser.add_argument("--review-csv", default="review_edits.csv", help="Path to save/load review CSV")
    parser.add_argument("--time-col", default=DEFAULT_TIME_COL, help="Time column")
    parser.add_argument("--event-col", default=DEFAULT_EVENT_COL, help="Event column")
    parser.add_argument("--block-col", default=DEFAULT_BLOCK_COL, help="Block column")
    parser.add_argument("--context-seconds", type=int, default=300, help="Display window around selected cluster")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    root = tk.Tk()
    app = MarkReviewApp(
        root=root,
        cluster_csv=args.cluster_csv,
        context_csv=args.context_csv,
        source_csv=args.source_csv,
        review_csv=args.review_csv,
        time_col=args.time_col,
        event_col=args.event_col,
        block_col=args.block_col,
        context_seconds=args.context_seconds,
    )
    root.mainloop()


if __name__ == "__main__":
    main()