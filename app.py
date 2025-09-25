import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import os
import threading
import queue

class DataFilterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("交互式数据筛选工具 (支持后台加载)")
        self.root.geometry("900x700")

        self.original_df = None # To store the original data for resetting
        self.df = None
        self.file_path = ""
        self.data_queue = queue.Queue()

        # --- Main Layout ---
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Top Frame for Controls ---
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill=tk.X, pady=5)

        self.load_button = ttk.Button(top_frame, text="加载数据文件", command=self.start_loading_thread)
        self.load_button.pack(side=tk.LEFT, padx=5)

        self.file_label = ttk.Label(top_frame, text="未加载文件", width=50, wraplength=400)
        self.file_label.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        # --- Middle Frame for Column Selection ---
        middle_frame = ttk.LabelFrame(main_frame, text="选择要筛选的列 (可多选)")
        middle_frame.pack(fill=tk.BOTH, expand=True, pady=10)

        # --- Listbox with Scrollbars ---
        list_frame = ttk.Frame(middle_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.column_listbox = tk.Listbox(list_frame, selectmode=tk.EXTENDED, exportselection=False)
        
        v_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.column_listbox.yview)
        h_scrollbar = ttk.Scrollbar(list_frame, orient=tk.HORIZONTAL, command=self.column_listbox.xview)
        
        self.column_listbox.config(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)

        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        self.column_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # --- Advanced Filter Frame ---
        adv_filter_frame = ttk.LabelFrame(main_frame, text="高级筛选")
        adv_filter_frame.pack(fill=tk.X, pady=10)
        
        # Column selection for advanced filter
        ttk.Label(adv_filter_frame, text="列:").grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.adv_filter_col = ttk.Combobox(adv_filter_frame, state='readonly')
        self.adv_filter_col.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
        self.adv_filter_col.bind("<<ComboboxSelected>>", self.on_adv_col_selected)

        # Operator
        ttk.Label(adv_filter_frame, text="条件:").grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.adv_filter_op = ttk.Combobox(adv_filter_frame, state='readonly', values=['>', '<', '>=', '<=', '==', '!='])
        self.adv_filter_op.grid(row=0, column=3, padx=5, pady=5, sticky='ew')
        self.adv_filter_op.set('==')

        # Value
        ttk.Label(adv_filter_frame, text="值:").grid(row=0, column=4, padx=5, pady=5, sticky='w')
        self.adv_filter_val = ttk.Entry(adv_filter_frame)
        self.adv_filter_val.grid(row=0, column=5, padx=5, pady=5, sticky='ew')

        # Apply button
        self.apply_adv_filter_button = ttk.Button(adv_filter_frame, text="应用高级筛选", command=self.apply_advanced_filter)
        self.apply_adv_filter_button.grid(row=0, column=6, padx=10, pady=5)
        self.apply_adv_filter_button.config(state=tk.DISABLED)

        adv_filter_frame.columnconfigure(1, weight=1)
        adv_filter_frame.columnconfigure(5, weight=1)

        # --- Bottom Frame for Actions ---
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill=tk.X, pady=5)

        self.filter_button = ttk.Button(action_frame, text="筛选选中列 (非空)", command=self.filter_non_empty)
        self.filter_button.pack(side=tk.LEFT, padx=5)
        self.filter_button.config(state=tk.DISABLED) # Disabled until data is loaded

        self.export_button = ttk.Button(action_frame, text="导出数据...", command=self.open_export_window)
        self.export_button.pack(side=tk.LEFT, padx=5)
        self.export_button.config(state=tk.DISABLED)

        self.reset_button = ttk.Button(action_frame, text="重置所有筛选", command=self.reset_data)
        self.reset_button.pack(side=tk.RIGHT, padx=10)
        self.reset_button.config(state=tk.DISABLED)
        
        # --- Status Bar ---
        self.status_bar = ttk.Label(main_frame, text="请先加载数据...", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def start_loading_thread(self):
        """Starts the file loading process in a background thread to keep the UI responsive."""
        file_path = filedialog.askopenfilename(
            title="请选择一个数据文件",
            filetypes=(("Stata DTA 文件", "*.dta"), ("CSV 文件", "*.csv"), ("所有文件", "*.*"))
        )
        if not file_path:
            return

        self.file_path = file_path
        self.load_button.config(state=tk.DISABLED)
        self.status_bar.config(text=f"正在后台加载: {os.path.basename(self.file_path)}...")
        self.file_label.config(text=f"加载中... {os.path.basename(self.file_path)}")

        # Create and start the background thread
        self.thread = threading.Thread(target=self.data_loader_worker, args=(self.file_path,))
        self.thread.daemon = True  # Allows main program to exit even if thread is running
        self.thread.start()

        # Periodically check the queue for the result
        self.root.after(100, self.check_data_queue)

    def data_loader_worker(self, path):
        """Worker function that runs in a separate thread to load data."""
        try:
            if path.lower().endswith('.dta'):
                df = pd.read_stata(path)
            elif path.lower().endswith('.csv'):
                df = pd.read_csv(path, low_memory=False)
            else:
                self.data_queue.put(("error", "不支持的文件类型。"))
                return
            
            self.data_queue.put(("success", df))
        except Exception as e:
            self.data_queue.put(("error", str(e)))

    def check_data_queue(self):
        """Checks the queue for data from the worker thread and updates the UI."""
        try:
            status, data = self.data_queue.get_nowait()

            if status == "success":
                self.original_df = data.copy() # Save a copy of the original data
                self.df = data
                self.file_label.config(text=os.path.basename(self.file_path))
                self.update_status_bar()
                self.populate_column_listbox()
                self.filter_button.config(state=tk.NORMAL)
                self.export_button.config(state=tk.NORMAL)
                self.apply_adv_filter_button.config(state=tk.NORMAL)
                self.reset_button.config(state=tk.NORMAL)
                self.adv_filter_col['values'] = self.df.columns.tolist()
                messagebox.showinfo("加载成功", f"成功加载 {len(self.df)} 条记录和 {len(self.df.columns)} 个变量。")
            elif status == "error":
                messagebox.showerror("加载失败", f"加载文件时发生错误:\n{data}")
                self.status_bar.config(text="加载失败，请重试。")
                self.file_label.config(text="未加载文件")
                self.filter_button.config(state=tk.DISABLED)
                self.export_button.config(state=tk.DISABLED)
                self.apply_adv_filter_button.config(state=tk.DISABLED)
                self.reset_button.config(state=tk.DISABLED)

            self.load_button.config(state=tk.NORMAL)

        except queue.Empty:
            # If the queue is empty, it means the worker is still busy.
            # We schedule this check to run again after a short delay.
            self.root.after(100, self.check_data_queue)

    def update_status_bar(self):
        """Updates the status bar with the current dataframe's shape."""
        if self.df is not None:
            self.status_bar.config(text=f"当前记录数: {len(self.df)}")
        else:
            self.status_bar.config(text="无数据")

    def populate_column_listbox(self):
        """Clears and fills the column listbox with columns from the dataframe."""
        self.column_listbox.delete(0, tk.END) # Clear existing items
        if self.df is not None:
            for col in self.df.columns:
                self.column_listbox.insert(tk.END, col)

    def filter_non_empty(self):
        """Filters the dataframe to keep only rows where selected columns are not empty."""
        if self.df is None:
            messagebox.showwarning("无数据", "请先加载数据文件。")
            return

        selected_indices = self.column_listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("未选择", "请在列表中至少选择一列进行筛选。")
            return

        selected_columns = [self.column_listbox.get(i) for i in selected_indices]
        
        initial_count = len(self.df)
        
        # Use dropna on the subset of selected columns
        self.df.dropna(subset=selected_columns, inplace=True)
        
        final_count = len(self.df)
        removed_count = initial_count - final_count
        
        self.update_status_bar()
        messagebox.showinfo("筛选完成", f"操作完成。\n\n移除了 {removed_count} 条记录。\n剩余记录数: {final_count}")

    def on_adv_col_selected(self, event=None):
        """Handles event when a column is selected for advanced filtering."""
        selected_col = self.adv_filter_col.get()
        if not selected_col or self.df is None:
            return

        # If the column is categorical (object/category) with few unique values, show unique values
        if pd.api.types.is_object_dtype(self.df[selected_col]) or pd.api.types.is_categorical_dtype(self.df[selected_col]):
            unique_vals = self.df[selected_col].unique()
            if 1 < len(unique_vals) < 20: # Heuristic for what's a "few" unique values
                self.adv_filter_op['values'] = ['==', '!=']
                self.adv_filter_op.set('==')
                # Replace the entry with a combobox of unique values
                self.adv_filter_val.destroy()
                self.adv_filter_val = ttk.Combobox(self.adv_filter_op.master, values=unique_vals.tolist())
                self.adv_filter_val.grid(row=0, column=5, padx=5, pady=5, sticky='ew')
            else: # Too many unique values, treat as text
                self.adv_filter_op['values'] = ['==', '!=', 'contains']
                self.adv_filter_op.set('==')
                self.adv_filter_val.destroy()
                self.adv_filter_val = ttk.Entry(self.adv_filter_op.master)
                self.adv_filter_val.grid(row=0, column=5, padx=5, pady=5, sticky='ew')
        # If numeric, show numeric operators
        elif pd.api.types.is_numeric_dtype(self.df[selected_col]):
            self.adv_filter_op['values'] = ['>', '<', '>=', '<=', '==', '!=']
            self.adv_filter_op.set('==')
            self.adv_filter_val.destroy()
            self.adv_filter_val = ttk.Entry(self.adv_filter_op.master)
            self.adv_filter_val.grid(row=0, column=5, padx=5, pady=5, sticky='ew')

    def apply_advanced_filter(self):
        """Applies a filter based on column, operator, and value."""
        if self.df is None:
            messagebox.showwarning("无数据", "请先加载数据文件。")
            return
        
        col = self.adv_filter_col.get()
        op = self.adv_filter_op.get()
        val = self.adv_filter_val.get()

        if not all([col, op, val]):
            messagebox.showwarning("信息不全", "请选择列、条件和值。")
            return

        initial_count = len(self.df)
        
        try:
            # Convert value to the same type as the column for proper comparison
            col_type = self.df[col].dtype
            if pd.api.types.is_numeric_dtype(col_type):
                val = pd.to_numeric(val)

            # Apply filter based on operator
            if op == '>':
                self.df = self.df[self.df[col] > val]
            elif op == '<':
                self.df = self.df[self.df[col] < val]
            elif op == '>=':
                self.df = self.df[self.df[col] >= val]
            elif op == '<=':
                self.df = self.df[self.df[col] <= val]
            elif op == '==':
                self.df = self.df[self.df[col] == val]
            elif op == '!=':
                self.df = self.df[self.df[col] != val]
            elif op == 'contains' and isinstance(self.df[col].dtype, object):
                self.df = self.df[self.df[col].str.contains(val, na=False)]
            
            final_count = len(self.df)
            removed_count = initial_count - final_count
            self.update_status_bar()
            messagebox.showinfo("筛选完成", f"高级筛选完成。\n\n移除了 {removed_count} 条记录。\n剩余记录数: {final_count}")

        except Exception as e:
            messagebox.showerror("筛选失败", f"应用筛选时发生错误:\n{e}")

    def reset_data(self):
        """Resets the dataframe to its original state after loading."""
        if self.original_df is None:
            messagebox.showwarning("无数据", "没有可恢复的原始数据。")
            return
        
        self.df = self.original_df.copy()
        self.update_status_bar()
        messagebox.showinfo("重置成功", "数据已恢复到初始加载状态。")

    def open_export_window(self):
        if self.df is None:
            messagebox.showwarning("无数据", "请先加载并筛选数据。")
            return
        ExportWindow(self.root, self.df)


class ExportWindow(tk.Toplevel):
    def __init__(self, parent, df):
        super().__init__(parent)
        self.title("导出数据并重命名列")
        self.geometry("600x400")
        self.df = df
        self.entries = {}

        # --- Main Frame with Canvas for scrolling ---
        main_frame = ttk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # --- Populate with column names and entry boxes ---
        ttk.Label(scrollable_frame, text="原始列名", font=('Helvetica', 10, 'bold')).grid(row=0, column=0, padx=5, pady=5, sticky='w')
        ttk.Label(scrollable_frame, text="新列名 (留空则不更改)", font=('Helvetica', 10, 'bold')).grid(row=0, column=1, padx=5, pady=5, sticky='w')

        for i, col in enumerate(self.df.columns, start=1):
            ttk.Label(scrollable_frame, text=col).grid(row=i, column=0, padx=5, pady=2, sticky='w')
            entry = ttk.Entry(scrollable_frame, width=30)
            entry.grid(row=i, column=1, padx=5, pady=2, sticky='w')
            self.entries[col] = entry

        # --- Bottom frame for action buttons ---
        bottom_frame = ttk.Frame(self)
        bottom_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=5)
        
        ttk.Button(bottom_frame, text="确认并导出", command=self.export_data).pack(side=tk.RIGHT, padx=10)
        ttk.Button(bottom_frame, text="取消", command=self.destroy).pack(side=tk.RIGHT)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def export_data(self):
        rename_map = {orig_col: entry.get() for orig_col, entry in self.entries.items() if entry.get()}
        
        try:
            df_to_export = self.df.rename(columns=rename_map)
            
            save_path = filedialog.asksaveasfilename(
                title="保存文件",
                defaultextension=".csv",
                filetypes=(("CSV 文件", "*.csv"), ("Stata DTA 文件", "*.dta"), ("所有文件", "*.*"))
            )

            if not save_path:
                return

            if save_path.lower().endswith('.dta'):
                df_to_export.to_stata(save_path, write_index=False, version=118)
            else:
                df_to_export.to_csv(save_path, index=False)
            
            messagebox.showinfo("导出成功", f"文件已成功保存到:\n{save_path}")
            self.destroy()

        except Exception as e:
            messagebox.showerror("导出失败", f"导出时发生错误:\n{e}")


if __name__ == "__main__":
    # To run this app, you might need pandas and its dependencies:
    # pip install pandas pyreadstat
    root = tk.Tk()
    app = DataFilterApp(root)
    root.mainloop()