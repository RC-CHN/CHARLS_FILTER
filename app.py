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
        self.export_queue = queue.Queue()

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

        # --- Paned Window for resizable layout ---
        paned_window = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        paned_window.pack(fill=tk.BOTH, expand=True, pady=10)

        # --- Left Pane: Column Selection ---
        left_pane = ttk.LabelFrame(paned_window, text="选择列 (单击预览, Ctrl/Shift多选)")
        paned_window.add(left_pane, weight=1)

        self.column_listbox = tk.Listbox(left_pane, selectmode=tk.EXTENDED, exportselection=False)
        v_scrollbar = ttk.Scrollbar(left_pane, orient=tk.VERTICAL, command=self.column_listbox.yview)
        h_scrollbar = ttk.Scrollbar(left_pane, orient=tk.HORIZONTAL, command=self.column_listbox.xview)
        self.column_listbox.config(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        self.column_listbox.bind('<<ListboxSelect>>', self.update_data_preview)

        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        self.column_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # --- Right Pane: Data Preview ---
        self.right_pane = ttk.LabelFrame(paned_window, text="数据预览")
        paned_window.add(self.right_pane, weight=2)

        self.data_preview_tree = ttk.Treeview(self.right_pane, columns=('Index', 'Value'), show='headings')
        self.data_preview_tree.heading('Index', text='行号')
        self.data_preview_tree.heading('Value', text='值')
        self.data_preview_tree.column('Index', width=80, anchor='center')
        self.data_preview_tree.column('Value', width=300)

        tree_scrollbar = ttk.Scrollbar(self.right_pane, orient=tk.VERTICAL, command=self.data_preview_tree.yview)
        self.data_preview_tree.config(yscrollcommand=tree_scrollbar.set)

        tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.data_preview_tree.pack(fill=tk.BOTH, expand=True)

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
        self.data_preview_tree.delete(*self.data_preview_tree.get_children()) # Clear preview
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
        self.update_data_preview() # Refresh the preview
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
            self.update_data_preview() # Refresh the preview
            messagebox.showinfo("筛选完成", f"高级筛选完成。\n\n移除了 {removed_count} 条记录。\n剩余记录数: {final_count}")

        except Exception as e:
            messagebox.showerror("筛选失败", f"应用筛选时发生错误:\n{e}")

    def update_data_preview(self, event=None):
        """Updates the treeview with data from the selected column."""
        if not self.column_listbox.curselection():
            return
            
        # Clear previous preview
        self.data_preview_tree.delete(*self.data_preview_tree.get_children())

        # Get selected column (only preview the first selected item)
        selected_index = self.column_listbox.curselection()[0]
        selected_col = self.column_listbox.get(selected_index)

        if self.df is None or selected_col not in self.df.columns:
            return

        # --- New: Link preview selection to advanced filter & show dtype ---
        self.adv_filter_col.set(selected_col)
        self.on_adv_col_selected() # Manually trigger the logic to update operators/values
        
        col_dtype = self.df[selected_col].dtype
        self.right_pane.config(text=f"预览: {selected_col} - 类型: {col_dtype}")
        # ----------------------------------------------------------------

        # Display all rows
        preview_df = self.df[[selected_col]]
        
        for index, row in preview_df.iterrows():
            value = row[selected_col]
            if pd.isna(value):
                display_value = "(空值)"
            else:
                display_value = str(value)
            self.data_preview_tree.insert("", "end", values=(index, display_value))

    def reset_data(self):
        """Resets the dataframe to its original state after loading."""
        if self.original_df is None:
            messagebox.showwarning("无数据", "没有可恢复的原始数据。")
            return
        
        self.df = self.original_df.copy()
        self.update_status_bar()
        self.update_data_preview() # Refresh the preview
        messagebox.showinfo("重置成功", "数据已恢复到初始加载状态。")

    def open_export_window(self):
        if self.df is None:
            messagebox.showwarning("无数据", "请先加载并筛选数据。")
            return
        # Pass the main app instance (self) to the dialog
        dialog = ExportDialog(self.root, self, self.df.columns)
        self.root.wait_window(dialog) # Wait until the dialog is closed

    def start_export_thread(self, df_to_export, save_path):
        """Starts the file exporting process in a background thread."""
        self.status_bar.config(text=f"正在后台导出: {os.path.basename(save_path)}...")
        
        thread = threading.Thread(target=self.data_exporter_worker, args=(df_to_export, save_path))
        thread.daemon = True
        thread.start()
        
        self.root.after(100, self.check_export_queue)

    def data_exporter_worker(self, df, path):
        """Worker function to save data to a file."""
        try:
            if path.lower().endswith('.dta'):
                df.to_stata(path, write_index=False, version=118)
            else: # Default to CSV
                df.to_csv(path, index=False)
            self.export_queue.put(("success", path))
        except Exception as e:
            self.export_queue.put(("error", str(e)))

    def check_export_queue(self):
        """Checks the queue for export status and shows a message."""
        try:
            status, data = self.export_queue.get_nowait()
            if status == "success":
                messagebox.showinfo("导出成功", f"文件已成功保存到:\n{data}")
            elif status == "error":
                messagebox.showerror("导出失败", f"导出时发生错误:\n{data}")
            
            self.update_status_bar() # Reset status bar text

        except queue.Empty:
            self.root.after(100, self.check_export_queue)


class ExportDialog(tk.Toplevel):
    def __init__(self, parent, app_instance, columns):
        super().__init__(parent)
        self.app = app_instance
        self.title("导出数据并重命名列 (双击'新列名'单元格进行编辑)")
        self.geometry("700x550")
        self.transient(parent)
        self.grab_set()

        self.columns = columns
        self.temp_entry = None
        self.editing_item_id = None # Store the ID of the item being edited

        # --- Top Button Frame ---
        top_btn_frame = ttk.Frame(self)
        top_btn_frame.pack(fill='x', padx=10, pady=5)
        ttk.Button(top_btn_frame, text="全部填充", command=self.fill_all).pack(side='left')
        ttk.Button(top_btn_frame, text="全部清空", command=self.clear_all).pack(side='left', padx=5)

        # --- Treeview Frame ---
        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.tree = ttk.Treeview(tree_frame, columns=('Original', 'New'), show='headings')
        self.tree.heading('Original', text='原始列名')
        self.tree.heading('New', text='新列名 (留空则不修改)')
        self.tree.column('Original', width=250)
        self.tree.column('New', width=250)

        for col in self.columns:
            self.tree.insert('', 'end', values=(col, ''))

        # --- Scrollbar ---
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        self.tree.pack(side='left', fill='both', expand=True)

        # --- Bindings ---
        self.tree.bind('<Double-1>', self.on_double_click)

        # --- Bottom Buttons ---
        bottom_btn_frame = ttk.Frame(self)
        bottom_btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(bottom_btn_frame, text="取消", command=self.destroy).pack(side='right', padx=5)
        ttk.Button(bottom_btn_frame, text="确认并导出", command=self.confirm_export).pack(side='right')

    def on_double_click(self, event):
        self.save_temp_entry() # Save any previously active entry
        
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            return

        column_id = self.tree.identify_column(event.x)
        if column_id != '#2': # Only allow editing the 'New' column
            return

        self.editing_item_id = self.tree.identify_row(event.y)
        if not self.editing_item_id:
            return
        
        x, y, width, height = self.tree.bbox(self.editing_item_id, column_id)

        self.temp_entry = ttk.Entry(self.tree, justify='left')
        self.temp_entry.place(x=x, y=y, width=width, height=height)
        
        current_value = self.tree.item(self.editing_item_id, 'values')[1]
        self.temp_entry.insert(0, current_value)
        self.temp_entry.focus_force()

        self.temp_entry.bind('<Return>', self.save_temp_entry)
        self.temp_entry.bind('<FocusOut>', self.save_temp_entry)
        self.temp_entry.bind('<Escape>', self.cancel_edit)

    def save_temp_entry(self, event=None):
        if self.temp_entry and self.editing_item_id:
            new_value = self.temp_entry.get()
            current_values = self.tree.item(self.editing_item_id, 'values')
            self.tree.item(self.editing_item_id, values=(current_values[0], new_value))
        
        self.destroy_temp_entry()

    def cancel_edit(self, event=None):
        self.destroy_temp_entry()

    def destroy_temp_entry(self):
        if self.temp_entry:
            self.temp_entry.destroy()
            self.temp_entry = None
            self.editing_item_id = None

    def fill_all(self):
        self.save_temp_entry()
        for item_id in self.tree.get_children():
            values = self.tree.item(item_id, 'values')
            self.tree.item(item_id, values=(values[0], values[0]))

    def clear_all(self):
        self.save_temp_entry()
        for item_id in self.tree.get_children():
            values = self.tree.item(item_id, 'values')
            self.tree.item(item_id, values=(values[0], ''))

    def confirm_export(self):
        self.save_temp_entry()
        rename_map = {}
        for item_id in self.tree.get_children():
            old_name, new_name = self.tree.item(item_id, 'values')
            new_name = str(new_name).strip()
            if new_name:
                rename_map[old_name] = new_name
        
        try:
            df_to_export = self.app.df.rename(columns=rename_map)
            
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
            
            messagebox.showinfo("导出成功", f"文件已成功保存到:\n{save_path}", parent=self)
            self.destroy()

        except Exception as e:
            messagebox.showerror("导出失败", f"导出时发生错误:\n{e}", parent=self)


if __name__ == "__main__":
    # To run this app, you might need pandas and its dependencies:
    # pip install pandas pyreadstat
    root = tk.Tk()
    app = DataFilterApp(root)
    root.mainloop()