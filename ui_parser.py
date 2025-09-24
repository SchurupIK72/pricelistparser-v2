import tkinter as tk
from tkinter import filedialog, messagebox
import threading
import os
import parse  # подключаем parse.py

class ParserApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Парсер прайсов")

        tk.Label(root, text="Файл клиента:").grid(row=0, column=0, sticky="w")
        self.client_entry = tk.Entry(root, width=60); self.client_entry.grid(row=0, column=1)
        tk.Button(root, text="Обзор", command=self.browse_client).grid(row=0, column=2)

        tk.Label(root, text="Файл номенклатуры:").grid(row=1, column=0, sticky="w")
        self.nom_entry = tk.Entry(root, width=60); self.nom_entry.grid(row=1, column=1)
        tk.Button(root, text="Обзор", command=self.browse_nom).grid(row=1, column=2)

        tk.Label(root, text="Файл результата:").grid(row=2, column=0, sticky="w")
        self.out_entry = tk.Entry(root, width=60); self.out_entry.grid(row=2, column=1)
        tk.Button(root, text="Сохранить как", command=self.save_output).grid(row=2, column=2)

        tk.Label(root, text="Порог % совпадения:").grid(row=3, column=0, sticky="w")
        self.score_entry = tk.Entry(root, width=10); self.score_entry.insert(0, str(parse.DEFAULT_MIN_MATCH_SCORE))
        self.score_entry.grid(row=3, column=1, sticky="w")

        self.start_button = tk.Button(root, text="Запустить", command=self.start_process)
        self.start_button.grid(row=4, column=0, columnspan=3, pady=10)

        self.log_text = tk.Text(root, height=10, width=80, state="disabled")
        self.log_text.grid(row=5, column=0, columnspan=3)

    def browse_client(self):
        fn = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
        if fn: self.client_entry.delete(0, tk.END); self.client_entry.insert(0, fn)

    def browse_nom(self):
        fn = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
        if fn: self.nom_entry.delete(0, tk.END); self.nom_entry.insert(0, fn)

    def save_output(self):
        fn = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
        if fn: self.out_entry.delete(0, tk.END); self.out_entry.insert(0, fn)

    def start_process(self):
        client = self.client_entry.get().strip()
        nom = self.nom_entry.get().strip()
        out = self.out_entry.get().strip()
        try: score = int(self.score_entry.get().strip())
        except: score = parse.DEFAULT_MIN_MATCH_SCORE

        if not client or not os.path.exists(client):
            messagebox.showerror("Ошибка", "Файл клиента не выбран"); return
        if not nom or not os.path.exists(nom):
            messagebox.showerror("Ошибка", "Файл номенклатуры не выбран"); return
        if not out:
            messagebox.showerror("Ошибка", "Укажите файл для результата"); return

        threading.Thread(target=self.run_parse, args=(client, nom, out, score), daemon=True).start()

    def run_parse(self, client, nom, out, score):
        self.log("Запуск...\n")
        try:
            count = parse.main_process(client_path=client, nom_path=nom, output_path=out, min_score=score, interactive=False)
            self.log(f"✅ Готово! Найдено совпадений: {count}\nФайл: {out}\n")
            messagebox.showinfo("Успех", f"Результат сохранен в {out}")
        except Exception as e:
            self.log(f"❌ Ошибка: {e}\n")
            messagebox.showerror("Ошибка", str(e))

    def log(self, text):
        self.log_text.config(state="normal")
        self.log_text.insert("end", text)
        self.log_text.config(state="disabled")
        self.log_text.see("end")

if __name__ == "__main__":
    root = tk.Tk()
    app = ParserApp(root)
    root.mainloop()
