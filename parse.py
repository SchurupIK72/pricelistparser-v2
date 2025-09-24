import pandas as pd
from rapidfuzz import process, fuzz
import re
import os

DEFAULT_MIN_MATCH_SCORE = 65  # минимальный процент совпадения по умолчанию
OUTPUT_COLUMNS = [
    "Исходные тексты",
    "Извлеченный артикул",
    "Нормализованный артикул клиента",
    "Совпадение (из номенклатуры)",
    "Название (из номенклатуры)",
    "Нормализованный артикул совпадения",
    "Процент совпадения",
    "Цена",
    "Количество (из заказа)",
]


def smart_engine(path: str) -> str:
    ext = os.path.splitext(path.lower())[1]
    if ext == ".xlsx":
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    return "openpyxl"


def normalize_article(article: str) -> str:
    if not isinstance(article, str):
        return ""
    article = article.upper()
    tokens = re.findall(r"[A-ZА-Я0-9]+", article)
    tokens = sorted(tokens)
    return "".join(tokens)


def get_article_core(article: str) -> str:
    if not isinstance(article, str):
        return ""
    article = article.upper()
    article = re.sub(r"(-СПЕЦМАШ|-РК|-СБ|-В2|-01|-02|-03|-10-Р|-Р)$", "", article)
    tokens = re.findall(r"[A-ZА-Я0-9]+", article)
    tokens = sorted(tokens)
    return "".join(tokens)

def extract_numeric_core(article: str) -> str:
    """Возвращает самую длинную числовую последовательность (ядро), длиной >=5.
    Используется для приоритезации кандидатов с одинаковым базовым номером.
    """
    if not isinstance(article, str):
        return ""
    nums = re.findall(r"[0-9]+", article)
    nums = sorted(nums, key=len, reverse=True)
    for n in nums:
        if len(n) >= 5:
            return n
    return nums[0] if nums else ""


def extract_articles(text: str):
    if not isinstance(text, str):
        return []
    text = text.upper()
    # Нормализуем дефисы (– — ‑ − …) и неразрывные пробелы
    def _normalize_separators(s: str) -> str:
        if not isinstance(s, str):
            return ""
        hyphens = "\u2010\u2011\u2012\u2013\u2014\u2212"  # ‐ ‑ ‒ – — −
        trans = {ord(c): "-" for c in hyphens}
        trans[0xA0] = ord(" ")  # NBSP -> space
        return s.translate(trans)

    text = _normalize_separators(text)
    # Ищем группы из букв/цифр/дефисов длиной >=4
    candidates = re.findall(r"[A-ZА-Я0-9\-]{4,}", text)
    return candidates


def find_header_row(path: str, sheet_name=0, search_terms=("Артикул", "Код", "Номер", "Товар")) -> int:
    preview = pd.read_excel(
        path, sheet_name=sheet_name, header=None, nrows=60, engine=smart_engine(path)
    )
    for i in range(len(preview)):
        row_values = (
            preview.iloc[i]
            .astype(str)
            .str.replace("\n", " ")
            .str.strip()
            .str.lower()
            .tolist()
        )
        for term in search_terms:
            t = term.lower()
            if any(t == v or v.startswith(t) for v in row_values):
                return i
    return -1


def find_header_row_strict(
    path: str,
    sheet_name=0,
    search_terms=("Номенклатура", "Артикул", "Цена"),
    min_matches: int = 2,
    preview_rows: int = 80,
):
    preview = pd.read_excel(
        path, sheet_name=sheet_name, header=None, nrows=preview_rows, engine=smart_engine(path)
    )
    terms_lower = [t.lower() for t in search_terms]
    for i in range(len(preview)):
        row_values = (
            preview.iloc[i]
            .astype(str)
            .str.replace("\n", " ")
            .str.strip()
            .str.lower()
            .tolist()
        )
        exact_count = sum(1 for t in terms_lower if t in row_values)
        if exact_count >= min_matches:
            return i
    return -1


def main_process(
    client_path: str = None,
    nom_path: str = None,
    output_path: str = None,
    min_score: int = DEFAULT_MIN_MATCH_SCORE,
    interactive: bool = False,
):
    if interactive:
        print("=== Интерактивный режим ===")
        client_path = (
            input(f"Файл клиента [{client_path or 'price_client.xlsx'}]: ").strip()
            or (client_path or "price_client.xlsx")
        )
        nom_path = (
            input(f"Файл номенклатуры [{nom_path or 'nomenclature.xlsx'}]: ").strip()
            or (nom_path or "nomenclature.xlsx")
        )
        output_path = (
            input(f"Файл результата [{output_path or 'result.xlsx'}]: ").strip()
            or (output_path or "result.xlsx")
        )
        min_score_input = input(
            f"Минимальный процент совпадения [{min_score}]: "
        ).strip()
        try:
            min_score = int(min_score_input) if min_score_input else min_score
        except:
            pass
        print("\nПроверьте параметры:")
        print(f"  Клиентский файл: {client_path}")
        print(f"  Номенклатура:    {nom_path}")
        print(f"  Результат:       {output_path}")
        print(f"  Порог:           {min_score}")
        ok = input("Продолжить? (Y/n): ").strip().lower()
        if ok == "n":
            print("Отменено пользователем.")
            return 0

    client_path = client_path or "price_client.xlsx"
    nom_path = nom_path or "nomenclature.xlsx"
    output_path = output_path or "result.xlsx"

    if not os.path.exists(client_path):
        raise FileNotFoundError(f"Файл клиента не найден: {client_path}")
    if not os.path.exists(nom_path):
        raise FileNotFoundError(f"Файл номенклатуры не найден: {nom_path}")

    print(f"[INFO] Файл клиента: {client_path}")
    print(f"[INFO] Файл номенклатуры: {nom_path}")
    print(f"[INFO] Файл результата: {output_path}")
    print(f"[INFO] Порог совпадения: {min_score}")

    client_header = find_header_row(client_path)
    client_df = pd.read_excel(
        client_path, engine=smart_engine(client_path), header=client_header
    )

    # Для номенклатуры ищем строгую строку заголовка, чтобы не спутать с содержимым
    nom_header = find_header_row_strict(
        nom_path, search_terms=("Номенклатура", "Артикул", "Цена"), min_matches=2
    )
    nomenclature_df = pd.read_excel(
        nom_path, engine=smart_engine(nom_path), header=nom_header
    )
    nomenclature_df.rename(columns=lambda c: str(c).strip(), inplace=True)

    if "Артикул" not in nomenclature_df.columns:
        raise RuntimeError("Не найдена колонка 'Артикул' в номенклатуре")

    nomenclature_df["Нормализованный артикул"] = nomenclature_df["Артикул"].apply(normalize_article)
    nomenclature_articles = nomenclature_df["Нормализованный артикул"].tolist()
    # Карта для мгновенного точного совпадения по нормализованному артикулу
    norm_to_index = {}
    for idx, val in enumerate(nomenclature_articles):
        # Если дубль, оставим первый — поведение можно расширить при необходимости
        norm_to_index.setdefault(val, idx)

    client_article_cols = [
        col
        for col in client_df.columns
        if any(k in str(col).lower() for k in ["артик", "код", "номер"])
    ]
    description_cols = [
        c
        for c in client_df.columns
        if any(k in str(c).lower() for k in ["товар", "опис", "наимен", "назв"])
    ]
    quantity_cols = [
        col
        for col in client_df.columns
        if any(k in str(col).lower() for k in ["кол-во", "количество", "qty", "шт", "заказ"])
    ]
    price_cols = [
        col
        for col in client_df.columns
        if any(k in str(col).lower() for k in ["цена", "стоим", "price", "руб", "cost", "amount"])
    ]

    # Попробуем найти название товара и цену в номенклатуре
    nom_name_col = None
    nom_price_col = None
    for c in nomenclature_df.columns:
        lc = str(c).lower()
        if nom_name_col is None and any(k in lc for k in ["номенк", "наимен", "назв", "товар", "опис"]):
            nom_name_col = c
        if nom_price_col is None and any(k in lc for k in ["цена", "стоим", "price", "руб", "cost", "amount"]):
            nom_price_col = c
        if nom_name_col and nom_price_col:
            break

    # Индекс по числовому ядру артикула -> список индексов в номенклатуре
    num_core_to_indices = {}
    for idx, art in enumerate(nomenclature_df["Артикул"].astype(str)):
        nc = extract_numeric_core(art)
        if nc:
            num_core_to_indices.setdefault(nc, []).append(idx)

    results = []

    for _, row in client_df.iterrows():
        raw_texts = []
        for col in client_article_cols + description_cols:
            val = row.get(col, "")
            if isinstance(val, str) and val.strip():
                raw_texts.append(val)

        extracted_all = []
        for txt in raw_texts:
            extracted_all.extend(extract_articles(txt))
        extracted_all = list(dict.fromkeys(extracted_all))

        # Выбираем лучший матч по всем кандидатам, а не первый выше порога
        chosen = None  # tuple: (art, norm_art, best_row, best_score, priority_tuple)
        raw_join_upper = (" | ".join(raw_texts)).upper() if raw_texts else ""
        for art in extracted_all:
            norm_art = normalize_article(art)
            client_core = get_article_core(art)
            best_row = None
            best_score = -1

            # 1) Точное совпадение по нормализованному артикулу
            exact_idx = norm_to_index.get(norm_art)
            if exact_idx is not None:
                best_row, best_score = nomenclature_df.iloc[exact_idx], 100
                priority = (2, 100, len(norm_art))  # 2 — highest tier
            else:
                # 2) Приоритет по одинаковому числовому ядру + сравнение по названию
                nc = extract_numeric_core(art)
                priority = (0, 0, 0)
                if nc and nc in num_core_to_indices:
                    name_best = -1
                    name_best_row = None
                    for idx in num_core_to_indices[nc]:
                        nom_row = nomenclature_df.iloc[idx]
                        name_val = str(nom_row.get(nom_name_col, "")) if nom_name_col else ""
                        name_score = fuzz.WRatio(raw_join_upper, name_val.upper()) if name_val else 0
                        if name_score > name_best:
                            name_best = name_score
                            name_best_row = nom_row
                    if name_best_row is not None:
                        best_row = name_best_row
                        best_score = name_best  # используем как общую уверенность
                        priority = (1, name_best, len(norm_art))  # 1 — mid tier

                # 3) Общий fuzzy-поиск по нормализованным артикулам (если ещё не выбрали)
                if best_row is None:
                    for match, score, idx in process.extract(
                        norm_art, nomenclature_articles, scorer=fuzz.WRatio, limit=10
                    ):
                        nom_row = nomenclature_df.iloc[idx]
                        nomen_core = get_article_core(nom_row["Артикул"])
                        if client_core and nomen_core == client_core:
                            best_row, best_score = nom_row, 100
                            priority = (2, 100, len(norm_art))
                            break
                        elif score > best_score:
                            best_row, best_score = nom_row, score
                            priority = (0, score, len(norm_art))

            if best_row is not None:
                if chosen is None:
                    chosen = (art, norm_art, best_row, best_score, priority)
                else:
                    # сравниваем по priority tuple
                    if priority > chosen[4]:
                        chosen = (art, norm_art, best_row, best_score, priority)

            # (дубликат старой логики удален)

        matched = False
        if chosen is not None and chosen[3] >= min_score:
            art, norm_art, best_row, best_score, _ = chosen
            # Цена и количество из заказа (первое найденное поле)
            price_val_client = None
            for pc in price_cols:
                v = row.get(pc, None)
                if pd.notna(v) and str(v).strip() != "":
                    price_val_client = v
                    break
            qty_val = None
            for qc in quantity_cols:
                v = row.get(qc, None)
                if pd.notna(v) and str(v).strip() != "":
                    qty_val = v
                    break

            # Цена из номенклатуры приоритетнее
            price_val_nom = best_row.get(nom_price_col, None) if nom_price_col else None

            results.append(
                {
                    "Исходные тексты": " | ".join(raw_texts) if raw_texts else "",
                    "Извлеченный артикул": art,
                    "Нормализованный артикул клиента": norm_art,
                    "Совпадение (из номенклатуры)": best_row["Артикул"],
                    "Название (из номенклатуры)": best_row.get(nom_name_col, "") if nom_name_col else "",
                    "Нормализованный артикул совпадения": normalize_article(best_row["Артикул"]),
                    "Процент совпадения": best_score,
                    "Цена": (price_val_nom if (price_val_nom is not None and str(price_val_nom).strip() != "") else (price_val_client if price_val_client is not None else "")),
                    "Количество (из заказа)": qty_val if qty_val is not None else "",
                }
            )
            matched = True

        if not matched and raw_texts:
            # Фолбэк: используем объединенный текст, чтобы не терять артикули внутри длинных строк
            txt = " | ".join(raw_texts)
            match, score, idx = process.extractOne(
                txt.upper(),
                nomenclature_df["Артикул"].astype(str).tolist(),
                scorer=fuzz.WRatio,
            )
            if score >= min_score:
                best_row = nomenclature_df.iloc[idx]

                # Цена и количество из заказа (первое найденное поле)
                price_val_client = None
                for pc in price_cols:
                    v = row.get(pc, None)
                    if pd.notna(v) and str(v).strip() != "":
                        price_val_client = v
                        break
                qty_val = None
                for qc in quantity_cols:
                    v = row.get(qc, None)
                    if pd.notna(v) and str(v).strip() != "":
                        qty_val = v
                        break

                # Цена из номенклатуры приоритетнее
                price_val_nom = best_row.get(nom_price_col, None) if nom_price_col else None

                results.append(
                    {
                        "Исходные тексты": txt,
                        "Извлеченный артикул": "",
                        "Нормализованный артикул клиента": "",
                        "Совпадение (из номенклатуры)": best_row["Артикул"],
                        "Название (из номенклатуры)": best_row.get(nom_name_col, "") if nom_name_col else "",
                        "Нормализованный артикул совпадения": normalize_article(best_row["Артикул"]),
                        "Процент совпадения": score,
                        "Цена": (price_val_nom if (price_val_nom is not None and str(price_val_nom).strip() != "") else (price_val_client if price_val_client is not None else "")),
                        "Количество (из заказа)": qty_val if qty_val is not None else "",
                    }
                )

    # Сформируем результирующий DataFrame
    df_result = pd.DataFrame(results)

    # Гарантируем одинаковые столбцы и порядок в любом режиме (GUI/консоль)
    for col in OUTPUT_COLUMNS:
        if col not in df_result.columns:
            df_result[col] = ""
    # Отбрасываем лишние столбцы и упорядочиваем по эталону
    df_result = df_result[OUTPUT_COLUMNS]

    # Запишем в Excel и подсветим зеленым строки со 100% совпадением
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        sheet_name = "Sheet1"
        df_result.to_excel(writer, index=False, sheet_name=sheet_name)

        # Получаем лист и применяем заливку строкам с 100%
        ws = writer.sheets[sheet_name]
        try:
            from openpyxl.styles import PatternFill
        except Exception:
            PatternFill = None

        if PatternFill is not None and "Процент совпадения" in df_result.columns:
            green_fill = PatternFill(fill_type="solid", start_color="C6EFCE", end_color="C6EFCE")
            # Данные начинаются со 2-й строки (1-я — заголовки)
            for r_idx, score in enumerate(df_result["Процент совпадения"], start=2):
                # Аккуратно приводим к float (поддержка строк с запятой)
                is_hundred = False
                try:
                    val = float(str(score).replace(",", "."))
                    is_hundred = abs(val - 100.0) < 1e-9
                except Exception:
                    pass
                if is_hundred:
                    for c_idx in range(1, len(OUTPUT_COLUMNS) + 1):
                        ws.cell(row=r_idx, column=c_idx).fill = green_fill

    print(f"✅ Готово! Найдено совпадений: {len(results)}")
    return len(results)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Сопоставление артикулов")
    parser.add_argument("--client", "-c", help="Файл клиента")
    parser.add_argument("--nomenclature", "-n", help="Файл номенклатуры")
    parser.add_argument("--output", "-o", help="Файл результата")
    parser.add_argument("--min-score", type=int, default=DEFAULT_MIN_MATCH_SCORE)
    parser.add_argument("--interactive", "-i", action="store_true")
    args = parser.parse_args()

    if args.interactive:
        main_process(interactive=True)
    else:
        main_process(
            client_path=args.client,
            nom_path=args.nomenclature,
            output_path=args.output,
            min_score=args.min_score,
        )
