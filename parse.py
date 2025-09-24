import pandas as pd
from rapidfuzz import process, fuzz
import re
import os

DEFAULT_MIN_MATCH_SCORE = 65  # минимальный процент совпадения по умолчанию


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


def extract_articles(text: str):
    if not isinstance(text, str):
        return []
    text = text.upper()
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

    nom_header = find_header_row(nom_path, search_terms=("Артикул",))
    nomenclature_df = pd.read_excel(
        nom_path, engine=smart_engine(nom_path), header=nom_header
    )
    nomenclature_df.rename(columns=lambda c: str(c).strip(), inplace=True)

    if "Артикул" not in nomenclature_df.columns:
        raise RuntimeError("Не найдена колонка 'Артикул' в номенклатуре")

    nomenclature_df["Нормализованный артикул"] = nomenclature_df["Артикул"].apply(
        normalize_article
    )
    nomenclature_articles = nomenclature_df["Нормализованный артикул"].tolist()

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
        if any(k in str(col).lower() for k in ["кол-во", "количество", "qty", "шт"])
    ]

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

        matched = False

        for art in extracted_all:
            norm_art = normalize_article(art)
            client_core = get_article_core(art)
            best_row = None
            best_score = -1

            for match, score, idx in process.extract(
                norm_art, nomenclature_articles, scorer=fuzz.WRatio, limit=10
            ):
                nom_row = nomenclature_df.iloc[idx]
                nomen_core = get_article_core(nom_row["Артикул"])
                if client_core and nomen_core == client_core:
                    best_row, best_score = nom_row, 100
                    break
                elif score > best_score:
                    best_row, best_score = nom_row, score

            if best_row is not None and best_score >= min_score:
                results.append(
                    {
                        "Исходный текст": " | ".join(raw_texts[:3]),
                        "Артикул клиента": art,
                        "Совпадение (номенклатура)": best_row["Артикул"],
                        "Процент совпадения": best_score,
                    }
                )
                matched = True
                break

        if not matched and raw_texts:
            txt = raw_texts[0]
            match, score, idx = process.extractOne(
                txt.upper(),
                nomenclature_df["Артикул"].astype(str).tolist(),
                scorer=fuzz.WRatio,
            )
            if score >= min_score:
                best_row = nomenclature_df.iloc[idx]
                results.append(
                    {
                        "Исходный текст": txt,
                        "Артикул клиента": "",
                        "Совпадение (номенклатура)": best_row["Артикул"],
                        "Процент совпадения": score,
                    }
                )

    pd.DataFrame(results).to_excel(output_path, index=False)
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
