Remove-Item -Recurse -Force dist, build -ErrorAction SilentlyContinue
Remove-Item *.spec -Force -ErrorAction SilentlyContinue
python -m PyInstaller --onefile --windowed --name PriceParserGUI ui_parser.py# Сборка GUI (PriceParserGUI.exe)

## 1. Создать / активировать venv (один раз)
python -m venv .venv
.\.venv\Scripts\activate

## 2. Установить зависимости (при обновлении – повторить)
pip install --upgrade pip
pip install pandas rapidfuzz openpyxl xlrd pyinstaller

## 3. Очистить предыдущие сборки
Remove-Item -Recurse -Force dist, build -ErrorAction SilentlyContinue
Remove-Item *.spec -Force -ErrorAction SilentlyContinue

## 4. Собрать GUI (без консольного окна)
python -m PyInstaller --onefile --windowed --name PriceParserGUI ui_parser.py

## 5. Результат
dist\PriceParserGUI.exe

## 6. (Опционально) Сборка консольного варианта
python -m PyInstaller --onefile --name PriceParser parse.py

## 7. (Опционально) Иконка
python -m PyInstaller --onefile --windowed --icon app.ico --name PriceParserGUI ui_parser.py