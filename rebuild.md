Remove-Item -Recurse -Force dist, build -ErrorAction SilentlyContinue
Remove-Item *.spec -Force -ErrorAction SilentlyContinue
pyinstaller --onefile --windowed --name PriceParserGUI ui_parser.py