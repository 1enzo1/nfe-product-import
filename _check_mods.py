import importlib
for m in ["pandas","openpyxl","lxml"]:
    try:
        importlib.import_module(m)
        print(m, "OK")
    except Exception as e:
        print(m, "ERR:", e)
