cd /d "D:\0 Trading\TGtoMT5"

set ENGINE_RULES_MODE=semantic
set SEMANTIC_DICT_PATH=runtime/data/parser_semantic.yaml
set ENGINE_FAILSAFE_ON_UNPARSED=true
set MAX_LEGS=5
set DEFAULT_LEG_VOLUME=0.01
set SIGNAL_REQUIRE_SYMBOL=false
set SIGNAL_REQUIRE_PRICE=true

"C:\Users\begoo\AppData\Local\Programs\Python\Python313\python.exe" -m scripts.run_golden_tests

pause