
cd "D:\0 Trading\TGtoMT5"   

python -m app.cli.preview "BUY @ 3354/3350\n\nTP 3357\nTP 3361\nTP 3366\nTP OPEN\nSL 3349" --legs 5

python -m app.cli.preview "BUY @ 3354" --legs 5

python -m app.cli.preview "BUY @ 3354/3350" --legs 5


pause