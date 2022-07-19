
/* 取引所、通貨ごとにテーブルつくったほうが良さそう。 */


create TABLE execution (
    time    int,            /* unixtime in ns */
    buy     char,
    price   int,
    size    int,
    id      string,         /* unique 制約 */
    liquid  bool
)

