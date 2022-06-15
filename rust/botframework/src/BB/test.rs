use crate::exchange::Market;

#[cfg(test)]
fn load_test_data() -> Market {
    let market = Market::new();

    return market;
}

#[test]
fn test_load_data() {
    load_test_data();
}

use polars::prelude::groupby;
use polars::prelude::ClosedWindow;
use polars::prelude::DataFrame;
#[cfg(test)]
use polars::prelude::Duration;
use polars::prelude::DynamicGroupOptions;
use polars::prelude::PolarsTemporalGroupby;

#[cfg(test)]
use crate::bb::log::load_dummy_data;

// https://illumination-k.dev/techblog/posts/polars_pandas
// ここをみながらテスト
#[test]
fn test_load_dummy_data() {
    let mut m = load_dummy_data();

    m._print_head_history();
    m._print_tail_history();

    let df = m.df();

    // df.upsample(by, time_column, every, offset)

    //df.groupby_rolling(by, options)
    /*
        let result =     (df.groupby_dynamic(vec!["time"], "1m"))
            .agg([
                pl.col("time").count(),
                pl.col("time").max(),
                pl.sum("values"),
            ]);

    */

//    let group = df.groupby("time")?.

    let (a, b, c) = (df.groupby_dynamic(
        vec![],
        &DynamicGroupOptions {
            index_column: "time".into(),
            every: Duration::parse("60s"),
            period: Duration::parse("60s"),
            offset: Duration::parse("0"),
            truncate: true,
            include_boundaries: false,
            closed_window: ClosedWindow::Left,
        },
    ))
    .unwrap();

    println!("{}", a);
    println!("{}", b.len());

    //c.sort();

    let list_chunked = c.as_list_chunked();

    println!("{}", list_chunked.name());

    let mut sum = 0;
    for g in c.iter() {
        let n = g.len();
        sum += n;

        println!("{}", n);
    }

    println!("Total = {} / {}", sum, df.shape().0);

    // df.groupby(by).upsample("time", "1s", "0s");
}
