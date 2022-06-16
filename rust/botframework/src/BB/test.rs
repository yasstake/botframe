use std::fmt::Debug;

use crate::exchange::Market;
use crate::exchange::MarketInfo;

#[cfg(test)]
fn load_test_data() -> Market {
    let market = Market::new();

    return market;
}

#[test]
fn test_load_data() {
    load_test_data();
}

use polars::chunked_array::ChunkedArray;
use polars::export::arrow::types::NativeType;
use polars::prelude::NamedFromOwned;
use polars::prelude::groupby;
use polars::prelude::ClosedWindow;
use polars::prelude::DataFrame;
#[cfg(test)]
use polars::prelude::Duration;
use polars::prelude::DynamicGroupOptions;
use polars::prelude::PolarsTemporalGroupby;

#[cfg(test)]
use crate::bb::log::load_dummy_data;

use polars::prelude::AnyValue;
use polars::prelude::UInt32Chunked;

use polars_lazy::prelude::*;
use polars::prelude::Series;

use chrono::NaiveDateTime;


// use polars_lazy::prelude::col;
// use polars_lazy::frame::LazyGroupBy;


#[test]
fn test_for_each() {
    let mut m = load_dummy_data();

    let df = m.df();

    let h = df.height();

    for i in 0..h {
        let row = df.get_row(i);
        println!("{:?}", row);
    }
}



// https://illumination-k.dev/techblog/posts/polars_pandas
// ここをみながらテスト
#[test]
fn test_load_dummy_data() {
    let mut m = load_dummy_data();

    m._print_head_history();
    m._print_tail_history();

    let df = m.df();

    let t = df.column("time").unwrap();

    let mut new_t: Series = t.datetime().expect("nottype").into_iter().map(
        |x| (x.unwrap()/10000) as i64 * 10000
    ).collect();

    new_t.rename("time_slot");

    println!("{}", new_t);

    let mut new_df = df.hstack(&[new_t]).unwrap();

    /*
    let dfl = m.df().lazy();
    let sort_df = df.groupby_with_series(vec![new_t], true, true).unwrap()
        .agg([
        col("price").first().alias("open"),
        col("price").max().alias("high"),
        col("price").min().alias("low"),
        col("price").last().alias("close"),
        col("size").sum().alias("vol"),
        ]
    )
    .sort("time", Default::default()).collect().unwrap();

   */

    println!("{}", new_df.head(Some(5)));

    let dfl = new_df.lazy();

    let g = dfl.groupby([col("time_slot")])
    .agg([
        col("time").first(),
        col("price").first().alias("open"),
        col("price").max().alias("high"),
        col("price").min().alias("low"),
        col("price").last().alias("close"),
        col("size").sum().alias("vol"),


        /*
        if(col("side")==1) {
            col("size").sum().alias("vols"), 
        }
        */

        ]
    )
    .sort("time", Default::default()).collect().unwrap();

    println!("{}", g);

    let dfl = m.df().lazy();

    let g = (dfl.groupby_dynamic(
        vec![],
        DynamicGroupOptions {
            index_column: "time".into(),
            every: Duration::parse("60s"),
            period: Duration::parse("60s"),
            offset: Duration::parse("0"),
            truncate: true,
            include_boundaries: false,
            closed_window: ClosedWindow::Left,
        },
    ))
    .agg([
            col("price").first(),

            // col("price").max(),
            /*            
            col("price").min(),
            col("price").last(),
            */                        
            col("size").sum(),
            ]
    )
    .sort("time", Default::default()).collect().unwrap();

    println!("{}", g);
/*
    //df.groupby([col("tbime")]).agg

    //let new_time = time_chunk.iter().map(|x| x ).collect::<ChunkedArray<AnyValue>>();

    // let new_chunk = time_chunk.iter().map(|x| {return (x as i32)}).collect::<UInt32Chunked>();

*/

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

/*
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
    ));


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
*/    
}
