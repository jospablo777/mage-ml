use column_atlas_native::engine::{Table, DEFAULT_ORDER_CACHE_BYTES};
use column_atlas_native::{AtlasError, ViewSpec};
use polars::prelude::*;
use serde_json::{json, Value};
use tempfile::TempDir;

struct Fixture {
    _dir: TempDir,
    path: String,
}

fn write(frame: &mut DataFrame) -> Fixture {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data.parquet");
    let file = std::fs::File::create(&path).unwrap();
    // Small row groups, so windows and gathers cross row group boundaries.
    ParquetWriter::new(file)
        .with_row_group_size(Some(7))
        .finish(frame)
        .unwrap();
    Fixture {
        _dir: dir,
        path: path.to_string_lossy().to_string(),
    }
}

/// 40 rows of mixed types with nulls, NaN, infinity and duplicate sort keys.
fn mixed() -> Fixture {
    let n = 40usize;
    let ids: Vec<i64> = (0..n as i64).collect();
    let groups: Vec<Option<&str>> = (0..n)
        .map(|i| match i % 4 {
            0 => Some("a"),
            1 => Some("b"),
            2 => None,
            _ => Some("ñ \"x\""),
        })
        .collect();
    let scores: Vec<Option<f64>> = (0..n)
        .map(|i| match i {
            3 => None,
            5 => Some(f64::NAN),
            7 => Some(f64::INFINITY),
            _ => Some(i as f64 / 4.0),
        })
        .collect();
    let flags: Vec<Option<bool>> = (0..n)
        .map(|i| if i % 5 == 0 { None } else { Some(i % 2 == 0) })
        .collect();
    let big: Vec<u64> = (0..n).map(|i| u64::MAX - i as u64).collect();
    let ties: Vec<i32> = (0..n).map(|i| (i % 3) as i32).collect();
    let mut frame = df!(
        "id" => ids,
        "group" => groups,
        "score" => scores,
        "flag" => flags,
        "big" => big,
        "tie" => ties,
    )
    .unwrap();
    let category = frame
        .column("group")
        .unwrap()
        .cast(&DataType::from_categories(Categories::global()))
        .unwrap();
    frame
        .with_column(category.with_name("category".into()))
        .unwrap();
    let days: Vec<i32> = (0..n as i32).map(|i| 19_723 + i).collect(); // 2024-01-01 onwards
    let date = Series::new("day".into(), days)
        .cast(&DataType::Date)
        .unwrap();
    frame.with_column(date.into()).unwrap();
    let micros: Vec<i64> = (0..n as i64)
        .map(|i| 1_704_110_400_000_000 + i * 3_600_000_000)
        .collect(); // 2024-01-01 12:00 UTC
    let zoned = Series::new("at".into(), micros)
        .cast(&DataType::Datetime(
            TimeUnit::Microseconds,
            Some(
                TimeZone::opt_try_new(Some("America/Costa_Rica"))
                    .unwrap()
                    .unwrap(),
            ),
        ))
        .unwrap();
    frame.with_column(zoned.into()).unwrap();
    let decimal = Series::new(
        "price".into(),
        (0..n)
            .map(|i| format!("{}.{:02}", i, i % 100))
            .collect::<Vec<_>>(),
    )
    .strict_cast(&DataType::Decimal(12, 2))
    .unwrap();
    frame.with_column(decimal.into()).unwrap();
    let lists: Vec<Series> = (0..n)
        .map(|i| Series::new("".into(), vec![i as i64, i as i64 + 1]))
        .collect();
    frame
        .with_column(Series::new("items".into(), lists).into())
        .unwrap();
    write(&mut frame)
}

fn open(fixture: &Fixture) -> Table {
    Table::open(&fixture.path, DEFAULT_ORDER_CACHE_BYTES).unwrap()
}

fn view(value: Value) -> ViewSpec {
    ViewSpec::parse(&value.to_string()).unwrap()
}

fn id_of(table: &Table, name: &str) -> usize {
    table
        .columns()
        .iter()
        .position(|column| column.name == name)
        .unwrap()
}

fn column_values(table: &Table, spec: &ViewSpec, name: &str) -> Vec<Option<String>> {
    let total = table.count(spec).unwrap() as usize;
    let id = id_of(table, name);
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < total {
        // Windows of 6 rows over row groups of 7: every window crosses a boundary somewhere.
        let rows = table.rows(spec, offset, 6, &[id]).unwrap();
        assert_eq!(rows.offset, offset);
        values.extend(rows.rows.into_iter().map(|mut row| row.remove(0)));
        offset += 6;
    }
    values
}

#[test]
fn metadata_lists_columns_with_kinds() {
    let fixture = mixed();
    let table = open(&fixture);
    let metadata = table.metadata();
    assert_eq!(metadata["row_count"], 40);
    let kinds: Vec<(&str, &str)> = metadata["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| {
            (
                column["name"].as_str().unwrap(),
                column["kind"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("id", "numeric"),
            ("group", "string"),
            ("score", "numeric"),
            ("flag", "boolean"),
            ("big", "numeric"),
            ("tie", "numeric"),
            ("category", "string"),
            ("day", "temporal"),
            ("at", "temporal"),
            ("price", "numeric"),
            ("items", "nested"),
        ]
    );
    assert_eq!(metadata["columns"][4]["exact_integer"], true);
    assert_eq!(
        metadata["columns"][8]["dtype"],
        "Datetime(μs, America/Costa_Rica)"
    );
}

#[test]
fn identity_windows_are_contiguous_and_formatted() {
    let fixture = mixed();
    let table = open(&fixture);
    let all = ViewSpec::default();
    let ids = column_values(&table, &all, "id");
    assert_eq!(
        ids,
        (0..40).map(|i| Some(i.to_string())).collect::<Vec<_>>()
    );

    let rows = table
        .rows(&all, 0, 8, &(0..11).collect::<Vec<_>>())
        .unwrap();
    let row = &rows.rows[3];
    assert_eq!(row[1].as_deref(), Some("ñ \"x\""));
    assert_eq!(row[2], None); // a missing score
    assert_eq!(rows.rows[5][2].as_deref(), Some("NaN"));
    assert_eq!(rows.rows[7][2].as_deref(), Some("inf"));
    assert_eq!(rows.rows[0][4].as_deref(), Some("18446744073709551615"));
    assert_eq!(rows.rows[1][6].as_deref(), Some("b"));
    assert_eq!(rows.rows[0][7].as_deref(), Some("2024-01-01"));
    assert_eq!(rows.rows[0][9].as_deref(), Some("0.00"));
    assert_eq!(rows.rows[2][10].as_deref(), Some("[2,3]"));
    assert!(rows.rows[0][8]
        .as_deref()
        .unwrap()
        .starts_with("2024-01-01 06:00:00"));

    assert!(table.rows(&all, 40, 5, &[0]).unwrap().rows.is_empty());
    assert_eq!(table.rows(&all, 38, 5, &[0]).unwrap().rows.len(), 2);
}

#[test]
fn window_limits_are_enforced() {
    let fixture = mixed();
    let table = open(&fixture);
    let all = ViewSpec::default();
    assert!(matches!(
        table.rows(&all, 0, 0, &[0]),
        Err(AtlasError::Invalid(_))
    ));
    assert!(matches!(
        table.rows(&all, 0, 513, &[0]),
        Err(AtlasError::Invalid(_))
    ));
    assert!(matches!(
        table.rows(&all, 0, 5, &[]),
        Err(AtlasError::Invalid(_))
    ));
    assert!(matches!(
        table.rows(&all, 0, 5, &[99]),
        Err(AtlasError::Invalid(_))
    ));
    let filters: Vec<Value> = (0..17)
        .map(|_| json!({"column_id": 0, "op": "is_null"}))
        .collect();
    assert!(ViewSpec::parse(&json!({ "filters": filters }).to_string()).is_err());
    assert!(ViewSpec::parse(r#"{"filters":[],"sort":[],"sql":"drop"}"#).is_err());
}

#[test]
fn filters_by_type() {
    let fixture = mixed();
    let table = open(&fixture);
    let id = |name| id_of(&table, name);
    let count = |value: Value| table.count(&view(value)).unwrap();

    assert_eq!(
        count(json!({"filters": [{"column_id": id("id"), "op": "gte", "value": "30"}]})),
        10
    );
    assert_eq!(
        count(
            json!({"filters": [{"column_id": id("id"), "op": "between", "value": "5", "high": "9"}]})
        ),
        5
    );
    // Exact u64 comparison, beyond what a float can hold.
    assert_eq!(
        count(
            json!({"filters": [{"column_id": id("big"), "op": "eq", "value": "18446744073709551614"}]})
        ),
        1
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("group"), "op": "is_null"}]})),
        10
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("group"), "op": "contains", "value": "\"x"}]})),
        10
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("category"), "op": "eq", "value": "a"}]})),
        10
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("flag"), "op": "is_true"}]})),
        16
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("score"), "op": "is_nan"}]})),
        1
    );
    // 9.75 and infinity; NaN fails ordering comparisons, missing values fail every one.
    assert_eq!(
        count(json!({"filters": [{"column_id": id("score"), "op": "gt", "value": "9.5"}]})),
        2
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("score"), "op": "lt", "value": "100"}]})),
        37
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("score"), "op": "ne", "value": "0"}]})),
        38
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("day"), "op": "lt", "value": "2024-01-11"}]})),
        10
    );
    assert_eq!(
        count(json!({"filters": [{"column_id": id("price"), "op": "gte", "value": "38.38"}]})),
        2
    );
    // A wall time in the column's zone: 06:00 in Costa Rica is 12:00 UTC, the first row.
    assert_eq!(
        count(
            json!({"filters": [{"column_id": id("at"), "op": "lte", "value": "2024-01-01 07:00:00"}]})
        ),
        2
    );
    // Filters combine with and.
    assert_eq!(
        count(json!({"filters": [
            {"column_id": id("id"), "op": "lt", "value": "20"},
            {"column_id": id("group"), "op": "eq", "value": "a"},
        ]})),
        5
    );
}

#[test]
fn filters_reject_wrong_values_and_types() {
    let fixture = mixed();
    let table = open(&fixture);
    let id = |name| id_of(&table, name);
    let invalid = |value: Value| matches!(table.count(&view(value)), Err(AtlasError::Invalid(_)));
    assert!(invalid(
        json!({"filters": [{"column_id": id("id"), "op": "eq", "value": "1.5"}]})
    ));
    assert!(invalid(
        json!({"filters": [{"column_id": id("big"), "op": "eq", "value": "-1"}]})
    ));
    assert!(invalid(
        json!({"filters": [{"column_id": id("id"), "op": "contains", "value": "1"}]})
    ));
    assert!(invalid(
        json!({"filters": [{"column_id": id("score"), "op": "gt", "value": "inf"}]})
    ));
    assert!(invalid(
        json!({"filters": [{"column_id": id("items"), "op": "eq", "value": "1"}]})
    ));
    assert!(invalid(
        json!({"filters": [{"column_id": id("id"), "op": "eq"}]})
    ));
    assert!(invalid(
        json!({"filters": [{"column_id": id("flag"), "op": "eq", "value": "yes"}]})
    ));
}

#[test]
fn sorting_is_stable_with_missing_values_last() {
    let fixture = mixed();
    let table = open(&fixture);
    let tie = id_of(&table, "tie");
    let group = id_of(&table, "group");
    let sorted = view(json!({"sort": [{"column_id": tie, "descending": true}]}));
    let ids = column_values(&table, &sorted, "id");
    // Ties keep file order.
    let expected: Vec<Option<String>> = [2, 1, 0]
        .iter()
        .flat_map(|key| {
            (0..40)
                .filter(move |i| i % 3 == *key)
                .map(|i| Some(i.to_string()))
        })
        .collect();
    assert_eq!(ids, expected);

    let by_group = view(json!({"sort": [{"column_id": group, "descending": false}]}));
    let groups = column_values(&table, &by_group, "group");
    assert!(groups[30..].iter().all(|value| value.is_none()));
    assert_eq!(groups[0].as_deref(), Some("a"));
    assert_eq!(table.count(&by_group).unwrap(), 40);
}

#[test]
fn filtered_and_sorted_windows_match_polars() {
    let fixture = mixed();
    let table = open(&fixture);
    let spec = view(json!({
        "filters": [{"column_id": id_of(&table, "id"), "op": "gte", "value": "5"}],
        "sort": [{"column_id": id_of(&table, "tie"), "descending": false}, {"column_id": id_of(&table, "id"), "descending": true}],
    }));
    let ours = column_values(&table, &spec, "id");
    let reference =
        LazyFrame::scan_parquet(PlRefPath::from(fixture.path.as_str()), Default::default())
            .unwrap()
            .filter(col("id").gt_eq(lit(5i64)))
            .sort_by_exprs(
                [col("tie"), col("id")],
                SortMultipleOptions::default().with_order_descending_multi([false, true]),
            )
            .select([col("id")])
            .collect()
            .unwrap();
    let expected: Vec<Option<String>> = reference
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .iter()
        .map(|value| value.map(|v| v.to_string()))
        .collect();
    assert_eq!(ours, expected);
    // The second pass reads the cached order and returns the same rows.
    assert_eq!(column_values(&table, &spec, "id"), expected);
}

#[test]
fn numeric_summary_counts_add_up() {
    let fixture = mixed();
    let table = open(&fixture);
    let score = id_of(&table, "score");
    let summary = &table.summaries(&ViewSpec::default(), &[score], 8).unwrap()[0];
    assert_eq!(summary["count"], 40);
    assert_eq!(summary["missing"], 1);
    assert_eq!(summary["metrics"]["nan"], 1);
    assert_eq!(summary["metrics"]["infinite"], 1);
    assert_eq!(summary["metrics"]["finite"], 37);
    assert_eq!(summary["metrics"]["min"], "0.0");
    assert_eq!(summary["metrics"]["max"], "9.75");
    let histogram = summary["histogram"].as_array().unwrap();
    assert_eq!(histogram.len(), 8);
    let total: u64 = histogram
        .iter()
        .map(|bin| bin["count"].as_u64().unwrap())
        .sum();
    assert_eq!(total, 37);
    // Sample standard deviation of the finite values.
    let finite: Vec<f64> = (0..40)
        .filter(|i| ![3, 5, 7].contains(i))
        .map(|i| i as f64 / 4.0)
        .collect();
    let mean = finite.iter().sum::<f64>() / finite.len() as f64;
    let std =
        (finite.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (finite.len() - 1) as f64).sqrt();
    assert!((summary["metrics"]["std"].as_f64().unwrap() - std).abs() < 1e-9);
    assert!((summary["metrics"]["mean"].as_f64().unwrap() - mean).abs() < 1e-9);
}

#[test]
fn small_integer_ranges_get_one_bin_per_value() {
    let fixture = mixed();
    let table = open(&fixture);
    let tie = id_of(&table, "tie");
    let summary = &table.summaries(&ViewSpec::default(), &[tie], 24).unwrap()[0];
    let counts: Vec<u64> = summary["histogram"]
        .as_array()
        .unwrap()
        .iter()
        .map(|bin| bin["count"].as_u64().unwrap())
        .collect();
    assert_eq!(counts, vec![14, 13, 13]);
}

#[test]
fn text_summaries_are_exact() {
    let fixture = mixed();
    let table = open(&fixture);
    let ids = [
        id_of(&table, "group"),
        id_of(&table, "category"),
        id_of(&table, "flag"),
    ];
    let filtered =
        view(json!({"filters": [{"column_id": id_of(&table, "id"), "op": "lt", "value": "20"}]}));
    let summaries = table.summaries(&filtered, &ids, 24).unwrap();
    for summary in &summaries[..2] {
        assert_eq!(summary["count"], 20);
        assert_eq!(summary["missing"], 5);
        assert_eq!(summary["distinct"], 3);
        assert_eq!(summary["distinct_exact"], true);
        let top = summary["top_values"].as_array().unwrap();
        assert_eq!(top.len(), 3);
        assert!(top.iter().all(|entry| entry["count"] == 5));
        assert_eq!(summary["other_count"], 0);
    }
    let flag = &summaries[2];
    assert_eq!(flag["metrics"]["true"], 8);
    assert_eq!(flag["metrics"]["false"], 8);
    assert_eq!(flag["missing"], 4);
}

#[test]
fn temporal_and_nested_summaries() {
    let fixture = mixed();
    let table = open(&fixture);
    let ids = [
        id_of(&table, "day"),
        id_of(&table, "at"),
        id_of(&table, "items"),
        id_of(&table, "price"),
    ];
    let summaries = table.summaries(&ViewSpec::default(), &ids, 4).unwrap();
    let day = &summaries[0];
    assert_eq!(day["metrics"]["min"], "2024-01-01");
    assert_eq!(day["metrics"]["max"], "2024-02-09");
    let bins = day["histogram"].as_array().unwrap();
    assert_eq!(
        bins.iter()
            .map(|bin| bin["count"].as_u64().unwrap())
            .sum::<u64>(),
        40
    );
    assert_eq!(bins[0]["start_label"], "2024-01-01");
    assert!(summaries[1]["metrics"]["min"]
        .as_str()
        .unwrap()
        .contains("2024-01-01"));
    assert_eq!(summaries[2]["missing"], 0);
    assert_eq!(summaries[2]["kind"], "nested");
    assert_eq!(summaries[3]["metrics"]["max"], "39.39");
}

#[test]
fn errors_do_not_reveal_the_path() {
    let error = Table::open("/no/such/dir/data.parquet", 1024)
        .err()
        .unwrap();
    assert!(!error.message().contains("/no/such/dir"));
    let fixture = mixed();
    std::fs::write(&fixture.path, b"not parquet").unwrap();
    let error = Table::open(&fixture.path, 1024).err().unwrap();
    assert!(
        !error.message().contains(&fixture.path),
        "{}",
        error.message()
    );
}

#[test]
fn the_order_cache_respects_its_budget() {
    let fixture = mixed();
    // Room for no order at all: every window recomputes it and stays correct.
    let table = Table::open(&fixture.path, 8).unwrap();
    let spec = view(json!({"sort": [{"column_id": 0, "descending": true}]}));
    let ids = column_values(&table, &spec, "id");
    assert_eq!(
        ids,
        (0..40)
            .rev()
            .map(|i| Some(i.to_string()))
            .collect::<Vec<_>>()
    );
}
