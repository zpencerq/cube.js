use crate::metastore::MetaStore;
use crate::remotefs::RemoteFs;
use crate::CubeError;
use arrow::array::ArrayRef;
use datafusion::cube_ext::util::cmp_array_row_same_types;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::ExecutionPlan;
use futures::{future, StreamExt};
use itertools::Itertools;
use std::cmp::Ordering;
use std::pin::Pin;

pub async fn validate_table(
    schema: &str,
    table: &str,
    m: &dyn MetaStore,
    fs: &dyn RemoteFs,
) -> Result<(), CubeError> {
    let table = m.get_table(schema.to_string(), table.to_string()).await?;
    let indexes = m.get_table_indexes(table.get_id()).await?;
    let index_ids = indexes.iter().map(|i| i.get_id()).collect_vec();
    let partitions = m
        .get_active_partitions_and_chunks_by_index_id_for_select(index_ids)
        .await?;

    for i in 0..indexes.len() {
        let ind = indexes[i].get_row();
        let ind_id = indexes[i].get_id();
        log::info!("Validating index {}: {}", ind_id, ind.get_name());
        for (p, cs) in &partitions[i] {
            if let Some(file) = p.get_row().get_full_name(p.get_id()) {
                let file = fs.local_file(&file).await?;
                validate_chunk(&file, ind.sort_key_size() as usize).await?;
            }
            for c in cs.as_slice() {
                let file = c.get_row().get_full_name(c.get_id());
                let file = fs.local_file(&file).await?;
                validate_chunk(&file, ind.sort_key_size() as usize).await?;
            }
        }
        log::info!("Index ok");
    }

    Ok(())
}

pub async fn validate_chunk(file: &str, key_len: usize) -> Result<(), CubeError> {
    // TODO: validate metadata matches what's inside the file.
    // Validate sort order inside each file.
    log::info!("Validating file {}", file);
    let p = ParquetExec::try_from_files(&[file], None, None, 4096, 1, None)?;
    debug_assert_eq!(p.output_partitioning().partition_count(), 1);
    let mut p = p
        .execute(0)
        .await?
        .filter(|r| future::ready(matches!(r, Ok(r) if r.num_rows() != 0)))
        .peekable();
    while let Some(b) = p.next().await {
        let b = b?;
        let key_cols = &b.columns()[..key_len as usize];
        for i in 1..b.num_rows() {
            if lexcmp_rows(key_cols, i, key_cols, i - 1) < Ordering::Equal {
                log::error!(
                    "Unsorted data at row {}: {} and {}",
                    i - 1,
                    display_row(key_cols, i - 1),
                    display_row(key_cols, i)
                );
                return Err(CubeError::internal(
                    "unsorted data in partition".to_string(),
                ));
            }
        }

        if let Some(Ok(n)) = Pin::new(&mut p).peek().await {
            let b_row = b.num_rows() - 1;
            let n_cols = &n.columns()[..key_len as usize];
            if lexcmp_rows(n_cols, 0, key_cols, b_row) < Ordering::Equal {
                log::error!(
                    "Unsorted data between batches: {} and {}",
                    display_row(key_cols, b_row),
                    display_row(n_cols, 0)
                );
                return Err(CubeError::internal(
                    "unsorted data in partition".to_string(),
                ));
            }
        }
    }

    log::info!("File ok");
    Ok(())
}

fn display_row(cols: &[ArrayRef], row: usize) -> String {
    format!("{:?}", cols.iter().map(|c| c.slice(row, 1)).collect_vec())
}

fn lexcmp_rows(l: &[ArrayRef], l_row: usize, r: &[ArrayRef], r_row: usize) -> Ordering {
    debug_assert_eq!(l.len(), r.len());
    for i in 0..l.len() {
        let o = cmp_array_row_same_types(&l[i], l_row, &r[i], r_row);
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}
