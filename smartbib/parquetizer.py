import fire
from glob import glob
from loguru import logger
import pandas as pd
import numpy as np
import gzip
import os
import pickle
from typing import Optional
from tqdm.auto import tqdm
from smartbib.utils import id_str_to_bytes
import pyarrow as pa
from multiprocessing import Pool, cpu_count


PARQUET_SCHEMA = pa.schema([
    ('title', pa.string()),
    ('paperAbstract', pa.string()),
    (
        'authors', pa.list_(
            pa.struct([
                ('id_author', pa.int32()),
                ('name', pa.string())
            ])
        )
    ),
    ('inCitations', pa.list_(pa.binary())),
    ('year', pa.int16()),
    ('s2Url', pa.string()),
    ('venue', pa.string()),
    ('fieldsOfStudy', pa.list_(pa.string())),
    ('id_', pa.binary())
])

def _read_json_gzip(path_file):
    with gzip.open(path_file, 'r') as file:
        df_data = (
            pd.read_json(file, lines=True)
            # Drop deprecated fields (see api.semanticscholar.org/corpus)
            # and other unused columns
            .drop(
                [
                    'entities', 's2PdfUrl', 'doi', 'doiUrl',
                    'journalVolume', 'journalPages', 'pmid', 'magId',
                    'sources', 'pdfUrls', 'outCitations'
                ],
                axis=1
            )
            .assign(
                # Convert the id to bytes
                id_=lambda df: df['id'].apply(id_str_to_bytes),
                # Convert hash IDs into integers
                inCitations=lambda df: df.inCitations.apply(
                    lambda el: [id_str_to_bytes(id_str) for id_str in el]
                ),
                # Get use the journalName when available, otherwise, use the
                # value from venue
                venue=lambda df: df['journalName'].where(
                    df['journalName'] != '', df['venue']
                ),
                # Convert the year to int16
                year=lambda df: df.year.fillna(-1).astype(np.int16),
                # Convert the authors
                authors=lambda df: df.authors.apply(
                    lambda list_authors: [
                        (
                            int(aut['ids'][0]) if len(aut['ids']) == 1 else -1,
                            aut['name']
                        )
                        for aut in list_authors
                    ]
                )
            )
            .drop(['id', 'journalName'], axis=1)
            # Use the id column as index
            .set_index(['id_'])
        )
        logger.debug(f"File loaded: '{path_file}'")
    return df_data


def store_parquet(df, path_file, output_folder=None):
    if output_folder is None:
        output_folder = os.path.dirname(path_file)
    path_file = path_file[:-3] if path_file.endswith('.gz') else path_file
    path_file = os.path.basename(path_file)
    path_parquet = os.path.join(output_folder, path_file + '.parquet')
    df.to_parquet(path_parquet, engine='pyarrow', schema=PARQUET_SCHEMA)
    logger.debug(f"File stored as parquet: '{path_parquet}'")


def generate_parquet_files(
    input_path_pattern: str,
    output_folder: Optional[str] = None,
    n_jobs: int = 1,
):
    """Process Semantic Scholar files in a folder.

    This function decompress (in-memory) gzip files downloaded from [Semantic
    Scholar Open Research Corpus](https://api.semanticscholar.org/corpus) and
    store then as parquet files, so that they can be read using libraries like
    [dask](https://dask.org/).

    Assuming that files from 01/10/2021 were downloaded using the command:

    ```bash
        aws s3 cp --no-sign-request --recursive \
        s3://ai2-s2-research-public/open-corpus/2021-10-01/ destinationPath
    ```

    the folder `destinationPath` is populated with files in files named
    `s2-corpus-ID_NUMBER.gz`, which can then be processed using

    ```bash
        python -m recsearch.parquetizer destinationPath/s2-corpus*.gz
    ```

    Args:
        input_path_pattern: Pattern to the files. It uses glob-like patterns.
        output_folder (optional): In case you want to store the parquet files
            in a different location.
        n_jobs: Number of jobs to run in parallel.
    """
    file_list = glob(input_path_pattern)
    logger.debug(
        f"Loading files from '{input_path_pattern}'. "
        f"{len(file_list)} files found"
    )
    if n_jobs != 1:
        assert n_jobs > 0 or n_jobs == -1, 'Inconsistent n_jobs'
        n_jobs = cpu_count() if n_jobs == -1 else n_jobs
        with Pool(n_jobs) as p:
            p.map(
                _process_file,
                zip(file_list, [output_folder] * len(file_list))
            )
    else:
        for file_path in tqdm(file_list):
            df = _read_json_gzip(file_path)
            store_parquet(df, file_path, output_folder)


if __name__ == "__main__":
    fire.Fire(generate_parquet_files)
