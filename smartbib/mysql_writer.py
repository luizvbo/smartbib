import pandas as pd
from smartbib.model import PaperDatabase
from loguru import logger
import fire
from tqdm.auto import tqdm
from smartbib.utils import chunks


def _papers_to_records(df):
    """Convert a dataframe containing papers to s2 into records.

    Take a pandas dataframe from data downloaded from Semantic Scholar and
    convert into a list of dictionaries containing the data to write to a mysql
    database.

    Args:
        df: Pandas dataframe from s2 data.

    Returns:
        List[Dict]: Containing the records.
    """
    return (
        df.reset_index().drop(
            ['authors', 'inCitations', 'sources', 'pdfUrls', 'fieldsOfStudy'],
            axis=1
        )
        .rename(
            columns=dict(
                id='id_paper', paperAbstract='paper_abstract', s2Url='s2_url',
                journalName='journal_name', journalVolume='journal_volume',
                journalPages='journal_pages'
            )
        )
        .to_dict(orient='records')
    )

def _insert_papers(db, conn, df):
    logger.debug(f"{df.shape[0]} papers to insert")
    # Insert papers
    insert_clause = db.insert(db.paper)
    records = _papers_to_records(df)
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("Papers up/inserted")

def _insert_citations(db, conn, df):
    df_citations = df.inCitations.explode().dropna().to_frame()
    logger.debug(f"{df_citations.shape[0]} citations in the dataframe")

    # Convert the dataframe into records
    records = (
        df_citations
        .reset_index()
        .set_axis(['id_cited', 'id_citer'], axis='columns')
        .to_dict(orient='records')
    )
    insert_clause = db.insert(db.citation)
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("Citations inserted")
    return df


def _insert_fos(db, conn, df):
    # Prepare the records
    records = (
        df.fieldsOfStudy.explode().dropna()
        .reset_index(drop=False)
        .set_axis(['id_paper', 'content'], axis='columns')
        .to_dict(orient='records')
    )
    # Insert the data
    insert_clause = db.insert(db.fos)
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("FoS inserted")

def _insert_pdf_urls(db, conn, df):
    # Prepare the records
    records = (
        df.pdfUrls.explode().dropna()
        .reset_index(drop=False)
        .set_axis(['id_paper', 'content'], axis='columns')
        .to_dict(orient='records')
    )
    # Insert the data
    insert_clause = db.insert(db.pdf_url)
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("PDF url's inserted")

def _insert_authors(db, conn, df):
    # Explode the authors column and convert the dictionary into columns
    df_authors = (
        df.authors.explode().dropna()
        .pipe(lambda s: pd.DataFrame(s.tolist(), index=s.index))
        .reset_index(drop=False)
        .set_axis(['id_paper', 'id_author', 'name'], axis='columns')
    )
    logger.debug(f"{df_authors.shape[0]} authors in the dataframe")
    # Insert the data
    insert_clause = db.insert(db.author, ignore_dup=True)
    records = df_authors.to_dict(orient='records')
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("Authors inserted")


def write_s2_data_to_db(df, engine):
    db = PaperDatabase()
    db.create_tables(engine)
    with engine.connect() as conn:
        _insert_papers(db, conn, df)
        df = _insert_citations(db, conn, df)
        _insert_fos(db, conn, df)
        _insert_pdf_urls(db, conn, df)
        _insert_authors(db, conn, df)


def write_data_to_db(
    path_data: str, path_config: str, path_credentials: str
):
    """Load dataframes from parquet files and write to database

    Args:
        path_data: Glob-like patter for input files.
        path_config: Path to the configuration file used to access the
            database
        path_credentials: Path to the credentials file used to access the
            database
    """
    import yaml
    from sqlalchemy import create_engine
    from glob import glob

    with open(path_config, 'r') as file:
        config = yaml.safe_load(file)['mysql']
    with open(path_credentials, 'r') as file:
        credentials = yaml.safe_load(file)['mysql']
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{server}/{db}?charset=utf8mb4".format(
            user=credentials['user'],
            pw=credentials['password'],
            server=config['server'],
            db=config['database']
        ), pool_timeout=300
    )
    file_list = glob(path_data)
    logger.debug(
        f"Loading files from '{path_data}'. {len(file_list)} files found"
    )
    for path_parquet in tqdm(file_list):
        logger.debug(f"Loading parquet file from {path_parquet}")
        df = (pd.read_parquet(path_parquet))
        write_s2_data_to_db(df, engine)


if __name__ == "__main__":
    fire.Fire(write_data_to_db)
