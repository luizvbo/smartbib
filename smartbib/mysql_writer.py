import pandas as pd
import numpy as np
from smartbib.model import PaperDatabase
from loguru import logger
import fire

CHUNK_SIZE = 5_000
# TODO: Move this to utils
def chunks(lst):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), CHUNK_SIZE):
        yield lst[i:i + CHUNK_SIZE]


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
            ['authors', 'inCitations', 'outCitations', 'sources',
             'pdfUrls', 'pmid', 'fieldsOfStudy', 'magId'], axis=1
        )
        .rename(
            columns=dict(
                id='id_paper', paperAbstract='paper_abstract', s2Url='s2_url',
                journalName='journal_name', journalVolume='journal_volume',
                journalPages='journal_pages', doiUrl='doi_url'
            )
        )
        .to_dict(orient='records')
    )

def _get_papers_for_update_or_insert(db, conn, df):
    sel = db.select_where_in(
        db.paper, 'id_paper', df.index, selected_columns=['id_paper']
    )
    res = conn.execute(sel)
    ids_in_db = set([row[0] for row in res.fetchall()])

    return {
        'update': df[df.index.isin(ids_in_db)],
        'insert': df[~df.index.isin(ids_in_db)]
    }

def _insert_papers(db, conn, df):
    dict_df = _get_papers_for_update_or_insert(db, conn, df)
    logger.debug(
        "{up} papers to update and {ins} papers to insert".format(
            up=dict_df['update'].shape[0], ins=dict_df['insert'].shape[0]
        )
    )
    # Update papers
    records = _papers_to_records(dict_df['update'])
    res = []
    for record in records:
        update_clause = db.update_where_equal(
            db.paper, 'id_paper', record['id_paper'], record
        )
        res.append(conn.execute(update_clause))
    # Insert papers
    insert_clause = db.insert(db.paper)
    records = _papers_to_records(dict_df['insert'])
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("Papers up/inserted")

def _get_papers_db_id(db, conn, df):
    sel = db.select_where_in(
        db.paper, 'id_paper', df.index, selected_columns=['id', 'id_paper']
    )
    res = conn.execute(sel)

    return pd.DataFrame(res, columns=['id_db', 'id']).set_index('id')

def _insert_citations(db, conn, df):
    s_id_in_citations = df.inCitations.explode().dropna()
    logger.debug(f"{s_id_in_citations.shape[0]} citations in the dataframe")
    # Add all paper IDs referred in citations
    insert_clause = db.insert(db.paper, ignore_dup=True)
    records = [
        {'id_paper': value} for value in s_id_in_citations.values
    ]
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("Paper IDs from citations inserted")
    # Get the paper IDs (from the DB) for all papers referred
    paper_ids = np.unique(
        np.concatenate((s_id_in_citations.values, df.index.values))
    )
    res = []
    for chunk in chunks(paper_ids):
        select_clause = db.select_where_in(
            db.paper, 'id_paper',
            # Get all papers from citations and from the dataframe
            chunk,
            selected_columns=['id', 'id_paper']
        )
        res.extend(conn.execute(select_clause).fetchall())

    #  import pickle
    #  pickle.dump(res, open('/tmp/debug.pickle', 'wb'))

    df_db_papers = pd.DataFrame(res, columns=['id_db', 'id']).set_index('id')
    # Update the dataframe with the DB IDs
    df = (
        df.merge(df_db_papers, left_index=True, right_index=True, how='inner')
        .reset_index(drop=False).set_index('id_db')
    )
    # Convert the dataframe into records
    records = (
        df.inCitations.explode().dropna().to_frame()
        .merge(
            df_db_papers, right_index=True, left_on='inCitations'
        )
        .rename(columns={'id_db': 'id_citer'})
        .drop('inCitations', axis=1)
        .reset_index(drop=False)
        .rename(columns={'id_db': 'id_cited'})
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
        .rename(columns={'id_db': 'id_paper', 'fieldsOfStudy': 'content'})
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
        df.pdfUrls.explode().dropna().reset_index(drop=False)
        .rename(columns={'id_db': 'id_paper', 'pdfUrls': 'content'})
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
        df.authors.explode().dropna().apply(
            lambda d: (int(d['ids'][0]) if len(d['ids'])==1 else -1, d['name'])
        )
        .pipe(
            lambda s: pd.DataFrame(
                s.tolist(), index=s.index, columns=['id_author', 'name']
            )
        )
    )
    # Insert the data
    insert_clause = db.insert(db.author, ignore_dup=True)
    records = df_authors.to_dict(orient='records')
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("Authors inserted")

    # Get the author IDs already in the DB
    select_clause = db.select_where_in(
        db.author, 'id_author',
        # Get all papers from citations and from the dataframe
        df_authors.id_author.values,
        selected_columns=['id', 'id_author']
    )
    res = conn.execute(select_clause)

    df_db_authors = (
        pd.DataFrame(res, columns=['id', 'id_author']).set_index('id_author')
    )
    # Generate the records
    records = (
        df_authors.merge(
            df_db_authors, right_index=True, left_on='id_author', how='left'
        )
        .reset_index(drop=False).drop(['id_author', 'name'], axis=1)
        .rename(columns={'id_db': 'id_paper', 'id': 'id_author'})
        .to_dict(orient='records')
    )
    # Insert the data
    insert_clause = db.insert(db.authorship)
    for chunk in chunks(records):
        conn.execute(insert_clause, chunk)
    logger.debug("Authorships inserted")


def write_s2_data_to_db(df, engine):
    db = PaperDatabase()
    db.create_tables(engine)
    with engine.connect() as conn:
        _insert_papers(db, conn, df)
        df = _insert_citations(db, conn, df)
        _insert_fos(db, conn, df)
        _insert_pdf_urls(db, conn, df)
        _insert_authors(db, conn, df)

def write_data_to_db(path_data: str, path_config: str, path_credentials: str):
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
    for path_parquet in glob(path_data):
        logger.debug(f"Loading parquet file from {path_parquet}")
        df = (
            pd.read_parquet(path_parquet)
            .assign(year=lambda df: df.year.fillna(-1))
        )
        write_s2_data_to_db(df, engine)

if __name__ == "__main__":
    fire.Fire(write_data_to_db)

