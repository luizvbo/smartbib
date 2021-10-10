# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# + tags=[]
# %load_ext autoreload
# %autoreload 2

# + tags=[]
# # !pip uninstall -y s2cholar
# # !pip install -e /tmp/s2cholar/
# # !pip install -e ../../s2cholar/

# + tags=[]
import os
import sys
PATH_S2 = os.path.abspath('../../s2cholar')
PATH_TR = os.path.abspath('..')
for path in (PATH_S2, PATH_TR):
    if path not in sys.path:
        sys.path.append(path)

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from top_research.model import (
    create_tables, Paper, 
    ExternalId, Author, FieldOfStudy, Intent, 
    PaperAuthor, PaperIntent, Context,
#     PaperFOS,
)
import s2cholar as s2
from typing import Optional

# + tags=[]
# !rm ../data/data.db

abs_path = os.path.abspath('../data/data.db')
engine = create_engine(f'sqlite:///{abs_path}', echo=True)

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()
if not os.path.exists(abs_path):
    create_tables(engine)

# + tags=[]
paper_1 = Paper(
    id_ = 'abc',
    is_influential = True,
    url = 'www.google.com',
    title = 'Google',
    abstract = 'Search Engine',
    venue = 'SV',
    year = 2000,
    reference_count = 1,
    citation_count = 2,
    influential_citation_count = 0,
    is_open_access = False,
    retrieved_at = datetime.now(),
    citations_retrieved_at = None
)

paper_2 = Paper(
    id_ = 'def',
    is_influential = True,
    url = 'www.uol.com',
    title = 'Uol',
    abstract = 'Univ. Online',
    venue = 'BR',
    year = 1995,
    reference_count = 10,
    citation_count = 3,
    influential_citation_count = 1,
    is_open_access = True,
    retrieved_at = datetime.now(),
    citations_retrieved_at = None
)
# session.add(paper)

# + tags=[]
paper_1.references = [paper_2]
# -



# + tags=[]
# paper.fields_of_study = [FieldOfStudy(text='ML'), FieldOfStudy(text='NLP')]

# + tags=[]
session.add(paper_1)
session.add(paper_2)
session.commit()

# + tags=[]
intents = [Intent(text='a'), Intent(text='b')]

# + tags=[]
session.query()


# + tags=[]
def add_or_update_paper(paper: Paper, session):
    paper_query = (
        session.query(Paper)
        .filter(Paper.id_ == paper.id_)
    )
    if paper_query.one_or_none():
        paper_query.update({
            attr: getattr(paper, attr, None)
            for attr in (
                'citation_count', 'citations_retrieved_at',
                'influential_citation_count', 'is_influential',
                'is_open_access', 'reference_count','retrieved_at'
            )
        })
    else:
        session.add(paper)
        



# + tags=[]
insert_paper(res_paper[0], retreived_at=datetime.now())

# + tags=[]
res_citers = s2_api.get_paper_citations(seed_paper_id, limit=1000)

# + tags=[]
res_citers[0].next

# + tags=[]
res_citers1 = s2_api.get_paper_citations(seed_paper_id, offset=0, limit=1000)
# res_citers2 = s2_api.get_paper_citations(seed_paper_id, offset=1000, limit=1000)

# + tags=[]
res_citers1[0].next, res_citers2[0].next


# -

def get_citations(paper_id):
    offset = 0
    res_citers = s2_api.get_paper_citations(seed_paper_id, limit=1000)
    if res_citers[1] == 200:



# + tags=[]
client = s2.ApiClient()
s2_api = s2.PaperApi(client)

#BERT seed_paper_id = 'df2b0e26d0599ce3e70df8a9da02e51594e0e992'
seed_paper_id = 'dbf88163f980441126bcae60b1abef6564414262'


res_paper = s2_api.get_paper(seed_paper_id)
if res_paper[1] == 200:
    insert_paper(
        res_paper[0], retreived_at=datetime.now()
    )
    res_citers = s2_api.get_paper_citations(seed_paper_id, limit=1000)


# + tags=[]



# + tags=[]

    res_paper[0]

# + tags=[]

# engine = create_engine('sqlite:///data/data.db')

# Base = declarative_base()

# t_paper = Table(
#     'paper', base.metadata,
#     Column('id', String, primary_key=True),
#     Column('is_influential', Boolean),
#     Column('url', String),
#     Column('title', String),
#     Column('abstract', String),
#     Column('venue', String),
#     Column('year', Integer),
#     Column('reference_count', Integer),
#     Column('citation_count', Integer),
#     Column('influential_citation_count', Integer),
#     Column('is_open_access', Boolean)
# )

# t_external_id = Table(
#     'external_id', base.metadata,
#     Column('id_paper', String, ForeignKey('paper.id')),
#     Column('source', String),
#     Column('ext_id', String)
# )

# t_intent = Table(
#     'intent', base.metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('text', String)
# )

# t_paper_intent = Table(
#     'paper_intent', base.metadata,
#     Column('id_paper', String, ForeignKey('paper.id')),
#     Column('id_intent', String, ForeignKey('intent.id')),
# )

# t_field_of_study = Table(
#     'field_of_study', base.metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('text', String)
# )

# t_paper_fos = Table(
#     'paper_field_of_study', base.metadata,
#     Column('id_paper', String, ForeignKey('paper.id')),
#     Column('id_fos', String, ForeignKey('field_of_study.id'))
# )

# t_context = Table(
#     'context', base.metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True),
#     Column('id_paper', String, ForeignKey('paper.id')),
#     Column('text', String),
# )

# t_author = Table(
#     'author', base.metadata,
#     Column('id', String, primary_key=True),
#     Column('name', String)
# )

# t_paper_fos = Table(
#     'paper_author', base.metadata,
#     Column('id_paper', String, ForeignKey('paper.id')),
#     Column('id_author', String, ForeignKey('author.id'))
# )

# t_references = Table(
#     'paper_reference', base.metadata,
#     Column('id_citer', String, ForeignKey('paper.id')),
#     Column('id_cited', String, ForeignKey('paper.id')),
# )

# base.create_all(engine)
