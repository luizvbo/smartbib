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
PATH_TR = os.path.abspath('..')
if PATH_TR not in sys.path:
    sys.path.append(PATH_TR)


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from top_research.model import (
    create_tables, Paper, ExternalId, Author, FieldOfStudy, Intent, 
    PaperAuthor, PaperFOS, PaperIntent, PaperReferences, Context
)
import s2cholar as s2
from typing import Optional

# + tags=[]
abs_path = os.path.abspath('../data/data.db')
engine = create_engine(f'sqlite:///{abs_path}', echo=True)

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()
if not os.path.exists(abs_path):
    create_tables(engine)

# + tags=[]
dir(Paper)

# + tags=[]
from datetime import datetime

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
        

def insert_paper(
    full_paper: s2.FullPaper, retreived_at: datetime, 
    citations_retrieved_at: Optional[datetime] = None,
    cited_paper: Optional[str] = None
):
    add_or_update_paper(
        Paper(
            id_ = full_paper.paper_id,
            is_influential = getattr(full_paper, 'is_influential', None),
            url = full_paper.url,
            title = full_paper.title,
            abstract = full_paper.abstract,
            venue = full_paper.venue,
            year = full_paper.year,
            reference_count = full_paper.reference_count,
            citation_count = full_paper.citation_count,
            influential_citation_count = full_paper.influential_citation_count,
            is_open_access = full_paper.is_open_access,
            retrieved_at = retreived_at,
            citations_retrieved_at = citations_retrieved_at
        ), session
    )
    
    for source, ext_id in full_paper.external_ids.items():
        session_add(session,
            ExternalId(
                id_paper=full_paper.paper_id, source=source, ext_id=ext_id
            )
        )

    for text in getattr(full_paper, 'contexts', []):
        session_add(session,
            Context(id_paper=full_paper.paper_id, text=text)
        )
    if hasattr(full_paper, 'intents'):
        intents = (
            session.query(Intent)
            .filter(Intent.text.in_(full_paper.intents)).all()
        )
        intents_text = {intent.text for intent in intents}
        for intent in full_paper.intents: 
            if intent not in intents_text:
                new_intent = Intent(text=intent)
                session_add(session,new_intent)
                intents.append(new_intent)
        for intent in intents:
            session_add(session,
                PaperIntent(
                    id_paper=full_paper.paper_id, id_intent=intent.id_
                )
            )
    
    fos = (
        session.query(FieldOfStudy)
        .filter(FieldOfStudy.text.in_(full_paper.fields_of_study))
        .all()
    )
    fos_text = {field.text for field in fos}
    for field in full_paper.fields_of_study:
        if field not in fos_text:
            new_fos = FieldOfStudy(text=field)
            session_add(session,new_fos)
            fos.append(new_fos)
    for field in fos:
        print('FOS', field.id_)
        session_add(session,
            PaperFOS(
                id_paper=full_paper.paper_id, id_fos=field.id_
            )
        )
    
    authors = (
        session.query(Author)
        .filter(Author.id_.in_([
            author['authorId'] for author in full_paper.authors
        ])).all()
    )
    author_ids = {author.id_ for author in authors}
    for author in full_paper.authors:
        if author['authorId'] not in author_ids:
            new_author = Author(id_=author['authorId'], name=author['name'])
            session_add(session,new_author)
            authors.append(new_author)
    for author in authors:
        session_add(session,
            PaperAuthor(
                id_paper=full_paper.paper_id, id_author=author.id_
            )
        )
    if cited_paper:
        session_add(session,
            PaperReferences(
                id_citer=full_paper.paper_id, id_cited=cited_paper
            )
        )
        
    session.commit()


# + tags=[]
insert_paper(res_paper[0], retreived_at=datetime.now())

# + tags=[]


# + tags=[]
seed_paper_id = 'df2b0e26d0599ce3e70df8a9da02e51594e0e992'

client = s2.ApiClient()
s2_api = s2.PaperApi(client)
res_paper = s2_api.get_paper(seed_paper_id)

full_paper = res_paper[0]

# + tags=[]
if res_paper[1] == 200:
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
