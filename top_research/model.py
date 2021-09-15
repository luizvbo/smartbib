from sqlalchemy import (
    String, Integer, Boolean, Column, ForeignKey, DateTime, Table
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class PaperReference(Base):
    __tablename__ = 'paper_reference'

    paper_citer = Column(String, ForeignKey('papers.id_'), primary_key=True)
    paper_cited = Column(String, ForeignKey('papers.id_'), primary_key=True)

class Paper(Base):
    __tablename__ = 'papers'

    id_ = Column(String, primary_key=True)
    is_influential = Column(Boolean)
    url = Column(String)
    title = Column(String)
    abstract = Column(String)
    venue = Column(String)
    year = Column(Integer)
    reference_count = Column(Integer)
    citation_count = Column(Integer)
    influential_citation_count = Column(Integer)
    is_open_access = Column(Boolean)
    retrieved_at = Column(DateTime)
    citations_retrieved_at = Column(DateTime)
    contexts = relationship('Context', back_populates='paper')
    external_ids = relationship('ExternalId', back_populates='paper')
    fields_of_study = relationship(
        'FieldOfStudy', secondary='paper_field_of_study',
        back_populates='papers'
    )
    intents = relationship(
        'Intent', secondary='paper_intent', back_populates='papers'
    )
    authors = relationship(
        'Author', secondary='paper_author', back_populates='papers'
    )
    reference = relationship(
        'Paper', secondary='paper_reference',
        primaryjoin=PaperReference.paper_citer==id_,
        secondaryjoin=PaperReference.paper_cited==id_,
        backref='citer'
    )
    paper_cited = relationship(
        'Paper', secondary='paper_reference', back_populates='papers'
    )
    paper_citer = relationship(
        'Paper', secondary='paper_reference', back_populates='papers'
    )

class Context(Base):
    __tablename__ = 'contexts'

    id_ = Column(Integer, primary_key=True, autoincrement=True)
    id_paper = Column(String, ForeignKey('papers.id_'))
    text = Column(String)
    paper = relationship('Paper', back_populates='contexts')

class ExternalId(Base):
    __tablename__ = 'external_ids'

    id_ = Column(Integer, primary_key=True, autoincrement=True)
    id_paper = Column(String, ForeignKey('papers.id_'))
    source = Column(String)
    ext_id = Column(String)
    paper = relationship('Paper', back_populates='external_ids')

class Author(Base):
    __tablename__ = 'authors'

    id_ = Column(String, primary_key=True)
    name = Column(String)
    papers = relationship(
        'Paper', secondary='paper_author', back_populates='authors'
    )

class FieldOfStudy(Base):
    __tablename__ = 'fields_of_study'

    id_ = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(String, unique=True)
    papers = relationship(
        'Paper', secondary='paper_field_of_study', back_populates='fields_of_study'
    )

class PaperFOS(Base):
    __tablename__ = 'paper_field_of_study'

    paper = Column(String, ForeignKey('papers.id_'), primary_key=True)
    fos = Column(Integer, ForeignKey('fields_of_study.id_'), primary_key=True)

class Intent(Base):
    __tablename__ = 'intents'

    id_ = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(String, unique=True)
    papers = relationship(
        'Paper', secondary='paper_intent', back_populates='intents'
    )

class PaperIntent(Base):
    __tablename__ = 'paper_intent'

    paper = Column(String, ForeignKey('papers.id_'), primary_key=True)
    intent = Column(Integer, ForeignKey('intents.id_'), primary_key=True)

class PaperAuthor(Base):
    __tablename__ = 'paper_author'

    paper = Column(String, ForeignKey('papers.id_'), primary_key=True)
    author = Column(String, ForeignKey('authors.id_'), primary_key=True)

def create_tables(db_engine):
    Base.metadata.create_all(db_engine)
