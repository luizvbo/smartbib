from sqlalchemy import (
    String, Integer, Boolean, Column, ForeignKey, DateTime, Table
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import relationship
from typing import List

Base = declarative_base()

class PaperReference(Base):
    __tablename__ = 'paper_reference'

    paper_citer = Column(String, ForeignKey('papers.id_'), primary_key=True)
    paper_cited = Column(String, ForeignKey('papers.id_'), primary_key=True)

class PaperFOS(Base):
    __tablename__ = 'paper_field_of_study'

    paper = Column(String, ForeignKey('papers.id_'), primary_key=True)
    fos = Column(Integer, ForeignKey('fields_of_study.id_'), primary_key=True)

class PaperIntent(Base):
    __tablename__ = 'paper_intent'

    paper = Column(String, ForeignKey('papers.id_'), primary_key=True)
    intent = Column(Integer, ForeignKey('intents.id_'), primary_key=True)

class PaperAuthor(Base):
    __tablename__ = 'paper_author'

    paper = Column(String, ForeignKey('papers.id_'), primary_key=True)
    author = Column(String, ForeignKey('authors.id_'), primary_key=True)

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
    references = relationship(
        'Paper', secondary='paper_reference',
        primaryjoin=PaperReference.paper_citer==id_,
        secondaryjoin=PaperReference.paper_cited==id_,
        backref='citer'
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
        'Paper', secondary='paper_field_of_study',
        back_populates='fields_of_study'
    )

class Intent(Base):
    __tablename__ = 'intents'

    id_ = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(String, unique=True)
    papers = relationship(
        'Paper', secondary='paper_intent', back_populates='intents'
    )

#  def get_tags(
#      session: Session, obj_type: DeclarativeMeta, text_list: List[str]
#  ):
#      tags = (
#          session.query(obj_type)
#          .filter(obj_type.text.in_(text_list))
#          .all()
#      )
#      text_found = {item.text for item in tags}
#      for text in text_list:
#          if text not in text_found:
#              new_tag = obj_type(text=text)
#              session.add(new_tag)
#              tags.append(new_tag)
#      return tags
#
#  def insert_paper(
#      session: Session, full_paper: FullPaper, retreived_at: datetime,
#      citations_retrieved_at: Optional[datetime] = None,
#      cited_paper: Optional[str] = None
#  ):
#
#      paper = (
#          session.query(Paper)
#          .filter(Paper.id_ == full_paper.paper_id)
#          .one_or_none()
#      )
#      if paper is not None:
#
#
#      add_or_update_paper(
#          Paper(
#              id_ = full_paper.paper_id,
#              is_influential = getattr(full_paper, 'is_influential', None),
#              url = full_paper.url,
#              title = full_paper.title,
#              abstract = full_paper.abstract,
#              venue = full_paper.venue,
#              year = full_paper.year,
#              reference_count = full_paper.reference_count,
#              citation_count = full_paper.citation_count,
#              influential_citation_count = full_paper.influential_citation_count,
#              is_open_access = full_paper.is_open_access,
#              retrieved_at = retreived_at,
#              citations_retrieved_at = citations_retrieved_at
#          ), session
#      )
#
#      for source, ext_id in full_paper.external_ids.items():
#          session.add(
#              ExternalId(
#                  id_paper=full_paper.paper_id, source=source, ext_id=ext_id
#              )
#          )
#
#      for text in getattr(full_paper, 'contexts', []):
#          session.add(
#              Context(id_paper=full_paper.paper_id, text=text)
#          )
#      if hasattr(full_paper, 'intents'):
#          intent_text = {
#              intent.text for intent in
#              session.query(Intent)
#              .filter(Intent.text.in_(full_paper.intents)).all()
#          }
#          for intent in full_paper.intents:
#              if intent not in intents_text:
#                  session.add(Intent(text=intent))
#          intents = (
#              session.query(Intent)
#              .filter(Intent.text.in_(full_paper.intents)).all()
#          )
#          for intent in intents:
#              session.add(
#                  PaperIntent(
#                      id_paper=full_paper.paper_id, id_intent=intent.id_
#                  )
#              )
#
#      fos_text = {
#          field.text for field in
#          session.query(FieldOfStudy)
#          .filter(FieldOfStudy.text.in_(full_paper.fields_of_study)).all()
#      }
#      for field in full_paper.fields_of_study:
#          if field not in fos_text:
#              session.add(FieldOfStudy(text=field))
#      fos = (
#         session.query(FieldOfStudy)
#         .filter(FieldOfStudy.text.in_(full_paper.fields_of_study)).all()
#      )
#      for field in fos:
#          print('FOS', field.id_)
#          session_add(session,
#              PaperFOS(
#                  id_paper=full_paper.paper_id, id_fos=field.id_
#              )
#          )
#
#      authors = (
#          session.query(Author)
#          .filter(Author.id_.in_([
#              author['authorId'] for author in full_paper.authors
#          ])).all()
#      )
#      author_ids = {author.id_ for author in authors}
#      for author in full_paper.authors:
#          if author['authorId'] not in author_ids:
#              new_author = Author(id_=author['authorId'], name=author['name'])
#              session.add(new_author)
#              authors.append(new_author)
#      for author in authors:
#          session.add(
#              PaperAuthor(
#                  id_paper=full_paper.paper_id, id_author=author.id_
#              )
#          )
#      if cited_paper:
#          session.add(
#              PaperReferences(
#                  id_citer=full_paper.paper_id, id_cited=cited_paper
#              )
#          )
#      session.commit()

def create_tables(db_engine):
    Base.metadata.create_all(db_engine)
