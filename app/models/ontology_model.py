"""SQLAlchemy ORM models for ontology domain."""
from datetime import datetime
from uuid import uuid4
from typing import List, Optional

from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    Boolean,
    ForeignKey,
    Text,
    JSON,
    UniqueConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Domain(Base):
    """CML Domain - Top-level categorization of ontologies."""

    __tablename__ = "cml_domain"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_domain_code", "domain_code"),
        Index("ix_domain_is_active", "is_active"),
    )

    domain_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    domain_code = Column(String(100), nullable=False, unique=True)
    domain_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    # Relationships
    ontology_versions: List["OntologyVersion"] = relationship(
        "OntologyVersion",
        back_populates="domain",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<Domain(domain_code='{self.domain_code}', domain_name='{self.domain_name}')>"


class OntologyVersion(Base):
    """CML Ontology Version - Snapshot of ontology at a point in time."""

    __tablename__ = "cml_ontology_version"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_ontology_version_code", "version_code"),
        Index("ix_ontology_status", "status"),
        Index("ix_domain_id", "domain_id"),
    )

    ontology_version_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    domain_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_domain.domain_id", ondelete="CASCADE"),
        nullable=False,
    )
    version_code = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False, default="draft")  # draft, active, deprecated
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        UniqueConstraint("domain_id", "version_code", name="uq_domain_version"),
        Index("ix_ontology_version_code", "version_code"),
        Index("ix_ontology_status", "status"),
        Index("ix_domain_id", "domain_id"),
    )

    # Relationships
    domain: Domain = relationship("Domain", back_populates="ontology_versions")
    concepts: List["Concept"] = relationship(
        "Concept",
        back_populates="ontology_version",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    change_logs: List["OntologyChangeLog"] = relationship(
        "OntologyChangeLog",
        back_populates="ontology_version",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<OntologyVersion(version_code='{self.version_code}', status='{self.status}')>"


class Concept(Base):
    """CML Concept - Real-world thing or idea that datasets describe."""

    __tablename__ = "cml_concept"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_concept_code", "concept_code"),
        Index("ix_ontology_version_id", "ontology_version_id"),
        Index("ix_parent_concept_id", "parent_concept_id"),
    )

    concept_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    ontology_version_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_ontology_version.ontology_version_id", ondelete="CASCADE"),
        nullable=False,
    )
    concept_code = Column(String(100), nullable=False)
    concept_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    parent_concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="SET NULL"),
        nullable=True,
    )
    is_abstract = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        UniqueConstraint("ontology_version_id", "concept_code", name="uq_version_concept"),
        Index("ix_concept_code", "concept_code"),
        Index("ix_ontology_version_id", "ontology_version_id"),
        Index("ix_parent_concept_id", "parent_concept_id"),
    )

    # Relationships
    ontology_version: OntologyVersion = relationship(
        "OntologyVersion", back_populates="concepts", foreign_keys=[ontology_version_id]
    )
    parent_concept: Optional["Concept"] = relationship(
        "Concept",
        remote_side=[concept_id],
        back_populates="child_concepts",
        foreign_keys=[parent_concept_id],
    )
    child_concepts: List["Concept"] = relationship(
        "Concept",
        back_populates="parent_concept",
        cascade="all, delete-orphan",
        foreign_keys=[parent_concept_id],
    )
    properties: List["Property"] = relationship(
        "Property",
        back_populates="concept",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    dataset_concepts: List["DatasetConcept"] = relationship(
        "DatasetConcept",
        back_populates="concept",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<Concept(concept_code='{self.concept_code}', concept_name='{self.concept_name}')>"


class Property(Base):
    """CML Property - Characteristic or attribute that describes a concept."""

    __tablename__ = "cml_property"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_property_code", "property_code"),
        Index("ix_concept_id", "concept_id"),
    )

    property_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="CASCADE"),
        nullable=False,
    )
    property_code = Column(String(100), nullable=False)
    property_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    data_type = Column(String(50), nullable=False)  # string, number, date, geometry, boolean, etc.
    is_required = Column(Boolean, nullable=False, default=False)
    is_multivalued = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        UniqueConstraint("concept_id", "property_code", name="uq_concept_property"),
        Index("ix_property_code", "property_code"),
        Index("ix_concept_id", "concept_id"),
    )

    # Relationships
    concept: Concept = relationship("Concept", back_populates="properties")
    constraints: List["PropertyConstraint"] = relationship(
        "PropertyConstraint",
        back_populates="property",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    dataset_field_mappings: List["DatasetFieldMapping"] = relationship(
        "DatasetFieldMapping",
        back_populates="property",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<Property(property_code='{self.property_code}', data_type='{self.data_type}')>"


class PropertyConstraint(Base):
    """CML Property Constraint - Rule that restricts or validates values of a property."""

    __tablename__ = "cml_property_constraint"
    __allow_unmapped__ = True
    __table_args__ = (Index("ix_property_id", "property_id"),)

    constraint_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    property_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_property.property_id", ondelete="CASCADE"),
        nullable=False,
    )
    constraint_type = Column(String(50), nullable=False)  # range, enum, regex, unit, geometry, vocab
    constraint_value = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (Index("ix_property_id", "property_id"),)

    # Relationships
    property: Property = relationship("Property", back_populates="constraints")

    def __repr__(self) -> str:
        return f"<PropertyConstraint(constraint_type='{self.constraint_type}')>"


class Vocabulary(Base):
    """CML Vocabulary - Controlled list of allowed values."""

    __tablename__ = "cml_vocab"
    __allow_unmapped__ = True
    __table_args__ = (Index("ix_vocab_code", "vocab_code"),)

    vocab_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    vocab_code = Column(String(100), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (Index("ix_vocab_code", "vocab_code"),)

    # Relationships
    terms: List["VocabularyTerm"] = relationship(
        "VocabularyTerm",
        back_populates="vocabulary",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        return f"<Vocabulary(vocab_code='{self.vocab_code}')>"


class VocabularyTerm(Base):
    """CML Vocabulary Term - Term within a controlled vocabulary."""

    __tablename__ = "cml_vocab_term"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_vocab_id", "vocab_id"),
        Index("ix_parent_term_id", "parent_term_id"),
    )

    term_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    vocab_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_vocab.vocab_id", ondelete="CASCADE"),
        nullable=False,
    )
    term_code = Column(String(100), nullable=False)
    term_label = Column(String(255), nullable=False)
    parent_term_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_vocab_term.term_id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        UniqueConstraint("vocab_id", "term_code", name="uq_vocab_term"),
        Index("ix_vocab_id", "vocab_id"),
        Index("ix_parent_term_id", "parent_term_id"),
    )

    # Relationships
    vocabulary: Vocabulary = relationship("Vocabulary", back_populates="terms")
    parent_term: Optional["VocabularyTerm"] = relationship(
        "VocabularyTerm",
        remote_side=[term_id],
        back_populates="child_terms",
        foreign_keys=[parent_term_id],
    )
    child_terms: List["VocabularyTerm"] = relationship(
        "VocabularyTerm",
        back_populates="parent_term",
        cascade="all, delete-orphan",
        foreign_keys=[parent_term_id],
    )

    def __repr__(self) -> str:
        return f"<VocabularyTerm(term_code='{self.term_code}', term_label='{self.term_label}')>"


class DatasetFieldMapping(Base):
    """CML Dataset Field Mapping - Maps dataset fields to ontology properties."""

    __tablename__ = "cml_dataset_field_mapping"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_dataset_id", "dataset_id"),
        Index("ix_property_id", "property_id"),
    )

    mapping_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    dataset_id = Column(Integer, nullable=False)  # References dataset_master.dataset_id
    concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="CASCADE"),
        nullable=False,
    )
    property_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_property.property_id", ondelete="CASCADE"),
        nullable=False,
    )
    external_field_name = Column(String(255), nullable=False)
    external_data_type = Column(String(50), nullable=True)
    transform_rule = Column(JSON, nullable=True)  # For concat, cast, unit conversion
    is_exposed = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        UniqueConstraint("dataset_id", "external_field_name", name="uq_dataset_field"),
        Index("ix_dataset_id", "dataset_id"),
        Index("ix_property_id", "property_id"),
    )

    # Relationships
    concept: Concept = relationship("Concept")
    property: Property = relationship("Property", back_populates="dataset_field_mappings")

    def __repr__(self) -> str:
        return f"<DatasetFieldMapping(external_field_name='{self.external_field_name}')>"


class DatasetConcept(Base):
    """CML Dataset Concept - Links datasets to ontology concepts."""

    __tablename__ = "cml_dataset_concept"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_dataset_id", "dataset_id"),
        Index("ix_concept_id", "concept_id"),
    )

    dataset_concept_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    dataset_id = Column(Integer, nullable=False)  # References dataset_master.dataset_id
    concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("dataset_id", "concept_id", name="uq_dataset_concept"),
        Index("ix_dataset_id", "dataset_id"),
        Index("ix_concept_id", "concept_id"),
    )

    # Relationships
    concept: Concept = relationship("Concept", back_populates="dataset_concepts")

    def __repr__(self) -> str:
        return f"<DatasetConcept(dataset_id={self.dataset_id})>"


class OntologyChangeLog(Base):
    """CML Ontology Change Log - Tracks changes to ontology versions."""

    __tablename__ = "cml_ontology_change_log"
    __allow_unmapped__ = True
    __table_args__ = (
        Index("ix_ontology_version_id", "ontology_version_id"),
        Index("ix_change_type", "change_type"),
    )

    change_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    ontology_version_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_ontology_version.ontology_version_id", ondelete="CASCADE"),
        nullable=False,
    )
    change_type = Column(String(50), nullable=False)  # add_property, deprecate_concept, etc.
    description = Column(Text, nullable=True)
    changed_by = Column(String(100), nullable=True)
    changed_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_ontology_version_id", "ontology_version_id"),
        Index("ix_change_type", "change_type"),
    )

    # Relationships
    ontology_version: OntologyVersion = relationship(
        "OntologyVersion", back_populates="change_logs"
    )

    def __repr__(self) -> str:
        return f"<OntologyChangeLog(change_type='{self.change_type}')>"
