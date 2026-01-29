"""SQLAlchemy models for ontology system."""
from datetime import datetime
from typing import Optional
from uuid import uuid4
from sqlalchemy import (
    Column, String, Text, Boolean, DateTime, ForeignKey, JSON, 
    Index, UniqueConstraint, Integer, Enum as SQLEnum
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship
import enum

Base = declarative_base()


class Dataset(Base):
    """Represents a dataset in the system."""
    
    __tablename__ = "cml_dataset"
    
    dataset_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    dataset_code = Column(String(100), unique=True, nullable=False, index=True)
    dataset_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    field_mappings = relationship(
        "DatasetFieldMapping",
        back_populates="dataset",
        cascade="all, delete-orphan"
    )
    dataset_concepts = relationship(
        "DatasetConcept",
        back_populates="dataset",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("idx_dataset_code", "dataset_code"),
        Index("idx_dataset_is_active", "is_active"),
    )


class Domain(Base):
    """Represents a domain (biodiversity, climate, health, etc.)."""
    
    __tablename__ = "cml_domain"
    
    domain_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    domain_code = Column(String(50), unique=True, nullable=False, index=True)
    domain_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    ontology_versions = relationship(
        "OntologyVersion",
        back_populates="domain",
        cascade="all, delete-orphan"
    )
    concepts = relationship(
        "Concept",
        back_populates="domain",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("idx_domain_code", "domain_code"),
        Index("idx_domain_is_active", "is_active"),
    )


class StatusEnum(str, enum.Enum):
    """Enumeration for ontology version status."""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"


class OntologyVersion(Base):
    """Represents a versioned snapshot of the ontology."""
    
    __tablename__ = "cml_ontology_version"
    
    ontology_version_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    domain_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_domain.domain_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    version_code = Column(String(50), nullable=False)
    status = Column(
        SQLEnum(StatusEnum),
        default=StatusEnum.DRAFT,
        nullable=False,
        index=True
    )
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    domain = relationship("Domain", back_populates="ontology_versions")
    concepts = relationship(
        "Concept",
        back_populates="ontology_version",
        cascade="all, delete-orphan"
    )
    change_logs = relationship(
        "OntologyChangeLog",
        back_populates="ontology_version",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        UniqueConstraint("domain_id", "version_code", name="uq_domain_version"),
        Index("idx_ontology_version_status", "status"),
    )


class Concept(Base):
    """Represents an ontology concept (class/entity)."""
    
    __tablename__ = "cml_concept"
    
    concept_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    domain_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_domain.domain_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    ontology_version_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_ontology_version.ontology_version_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    concept_code = Column(String(100), nullable=False)
    concept_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    parent_concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="SET NULL"),
        nullable=True,
        index=True
    )
    is_abstract = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    domain = relationship("Domain", back_populates="concepts")
    ontology_version = relationship("OntologyVersion", back_populates="concepts")
    properties = relationship(
        "Property",
        back_populates="concept",
        cascade="all, delete-orphan"
    )
    parent_concept = relationship(
        "Concept",
        remote_side=[concept_id],
        backref="child_concepts"
    )
    dataset_concepts = relationship(
        "DatasetConcept",
        back_populates="concept",
        cascade="all, delete-orphan"
    )
    dataset_field_mappings = relationship(
        "DatasetFieldMapping",
        back_populates="concept",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        UniqueConstraint(
            "domain_id",
            "ontology_version_id",
            "concept_code",
            name="uq_concept_code_per_version"
        ),
        Index("idx_concept_code", "concept_code"),
        Index("idx_concept_is_abstract", "is_abstract"),
    )


class Property(Base):
    """Represents a property/attribute of a concept."""
    
    __tablename__ = "cml_property"
    
    property_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    property_code = Column(String(100), nullable=False)
    property_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    data_type = Column(String(50), nullable=False)  # string, number, date, geometry, boolean
    is_required = Column(Boolean, default=False, nullable=False)
    is_multivalued = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    concept = relationship("Concept", back_populates="properties")
    constraints = relationship(
        "PropertyConstraint",
        back_populates="property",
        cascade="all, delete-orphan"
    )
    dataset_field_mappings = relationship(
        "DatasetFieldMapping",
        back_populates="property",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        UniqueConstraint(
            "concept_id",
            "property_code",
            name="uq_property_code_per_concept"
        ),
        Index("idx_property_code", "property_code"),
        Index("idx_property_data_type", "data_type"),
    )


class PropertyConstraint(Base):
    """Represents a constraint/rule on a property."""
    
    __tablename__ = "cml_property_constraint"
    
    constraint_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    property_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_property.property_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    constraint_type = Column(
        String(50),
        nullable=False,
        index=True
    )  # range, enum, regex, unit, geometry, vocab
    constraint_value = Column(JSON, nullable=False)  # Flexible JSON storage
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    property = relationship("Property", back_populates="constraints")
    
    __table_args__ = (
        Index("idx_constraint_type", "constraint_type"),
    )


class Vocab(Base):
    """Represents a controlled vocabulary."""
    
    __tablename__ = "cml_vocab"
    
    vocab_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    vocab_code = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    terms = relationship(
        "VocabTerm",
        back_populates="vocab",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("idx_vocab_code", "vocab_code"),
    )


class VocabTerm(Base):
    """Represents a term within a vocabulary."""
    
    __tablename__ = "cml_vocab_term"
    
    term_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    vocab_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_vocab.vocab_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    term_code = Column(String(100), nullable=False)
    term_label = Column(String(255), nullable=False)
    parent_term_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_vocab_term.term_id", ondelete="SET NULL"),
        nullable=True,
        index=True
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    vocab = relationship("Vocab", back_populates="terms")
    parent_term = relationship(
        "VocabTerm",
        remote_side=[term_id],
        backref="child_terms"
    )
    
    __table_args__ = (
        UniqueConstraint(
            "vocab_id",
            "term_code",
            name="uq_term_code_per_vocab"
        ),
        Index("idx_vocab_term_code", "term_code"),
    )


class DatasetFieldMapping(Base):
    """Maps dataset fields to ontology properties."""
    
    __tablename__ = "cml_dataset_field_mapping"
    
    mapping_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_dataset.dataset_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    property_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_property.property_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    external_field_name = Column(String(255), nullable=False)
    external_data_type = Column(String(50), nullable=True)
    transform_rule = Column(JSON, nullable=True)  # concat, cast, unit conversion, etc.
    is_exposed = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    dataset = relationship("Dataset", back_populates="field_mappings")
    concept = relationship("Concept", back_populates="dataset_field_mappings")
    property = relationship("Property", back_populates="dataset_field_mappings")
    
    __table_args__ = (
        UniqueConstraint(
            "dataset_id",
            "external_field_name",
            name="uq_dataset_field_mapping"
        ),
        Index("idx_dataset_field_mapping_dataset", "dataset_id"),
        Index("idx_dataset_field_mapping_property", "property_id"),
    )


class DatasetConcept(Base):
    """Associates datasets with ontology concepts."""
    
    __tablename__ = "cml_dataset_concept"
    
    dataset_concept_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_dataset.dataset_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    concept_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_concept.concept_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    dataset = relationship("Dataset", back_populates="dataset_concepts")
    concept = relationship("Concept", back_populates="dataset_concepts")
    
    __table_args__ = (
        UniqueConstraint(
            "dataset_id",
            "concept_id",
            name="uq_dataset_concept"
        ),
        Index("idx_dataset_concept_concept", "concept_id"),
    )


class ChangeTypeEnum(str, enum.Enum):
    """Enumeration for ontology change types."""
    ADD_PROPERTY = "add_property"
    REMOVE_PROPERTY = "remove_property"
    DEPRECATE_CONCEPT = "deprecate_concept"
    ADD_CONCEPT = "add_concept"
    MODIFY_PROPERTY = "modify_property"
    ADD_CONSTRAINT = "add_constraint"


class OntologyChangeLog(Base):
    """Logs changes to ontology versions."""
    
    __tablename__ = "cml_ontology_change_log"
    
    change_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    ontology_version_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cml_ontology_version.ontology_version_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    change_type = Column(SQLEnum(ChangeTypeEnum), nullable=False, index=True)
    description = Column(Text, nullable=False)
    changed_by = Column(String(255), nullable=True)
    changed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    ontology_version = relationship("OntologyVersion", back_populates="change_logs")
    
    __table_args__ = (
        Index("idx_change_log_change_type", "change_type"),
        Index("idx_change_log_changed_at", "changed_at"),
    )
