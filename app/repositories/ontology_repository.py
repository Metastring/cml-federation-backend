"""Repository implementations for ontology domain."""
from typing import List, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.ontology_model import (
    Domain,
    OntologyVersion,
    Concept,
    Property,
    PropertyConstraint,
    Vocabulary,
    VocabularyTerm,
    DatasetFieldMapping,
    DatasetConcept,
    OntologyChangeLog,
)


class DomainRepository:
    """Repository for Domain operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        domain_code: str,
        domain_name: str,
        description: Optional[str] = None,
    ) -> Domain:
        """Create a new domain."""
        domain = Domain(
            domain_code=domain_code,
            domain_name=domain_name,
            description=description,
        )
        self.db.add(domain)
        self.db.flush()
        return domain

    def get_by_id(self, domain_id: UUID) -> Optional[Domain]:
        """Get domain by ID."""
        return self.db.query(Domain).filter(Domain.domain_id == domain_id).first()

    def get_by_code(self, domain_code: str) -> Optional[Domain]:
        """Get domain by code."""
        return self.db.query(Domain).filter(Domain.domain_code == domain_code).first()

    def list_all(self, active_only: bool = True) -> List[Domain]:
        """List all domains."""
        query = self.db.query(Domain)
        if active_only:
            query = query.filter(Domain.is_active == True)
        return query.all()

    def update(self, domain_id: UUID, **kwargs) -> Domain:
        """Update a domain."""
        domain = self.get_by_id(domain_id)
        if not domain:
            return None
        for key, value in kwargs.items():
            if hasattr(domain, key):
                setattr(domain, key, value)
        self.db.flush()
        return domain

    def delete(self, domain_id: UUID) -> bool:
        """Delete a domain."""
        domain = self.get_by_id(domain_id)
        if not domain:
            return False
        self.db.delete(domain)
        return True


class OntologyVersionRepository:
    """Repository for OntologyVersion operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        domain_id: UUID,
        version_code: str,
        status: str = "draft",
        description: Optional[str] = None,
    ) -> OntologyVersion:
        """Create a new ontology version."""
        version = OntologyVersion(
            domain_id=domain_id,
            version_code=version_code,
            status=status,
            description=description,
        )
        self.db.add(version)
        self.db.flush()
        return version

    def get_by_id(self, ontology_version_id: UUID) -> Optional[OntologyVersion]:
        """Get ontology version by ID."""
        return self.db.query(OntologyVersion).filter(
            OntologyVersion.ontology_version_id == ontology_version_id
        ).first()

    def get_by_domain_and_version(
        self, domain_id: UUID, version_code: str
    ) -> Optional[OntologyVersion]:
        """Get ontology version by domain and version code."""
        return self.db.query(OntologyVersion).filter(
            OntologyVersion.domain_id == domain_id,
            OntologyVersion.version_code == version_code,
        ).first()

    def list_by_domain(self, domain_id: UUID) -> List[OntologyVersion]:
        """List all versions for a domain."""
        return self.db.query(OntologyVersion).filter(
            OntologyVersion.domain_id == domain_id
        ).all()

    def get_active_version(self, domain_id: UUID) -> Optional[OntologyVersion]:
        """Get active version for a domain."""
        return self.db.query(OntologyVersion).filter(
            OntologyVersion.domain_id == domain_id,
            OntologyVersion.status == "active",
        ).first()

    def update(self, ontology_version_id: UUID, **kwargs) -> Optional[OntologyVersion]:
        """Update an ontology version."""
        version = self.get_by_id(ontology_version_id)
        if not version:
            return None
        for key, value in kwargs.items():
            if hasattr(version, key):
                setattr(version, key, value)
        self.db.flush()
        return version

    def delete(self, ontology_version_id: UUID) -> bool:
        """Delete an ontology version."""
        version = self.get_by_id(ontology_version_id)
        if not version:
            return False
        self.db.delete(version)
        return True


class ConceptRepository:
    """Repository for Concept operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        ontology_version_id: UUID,
        concept_code: str,
        concept_name: str,
        description: Optional[str] = None,
        parent_concept_id: Optional[UUID] = None,
        is_abstract: bool = False,
    ) -> Concept:
        """Create a new concept."""
        concept = Concept(
            ontology_version_id=ontology_version_id,
            concept_code=concept_code,
            concept_name=concept_name,
            description=description,
            parent_concept_id=parent_concept_id,
            is_abstract=is_abstract,
        )
        self.db.add(concept)
        self.db.flush()
        return concept

    def get_by_id(self, concept_id: UUID) -> Optional[Concept]:
        """Get concept by ID."""
        return self.db.query(Concept).filter(Concept.concept_id == concept_id).first()

    def get_by_code(
        self, ontology_version_id: UUID, concept_code: str
    ) -> Optional[Concept]:
        """Get concept by code within a version."""
        return self.db.query(Concept).filter(
            Concept.ontology_version_id == ontology_version_id,
            Concept.concept_code == concept_code,
        ).first()

    def list_by_version(self, ontology_version_id: UUID) -> List[Concept]:
        """List all concepts in a version."""
        return self.db.query(Concept).filter(
            Concept.ontology_version_id == ontology_version_id
        ).all()

    def list_root_concepts(self, ontology_version_id: UUID) -> List[Concept]:
        """List root concepts (no parent) in a version."""
        return self.db.query(Concept).filter(
            Concept.ontology_version_id == ontology_version_id,
            Concept.parent_concept_id == None,
        ).all()

    def update(self, concept_id: UUID, **kwargs) -> Optional[Concept]:
        """Update a concept."""
        concept = self.get_by_id(concept_id)
        if not concept:
            return None
        for key, value in kwargs.items():
            if hasattr(concept, key):
                setattr(concept, key, value)
        self.db.flush()
        return concept

    def delete(self, concept_id: UUID) -> bool:
        """Delete a concept."""
        concept = self.get_by_id(concept_id)
        if not concept:
            return False
        self.db.delete(concept)
        return True


class PropertyRepository:
    """Repository for Property operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        concept_id: UUID,
        property_code: str,
        property_name: str,
        data_type: str,
        description: Optional[str] = None,
        is_required: bool = False,
        is_multivalued: bool = False,
    ) -> Property:
        """Create a new property."""
        prop = Property(
            concept_id=concept_id,
            property_code=property_code,
            property_name=property_name,
            data_type=data_type,
            description=description,
            is_required=is_required,
            is_multivalued=is_multivalued,
        )
        self.db.add(prop)
        self.db.flush()
        return prop

    def get_by_id(self, property_id: UUID) -> Optional[Property]:
        """Get property by ID."""
        return self.db.query(Property).filter(Property.property_id == property_id).first()

    def get_by_code(self, concept_id: UUID, property_code: str) -> Optional[Property]:
        """Get property by code within a concept."""
        return self.db.query(Property).filter(
            Property.concept_id == concept_id,
            Property.property_code == property_code,
        ).first()

    def list_by_concept(self, concept_id: UUID) -> List[Property]:
        """List all properties for a concept."""
        return self.db.query(Property).filter(Property.concept_id == concept_id).all()

    def update(self, property_id: UUID, **kwargs) -> Optional[Property]:
        """Update a property."""
        prop = self.get_by_id(property_id)
        if not prop:
            return None
        for key, value in kwargs.items():
            if hasattr(prop, key):
                setattr(prop, key, value)
        self.db.flush()
        return prop

    def delete(self, property_id: UUID) -> bool:
        """Delete a property."""
        prop = self.get_by_id(property_id)
        if not prop:
            return False
        self.db.delete(prop)
        return True


class PropertyConstraintRepository:
    """Repository for PropertyConstraint operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        property_id: UUID,
        constraint_type: str,
        constraint_value: dict,
    ) -> PropertyConstraint:
        """Create a new property constraint."""
        constraint = PropertyConstraint(
            property_id=property_id,
            constraint_type=constraint_type,
            constraint_value=constraint_value,
        )
        self.db.add(constraint)
        self.db.flush()
        return constraint

    def get_by_id(self, constraint_id: UUID) -> Optional[PropertyConstraint]:
        """Get constraint by ID."""
        return self.db.query(PropertyConstraint).filter(
            PropertyConstraint.constraint_id == constraint_id
        ).first()

    def list_by_property(self, property_id: UUID) -> List[PropertyConstraint]:
        """List all constraints for a property."""
        return self.db.query(PropertyConstraint).filter(
            PropertyConstraint.property_id == property_id
        ).all()

    def update(self, constraint_id: UUID, **kwargs) -> Optional[PropertyConstraint]:
        """Update a constraint."""
        constraint = self.get_by_id(constraint_id)
        if not constraint:
            return None
        for key, value in kwargs.items():
            if hasattr(constraint, key):
                setattr(constraint, key, value)
        self.db.flush()
        return constraint

    def delete(self, constraint_id: UUID) -> bool:
        """Delete a constraint."""
        constraint = self.get_by_id(constraint_id)
        if not constraint:
            return False
        self.db.delete(constraint)
        return True


class VocabularyRepository:
    """Repository for Vocabulary operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        vocab_code: str,
        description: Optional[str] = None,
    ) -> Vocabulary:
        """Create a new vocabulary."""
        vocab = Vocabulary(
            vocab_code=vocab_code,
            description=description,
        )
        self.db.add(vocab)
        self.db.flush()
        return vocab

    def get_by_id(self, vocab_id: UUID) -> Optional[Vocabulary]:
        """Get vocabulary by ID."""
        return self.db.query(Vocabulary).filter(Vocabulary.vocab_id == vocab_id).first()

    def get_by_code(self, vocab_code: str) -> Optional[Vocabulary]:
        """Get vocabulary by code."""
        return self.db.query(Vocabulary).filter(
            Vocabulary.vocab_code == vocab_code
        ).first()

    def list_all(self) -> List[Vocabulary]:
        """List all vocabularies."""
        return self.db.query(Vocabulary).all()

    def update(self, vocab_id: UUID, **kwargs) -> Optional[Vocabulary]:
        """Update a vocabulary."""
        vocab = self.get_by_id(vocab_id)
        if not vocab:
            return None
        for key, value in kwargs.items():
            if hasattr(vocab, key):
                setattr(vocab, key, value)
        self.db.flush()
        return vocab

    def delete(self, vocab_id: UUID) -> bool:
        """Delete a vocabulary."""
        vocab = self.get_by_id(vocab_id)
        if not vocab:
            return False
        self.db.delete(vocab)
        return True


class VocabularyTermRepository:
    """Repository for VocabularyTerm operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        vocab_id: UUID,
        term_code: str,
        term_label: str,
        parent_term_id: Optional[UUID] = None,
    ) -> VocabularyTerm:
        """Create a new vocabulary term."""
        term = VocabularyTerm(
            vocab_id=vocab_id,
            term_code=term_code,
            term_label=term_label,
            parent_term_id=parent_term_id,
        )
        self.db.add(term)
        self.db.flush()
        return term

    def get_by_id(self, term_id: UUID) -> Optional[VocabularyTerm]:
        """Get term by ID."""
        return self.db.query(VocabularyTerm).filter(
            VocabularyTerm.term_id == term_id
        ).first()

    def get_by_code(self, vocab_id: UUID, term_code: str) -> Optional[VocabularyTerm]:
        """Get term by code within a vocabulary."""
        return self.db.query(VocabularyTerm).filter(
            VocabularyTerm.vocab_id == vocab_id,
            VocabularyTerm.term_code == term_code,
        ).first()

    def list_by_vocabulary(self, vocab_id: UUID) -> List[VocabularyTerm]:
        """List all terms in a vocabulary."""
        return self.db.query(VocabularyTerm).filter(
            VocabularyTerm.vocab_id == vocab_id
        ).all()

    def list_root_terms(self, vocab_id: UUID) -> List[VocabularyTerm]:
        """List root terms (no parent) in a vocabulary."""
        return self.db.query(VocabularyTerm).filter(
            VocabularyTerm.vocab_id == vocab_id,
            VocabularyTerm.parent_term_id == None,
        ).all()

    def update(self, term_id: UUID, **kwargs) -> Optional[VocabularyTerm]:
        """Update a term."""
        term = self.get_by_id(term_id)
        if not term:
            return None
        for key, value in kwargs.items():
            if hasattr(term, key):
                setattr(term, key, value)
        self.db.flush()
        return term

    def delete(self, term_id: UUID) -> bool:
        """Delete a term."""
        term = self.get_by_id(term_id)
        if not term:
            return False
        self.db.delete(term)
        return True


class DatasetFieldMappingRepository:
    """Repository for DatasetFieldMapping operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        dataset_id: int,
        concept_id: UUID,
        property_id: UUID,
        external_field_name: str,
        external_data_type: Optional[str] = None,
        transform_rule: Optional[dict] = None,
        is_exposed: bool = True,
    ) -> DatasetFieldMapping:
        """Create a new dataset field mapping."""
        mapping = DatasetFieldMapping(
            dataset_id=dataset_id,
            concept_id=concept_id,
            property_id=property_id,
            external_field_name=external_field_name,
            external_data_type=external_data_type,
            transform_rule=transform_rule,
            is_exposed=is_exposed,
        )
        self.db.add(mapping)
        self.db.flush()
        return mapping

    def get_by_id(self, mapping_id: UUID) -> Optional[DatasetFieldMapping]:
        """Get mapping by ID."""
        return self.db.query(DatasetFieldMapping).filter(
            DatasetFieldMapping.mapping_id == mapping_id
        ).first()

    def list_by_dataset(self, dataset_id: int) -> List[DatasetFieldMapping]:
        """List all mappings for a dataset."""
        return self.db.query(DatasetFieldMapping).filter(
            DatasetFieldMapping.dataset_id == dataset_id
        ).all()

    def list_by_concept(self, concept_id: UUID) -> List[DatasetFieldMapping]:
        """List all mappings for a concept."""
        return self.db.query(DatasetFieldMapping).filter(
            DatasetFieldMapping.concept_id == concept_id
        ).all()

    def update(self, mapping_id: UUID, **kwargs) -> Optional[DatasetFieldMapping]:
        """Update a mapping."""
        mapping = self.get_by_id(mapping_id)
        if not mapping:
            return None
        for key, value in kwargs.items():
            if hasattr(mapping, key):
                setattr(mapping, key, value)
        self.db.flush()
        return mapping

    def delete(self, mapping_id: UUID) -> bool:
        """Delete a mapping."""
        mapping = self.get_by_id(mapping_id)
        if not mapping:
            return False
        self.db.delete(mapping)
        return True


class DatasetConceptRepository:
    """Repository for DatasetConcept operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        dataset_id: int,
        concept_id: UUID,
    ) -> DatasetConcept:
        """Create a new dataset concept."""
        dc = DatasetConcept(
            dataset_id=dataset_id,
            concept_id=concept_id,
        )
        self.db.add(dc)
        self.db.flush()
        return dc

    def get_by_id(self, dataset_concept_id: UUID) -> Optional[DatasetConcept]:
        """Get dataset concept by ID."""
        return self.db.query(DatasetConcept).filter(
            DatasetConcept.dataset_concept_id == dataset_concept_id
        ).first()

    def list_by_dataset(self, dataset_id: int) -> List[DatasetConcept]:
        """List all concepts for a dataset."""
        return self.db.query(DatasetConcept).filter(
            DatasetConcept.dataset_id == dataset_id
        ).all()

    def list_by_concept(self, concept_id: UUID) -> List[DatasetConcept]:
        """List all datasets using a concept."""
        return self.db.query(DatasetConcept).filter(
            DatasetConcept.concept_id == concept_id
        ).all()

    def delete(self, dataset_concept_id: UUID) -> bool:
        """Delete a dataset concept."""
        dc = self.get_by_id(dataset_concept_id)
        if not dc:
            return False
        self.db.delete(dc)
        return True


class OntologyChangeLogRepository:
    """Repository for OntologyChangeLog operations."""

    def __init__(self, db: Session):
        """Initialize repository with database session."""
        self.db = db

    def create(
        self,
        ontology_version_id: UUID,
        change_type: str,
        description: Optional[str] = None,
        changed_by: Optional[str] = None,
    ) -> OntologyChangeLog:
        """Create a new change log entry."""
        log = OntologyChangeLog(
            ontology_version_id=ontology_version_id,
            change_type=change_type,
            description=description,
            changed_by=changed_by,
        )
        self.db.add(log)
        self.db.flush()
        return log

    def get_by_id(self, change_id: UUID) -> Optional[OntologyChangeLog]:
        """Get change log by ID."""
        return self.db.query(OntologyChangeLog).filter(
            OntologyChangeLog.change_id == change_id
        ).first()

    def list_by_version(self, ontology_version_id: UUID) -> List[OntologyChangeLog]:
        """List all changes for a version."""
        return self.db.query(OntologyChangeLog).filter(
            OntologyChangeLog.ontology_version_id == ontology_version_id
        ).order_by(OntologyChangeLog.changed_at.desc()).all()

    def delete(self, change_id: UUID) -> bool:
        """Delete a change log entry."""
        log = self.get_by_id(change_id)
        if not log:
            return False
        self.db.delete(log)
        return True
