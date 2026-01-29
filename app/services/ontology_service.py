"""Business logic for ontology domain."""
from typing import List, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from app.core.exceptions import ValidationError, NotFoundError
from app.repositories.ontology_repository import (
    DomainRepository,
    VocabularyRepository,
    VocabularyTermRepository,
    OntologyVersionRepository,
    ConceptRepository,
    PropertyRepository,
    PropertyConstraintRepository,
    DatasetFieldMappingRepository,
    DatasetConceptRepository,
    OntologyChangeLogRepository,
)
from app.models.ontology_model import (
    Domain,
    Vocabulary,
    VocabularyTerm,
    OntologyVersion,
    Concept,
    Property,
    PropertyConstraint,
    DatasetFieldMapping,
    DatasetConcept,
    OntologyChangeLog,
)


class DomainService:
    """Service for domain operations."""

    def __init__(self, db: Session):
        """Initialize service with database session."""
        self.db = db
        self.repository = DomainRepository(db)

    def create_domain(
        self, domain_code: str, domain_name: str, description: Optional[str] = None
    ) -> Domain:
        """Create a new domain."""
        # Validation
        existing = self.repository.get_by_code(domain_code)
        if existing:
            raise ValidationError(f"Domain with code '{domain_code}' already exists")

        return self.repository.create(domain_code, domain_name, description)

    def get_domain(self, domain_id: UUID) -> Domain:
        """Get domain by ID."""
        domain = self.repository.get_by_id(domain_id)
        if not domain:
            raise NotFoundError(f"Domain not found: {domain_id}")
        return domain

    def list_domains(self, is_active: Optional[bool] = True) -> List[Domain]:
        """List all domains."""
        return self.repository.list_all(is_active)

    def update_domain(
        self, domain_id: UUID, domain_name: Optional[str] = None, description: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> Domain:
        """Update domain."""
        domain = self.get_domain(domain_id)
        kwargs = {}
        if domain_name is not None:
            kwargs["domain_name"] = domain_name
        if description is not None:
            kwargs["description"] = description
        if is_active is not None:
            kwargs["is_active"] = is_active

        updated = self.repository.update(domain_id, **kwargs)
        if not updated:
            raise NotFoundError(f"Domain not found: {domain_id}")
        return updated

    def delete_domain(self, domain_id: UUID) -> bool:
        """Delete domain."""
        domain = self.get_domain(domain_id)  # Verify exists
        return self.repository.delete(domain_id)


class VocabularyService:
    """Service for vocabulary operations."""

    def __init__(self, db: Session):
        """Initialize service with database session."""
        self.db = db
        self.vocab_repository = VocabularyRepository(db)
        self.term_repository = VocabularyTermRepository(db)

    def create_vocabulary(
        self, vocab_code: str, vocab_name: str, description: Optional[str] = None
    ) -> Vocabulary:
        """Create a new vocabulary."""
        # Validation
        existing = self.vocab_repository.get_by_code(vocab_code)
        if existing:
            raise ValidationError(f"Vocabulary with code '{vocab_code}' already exists")

        return self.vocab_repository.create(vocab_code, vocab_name, description)

    def get_vocabulary(self, vocab_id: UUID) -> Vocabulary:
        """Get vocabulary by ID."""
        vocab = self.vocab_repository.get_by_id(vocab_id)
        if not vocab:
            raise NotFoundError(f"Vocabulary not found: {vocab_id}")
        return vocab

    def list_vocabularies(self) -> List[Vocabulary]:
        """List all vocabularies."""
        return self.vocab_repository.list_all()

    def add_term_to_vocabulary(
        self,
        vocab_id: UUID,
        term_code: str,
        term_label: str,
        description: Optional[str] = None,
        parent_term_id: Optional[UUID] = None,
    ) -> VocabularyTerm:
        """Add term to vocabulary."""
        vocab = self.get_vocabulary(vocab_id)
        return self.term_repository.create(
            vocab_id, term_code, term_label, description, parent_term_id
        )

    def get_term(self, term_id: UUID) -> VocabularyTerm:
        """Get vocabulary term by ID."""
        term = self.term_repository.get_by_id(term_id)
        if not term:
            raise NotFoundError(f"Term not found: {term_id}")
        return term

    def list_terms_by_vocabulary(self, vocab_id: UUID) -> List[VocabularyTerm]:
        """List terms by vocabulary."""
        vocab = self.get_vocabulary(vocab_id)
        return self.term_repository.list_by_vocab(vocab_id)

    def delete_vocabulary(self, vocab_id: UUID) -> bool:
        """Delete vocabulary."""
        vocab = self.get_vocabulary(vocab_id)  # Verify exists
        return self.vocab_repository.delete(vocab_id)


class OntologyVersionService:
    """Service for ontology version operations."""

    def __init__(self, db: Session):
        """Initialize service with database session."""
        self.db = db
        self.version_repository = OntologyVersionRepository(db)
        self.domain_repository = DomainRepository(db)
        self.changelog_repository = OntologyChangeLogRepository(db)

    def create_version(
        self, domain_id: UUID, version_code: str, description: Optional[str] = None
    ) -> OntologyVersion:
        """Create a new ontology version."""
        # Validate domain exists
        domain = self.domain_repository.get_by_id(domain_id)
        if not domain:
            raise NotFoundError(f"Domain not found: {domain_id}")

        # Create version
        version = self.version_repository.create(domain_id, version_code, description)

        # Log creation
        self.changelog_repository.create(
            version.ontology_version_id,
            "version_created",
            f"Created new ontology version {version_code}",
        )

        return version

    def get_version(self, ontology_version_id: UUID) -> OntologyVersion:
        """Get ontology version by ID."""
        version = self.version_repository.get_by_id(ontology_version_id)
        if not version:
            raise NotFoundError(f"Ontology version not found: {ontology_version_id}")
        return version

    def list_versions_by_domain(self, domain_id: UUID) -> List[OntologyVersion]:
        """List versions by domain."""
        domain = self.domain_repository.get_by_id(domain_id)
        if not domain:
            raise NotFoundError(f"Domain not found: {domain_id}")
        return self.version_repository.list_by_domain(domain_id)

    def activate_version(self, ontology_version_id: UUID) -> OntologyVersion:
        """Activate an ontology version."""
        version = self.get_version(ontology_version_id)

        # Get domain to deactivate other active versions
        active = self.version_repository.get_active_by_domain(version.domain_id)
        if active and active.ontology_version_id != ontology_version_id:
            self.version_repository.update(active.ontology_version_id, status="deprecated")

        # Activate new version
        updated = self.version_repository.update(ontology_version_id, status="active")

        # Log change
        self.changelog_repository.create(
            ontology_version_id,
            "version_activated",
            f"Activated ontology version",
        )

        return updated

    def deprecate_version(self, ontology_version_id: UUID) -> OntologyVersion:
        """Deprecate an ontology version."""
        version = self.get_version(ontology_version_id)
        updated = self.version_repository.update(ontology_version_id, status="deprecated")

        # Log change
        self.changelog_repository.create(
            ontology_version_id,
            "version_deprecated",
            f"Deprecated ontology version",
        )

        return updated


class ConceptService:
    """Service for concept operations."""

    def __init__(self, db: Session):
        """Initialize service with database session."""
        self.db = db
        self.concept_repository = ConceptRepository(db)
        self.version_repository = OntologyVersionRepository(db)
        self.property_repository = PropertyRepository(db)
        self.constraint_repository = PropertyConstraintRepository(db)
        self.changelog_repository = OntologyChangeLogRepository(db)

    def create_concept(
        self,
        domain_id: UUID,
        ontology_version_id: UUID,
        concept_code: str,
        concept_name: str,
        description: Optional[str] = None,
        parent_concept_id: Optional[UUID] = None,
        is_abstract: bool = False,
    ) -> Concept:
        """Create a new concept."""
        # Validate version exists
        version = self.version_repository.get_by_id(ontology_version_id)
        if not version:
            raise NotFoundError(f"Ontology version not found: {ontology_version_id}")

        # Create concept
        concept = self.concept_repository.create(
            domain_id,
            ontology_version_id,
            concept_code,
            concept_name,
            description,
            parent_concept_id,
            is_abstract,
        )

        # Log change
        self.changelog_repository.create(
            ontology_version_id,
            "concept_added",
            f"Added concept {concept_code}",
        )

        return concept

    def get_concept(self, concept_id: UUID) -> Concept:
        """Get concept by ID."""
        concept = self.concept_repository.get_by_id(concept_id)
        if not concept:
            raise NotFoundError(f"Concept not found: {concept_id}")
        return concept

    def list_concepts_by_version(self, ontology_version_id: UUID) -> List[Concept]:
        """List concepts by ontology version."""
        version = self.version_repository.get_by_id(ontology_version_id)
        if not version:
            raise NotFoundError(f"Ontology version not found: {ontology_version_id}")
        return self.concept_repository.list_by_version(ontology_version_id)

    def add_property_to_concept(
        self,
        concept_id: UUID,
        property_code: str,
        property_name: str,
        data_type: str,
        description: Optional[str] = None,
        is_required: bool = False,
        is_multivalued: bool = False,
    ) -> Property:
        """Add property to concept."""
        concept = self.get_concept(concept_id)
        prop = self.property_repository.create(
            concept_id, property_code, property_name, data_type, description, is_required, is_multivalued
        )

        # Log change
        self.changelog_repository.create(
            concept.ontology_version_id,
            "property_added",
            f"Added property {property_code} to concept {concept.concept_code}",
        )

        return prop

    def delete_concept(self, concept_id: UUID) -> bool:
        """Delete concept."""
        concept = self.get_concept(concept_id)  # Verify exists
        return self.concept_repository.delete(concept_id)


class PropertyService:
    """Service for property operations."""

    def __init__(self, db: Session):
        """Initialize service with database session."""
        self.db = db
        self.property_repository = PropertyRepository(db)
        self.constraint_repository = PropertyConstraintRepository(db)
        self.concept_repository = ConceptRepository(db)

    def get_property(self, property_id: UUID) -> Property:
        """Get property by ID."""
        prop = self.property_repository.get_by_id(property_id)
        if not prop:
            raise NotFoundError(f"Property not found: {property_id}")
        return prop

    def list_properties_by_concept(self, concept_id: UUID) -> List[Property]:
        """List properties by concept."""
        concept = self.concept_repository.get_by_id(concept_id)
        if not concept:
            raise NotFoundError(f"Concept not found: {concept_id}")
        return self.property_repository.list_by_concept(concept_id)

    def add_constraint_to_property(
        self,
        property_id: UUID,
        constraint_type: str,
        constraint_value: dict,
    ) -> PropertyConstraint:
        """Add constraint to property."""
        prop = self.get_property(property_id)

        # Validate constraint type
        valid_types = {"range", "enum", "regex", "unit", "geometry", "vocab"}
        if constraint_type not in valid_types:
            raise ValidationError(f"Invalid constraint type: {constraint_type}")

        return self.constraint_repository.create(property_id, constraint_type, constraint_value)

    def list_constraints_by_property(self, property_id: UUID) -> List[PropertyConstraint]:
        """List constraints by property."""
        prop = self.get_property(property_id)
        return self.constraint_repository.list_by_property(property_id)

    def delete_property(self, property_id: UUID) -> bool:
        """Delete property."""
        prop = self.get_property(property_id)  # Verify exists
        return self.property_repository.delete(property_id)


class DatasetMappingService:
    """Service for dataset field mapping operations."""

    def __init__(self, db: Session):
        """Initialize service with database session."""
        self.db = db
        self.mapping_repository = DatasetFieldMappingRepository(db)
        self.property_repository = PropertyRepository(db)
        self.concept_repository = ConceptRepository(db)

    def create_mapping(
        self,
        dataset_id: UUID,
        concept_id: UUID,
        property_id: UUID,
        external_field_name: str,
        external_data_type: Optional[str] = None,
        transform_rule: Optional[dict] = None,
        is_exposed: bool = True,
    ) -> DatasetFieldMapping:
        """Create a dataset field mapping."""
        # Validate concept and property exist
        concept = self.concept_repository.get_by_id(concept_id)
        if not concept:
            raise NotFoundError(f"Concept not found: {concept_id}")

        prop = self.property_repository.get_by_id(property_id)
        if not prop:
            raise NotFoundError(f"Property not found: {property_id}")

        # Verify property belongs to concept
        if prop.concept_id != concept_id:
            raise ValidationError(f"Property does not belong to concept")

        return self.mapping_repository.create(
            dataset_id, concept_id, property_id, external_field_name, external_data_type, transform_rule, is_exposed
        )

    def get_mapping(self, mapping_id: UUID) -> DatasetFieldMapping:
        """Get mapping by ID."""
        mapping = self.mapping_repository.get_by_id(mapping_id)
        if not mapping:
            raise NotFoundError(f"Mapping not found: {mapping_id}")
        return mapping

    def list_mappings_by_dataset(self, dataset_id: UUID) -> List[DatasetFieldMapping]:
        """List mappings by dataset."""
        return self.mapping_repository.list_by_dataset(dataset_id)

    def list_mappings_by_concept(self, concept_id: UUID) -> List[DatasetFieldMapping]:
        """List mappings by concept."""
        concept = self.concept_repository.get_by_id(concept_id)
        if not concept:
            raise NotFoundError(f"Concept not found: {concept_id}")
        return self.mapping_repository.list_by_concept(concept_id)

    def update_mapping(
        self,
        mapping_id: UUID,
        external_field_name: Optional[str] = None,
        transform_rule: Optional[dict] = None,
        is_exposed: Optional[bool] = None,
    ) -> DatasetFieldMapping:
        """Update mapping."""
        mapping = self.get_mapping(mapping_id)
        kwargs = {}
        if external_field_name is not None:
            kwargs["external_field_name"] = external_field_name
        if transform_rule is not None:
            kwargs["transform_rule"] = transform_rule
        if is_exposed is not None:
            kwargs["is_exposed"] = is_exposed

        updated = self.mapping_repository.update(mapping_id, **kwargs)
        if not updated:
            raise NotFoundError(f"Mapping not found: {mapping_id}")
        return updated

    def delete_mapping(self, mapping_id: UUID) -> bool:
        """Delete mapping."""
        mapping = self.get_mapping(mapping_id)  # Verify exists
        return self.mapping_repository.delete(mapping_id)


class DatasetConceptService:
    """Service for dataset concept operations."""

    def __init__(self, db: Session):
        """Initialize service with database session."""
        self.db = db
        self.repository = DatasetConceptRepository(db)
        self.concept_repository = ConceptRepository(db)

    def link_dataset_to_concept(self, dataset_id: UUID, concept_id: UUID) -> DatasetConcept:
        """Link dataset to concept."""
        concept = self.concept_repository.get_by_id(concept_id)
        if not concept:
            raise NotFoundError(f"Concept not found: {concept_id}")

        return self.repository.create(dataset_id, concept_id)

    def get_link(self, id: UUID) -> DatasetConcept:
        """Get dataset concept link by ID."""
        link = self.repository.get_by_id(id)
        if not link:
            raise NotFoundError(f"Link not found: {id}")
        return link

    def list_concepts_by_dataset(self, dataset_id: UUID) -> List[DatasetConcept]:
        """List concepts by dataset."""
        return self.repository.list_by_dataset(dataset_id)

    def list_datasets_by_concept(self, concept_id: UUID) -> List[DatasetConcept]:
        """List datasets by concept."""
        concept = self.concept_repository.get_by_id(concept_id)
        if not concept:
            raise NotFoundError(f"Concept not found: {concept_id}")
        return self.repository.list_by_concept(concept_id)

    def unlink_dataset_from_concept(self, id: UUID) -> bool:
        """Unlink dataset from concept."""
        link = self.get_link(id)  # Verify exists
        return self.repository.delete(id)
