"""Unit tests for ontology models and repositories."""
import pytest
from uuid import uuid4
from datetime import datetime

from sqlalchemy.orm import Session
from unittest.mock import Mock, MagicMock, patch

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
from app.repositories.ontology_repository import (
    DomainRepository,
    OntologyVersionRepository,
    ConceptRepository,
    PropertyRepository,
    PropertyConstraintRepository,
    VocabularyRepository,
    VocabularyTermRepository,
    DatasetFieldMappingRepository,
    DatasetConceptRepository,
    OntologyChangeLogRepository,
)


@pytest.fixture
def mock_db():
    """Create a mock database session."""
    return Mock(spec=Session)


class TestDomain:
    """Tests for Domain model."""

    def test_domain_creation(self):
        """Test creating a domain instance."""
        domain = Domain(
            domain_code="biodiversity",
            domain_name="Biodiversity",
            description="Biodiversity domain"
        )
        assert domain.domain_code == "biodiversity"
        assert domain.domain_name == "Biodiversity"
        assert domain.is_active is True

    def test_domain_repr(self):
        """Test domain string representation."""
        domain = Domain(
            domain_code="biodiversity",
            domain_name="Biodiversity"
        )
        assert "biodiversity" in repr(domain)
        assert "Biodiversity" in repr(domain)


class TestOntologyVersion:
    """Tests for OntologyVersion model."""

    def test_ontology_version_creation(self):
        """Test creating an ontology version instance."""
        domain_id = uuid4()
        version = OntologyVersion(
            domain_id=domain_id,
            version_code="v1.0",
            status="draft",
            description="Version 1.0"
        )
        assert version.version_code == "v1.0"
        assert version.status == "draft"
        assert version.domain_id == domain_id

    def test_ontology_version_default_status(self):
        """Test ontology version default status."""
        version = OntologyVersion(
            domain_id=uuid4(),
            version_code="v1.0"
        )
        assert version.status == "draft"


class TestConcept:
    """Tests for Concept model."""

    def test_concept_creation(self):
        """Test creating a concept instance."""
        concept = Concept(
            ontology_version_id=uuid4(),
            concept_code="Species",
            concept_name="Species",
            description="Species concept"
        )
        assert concept.concept_code == "Species"
        assert concept.is_abstract is False

    def test_concept_with_parent(self):
        """Test concept with parent concept."""
        parent_id = uuid4()
        concept = Concept(
            ontology_version_id=uuid4(),
            concept_code="Taxon",
            concept_name="Taxon",
            parent_concept_id=parent_id
        )
        assert concept.parent_concept_id == parent_id


class TestProperty:
    """Tests for Property model."""

    def test_property_creation(self):
        """Test creating a property instance."""
        prop = Property(
            concept_id=uuid4(),
            property_code="scientificName",
            property_name="Scientific Name",
            data_type="string"
        )
        assert prop.property_code == "scientificName"
        assert prop.data_type == "string"
        assert prop.is_required is False
        assert prop.is_multivalued is False

    def test_property_with_constraints(self):
        """Test property with constraints."""
        prop = Property(
            concept_id=uuid4(),
            property_code="latitude",
            property_name="Latitude",
            data_type="number",
            is_required=True
        )
        assert prop.is_required is True


class TestPropertyConstraint:
    """Tests for PropertyConstraint model."""

    def test_constraint_creation(self):
        """Test creating a constraint instance."""
        constraint = PropertyConstraint(
            property_id=uuid4(),
            constraint_type="range",
            constraint_value={"min": -90, "max": 90}
        )
        assert constraint.constraint_type == "range"
        assert constraint.constraint_value == {"min": -90, "max": 90}

    def test_constraint_with_vocab(self):
        """Test constraint with vocabulary."""
        constraint = PropertyConstraint(
            property_id=uuid4(),
            constraint_type="vocab",
            constraint_value={"vocab_code": "conservation_status"}
        )
        assert constraint.constraint_type == "vocab"


class TestVocabulary:
    """Tests for Vocabulary model."""

    def test_vocabulary_creation(self):
        """Test creating a vocabulary instance."""
        vocab = Vocabulary(
            vocab_code="conservation_status",
            description="Conservation status vocabulary"
        )
        assert vocab.vocab_code == "conservation_status"

    def test_vocabulary_repr(self):
        """Test vocabulary string representation."""
        vocab = Vocabulary(vocab_code="conservation_status")
        assert "conservation_status" in repr(vocab)


class TestVocabularyTerm:
    """Tests for VocabularyTerm model."""

    def test_term_creation(self):
        """Test creating a vocabulary term instance."""
        term = VocabularyTerm(
            vocab_id=uuid4(),
            term_code="EN",
            term_label="Endangered"
        )
        assert term.term_code == "EN"
        assert term.term_label == "Endangered"

    def test_term_with_parent(self):
        """Test term with parent term."""
        parent_id = uuid4()
        term = VocabularyTerm(
            vocab_id=uuid4(),
            term_code="SubEN",
            term_label="Sub Endangered",
            parent_term_id=parent_id
        )
        assert term.parent_term_id == parent_id


class TestDatasetFieldMapping:
    """Tests for DatasetFieldMapping model."""

    def test_mapping_creation(self):
        """Test creating a mapping instance."""
        mapping = DatasetFieldMapping(
            dataset_id=1,
            concept_id=uuid4(),
            property_id=uuid4(),
            external_field_name="species_name"
        )
        assert mapping.dataset_id == 1
        assert mapping.external_field_name == "species_name"
        assert mapping.is_exposed is True

    def test_mapping_with_transform_rule(self):
        """Test mapping with transform rule."""
        mapping = DatasetFieldMapping(
            dataset_id=1,
            concept_id=uuid4(),
            property_id=uuid4(),
            external_field_name="location",
            transform_rule={"type": "concat", "fields": ["lat", "lon"]}
        )
        assert mapping.transform_rule is not None


class TestDatasetConcept:
    """Tests for DatasetConcept model."""

    def test_dataset_concept_creation(self):
        """Test creating a dataset concept instance."""
        dc = DatasetConcept(
            dataset_id=1,
            concept_id=uuid4()
        )
        assert dc.dataset_id == 1


class TestOntologyChangeLog:
    """Tests for OntologyChangeLog model."""

    def test_change_log_creation(self):
        """Test creating a change log instance."""
        log = OntologyChangeLog(
            ontology_version_id=uuid4(),
            change_type="add_property",
            description="Added new property",
            changed_by="user@example.com"
        )
        assert log.change_type == "add_property"
        assert log.changed_by == "user@example.com"


class TestDomainRepository:
    """Tests for DomainRepository."""

    def test_create_domain(self, mock_db):
        """Test creating a domain."""
        repo = DomainRepository(mock_db)
        domain = repo.create("biodiversity", "Biodiversity Domain")
        
        assert domain.domain_code == "biodiversity"
        mock_db.add.assert_called_once_with(domain)
        mock_db.flush.assert_called_once()

    def test_get_by_id(self, mock_db):
        """Test getting domain by ID."""
        domain_id = uuid4()
        domain = Domain(
            domain_id=domain_id,
            domain_code="test",
            domain_name="Test"
        )
        mock_db.query.return_value.filter.return_value.first.return_value = domain
        
        repo = DomainRepository(mock_db)
        result = repo.get_by_id(domain_id)
        
        assert result == domain

    def test_get_by_code(self, mock_db):
        """Test getting domain by code."""
        domain = Domain(
            domain_code="biodiversity",
            domain_name="Biodiversity"
        )
        mock_db.query.return_value.filter.return_value.first.return_value = domain
        
        repo = DomainRepository(mock_db)
        result = repo.get_by_code("biodiversity")
        
        assert result == domain

    def test_update_domain(self, mock_db):
        """Test updating a domain."""
        domain_id = uuid4()
        domain = Domain(
            domain_id=domain_id,
            domain_code="test",
            domain_name="Original Name"
        )
        mock_db.query.return_value.filter.return_value.first.return_value = domain
        
        repo = DomainRepository(mock_db)
        result = repo.update(domain_id, domain_name="Updated Name")
        
        assert result.domain_name == "Updated Name"

    def test_delete_domain(self, mock_db):
        """Test deleting a domain."""
        domain_id = uuid4()
        domain = Domain(domain_id=domain_id, domain_code="test", domain_name="Test")
        mock_db.query.return_value.filter.return_value.first.return_value = domain
        
        repo = DomainRepository(mock_db)
        result = repo.delete(domain_id)
        
        assert result is True
        mock_db.delete.assert_called_once_with(domain)


class TestOntologyVersionRepository:
    """Tests for OntologyVersionRepository."""

    def test_create_version(self, mock_db):
        """Test creating an ontology version."""
        domain_id = uuid4()
        repo = OntologyVersionRepository(mock_db)
        version = repo.create(domain_id, "v1.0", "draft")
        
        assert version.version_code == "v1.0"
        mock_db.add.assert_called_once_with(version)

    def test_get_active_version(self, mock_db):
        """Test getting active version."""
        domain_id = uuid4()
        version = OntologyVersion(
            domain_id=domain_id,
            version_code="v1.0",
            status="active"
        )
        mock_db.query.return_value.filter.return_value.first.return_value = version
        
        repo = OntologyVersionRepository(mock_db)
        result = repo.get_active_version(domain_id)
        
        assert result.status == "active"


class TestConceptRepository:
    """Tests for ConceptRepository."""

    def test_create_concept(self, mock_db):
        """Test creating a concept."""
        ontology_version_id = uuid4()
        repo = ConceptRepository(mock_db)
        concept = repo.create(ontology_version_id, "Species", "Species")
        
        assert concept.concept_code == "Species"
        mock_db.add.assert_called_once_with(concept)

    def test_get_by_code(self, mock_db):
        """Test getting concept by code."""
        ontology_version_id = uuid4()
        concept = Concept(
            ontology_version_id=ontology_version_id,
            concept_code="Species",
            concept_name="Species"
        )
        mock_db.query.return_value.filter.return_value.first.return_value = concept
        
        repo = ConceptRepository(mock_db)
        result = repo.get_by_code(ontology_version_id, "Species")
        
        assert result == concept


class TestPropertyRepository:
    """Tests for PropertyRepository."""

    def test_create_property(self, mock_db):
        """Test creating a property."""
        concept_id = uuid4()
        repo = PropertyRepository(mock_db)
        prop = repo.create(concept_id, "scientificName", "Scientific Name", "string")
        
        assert prop.property_code == "scientificName"
        mock_db.add.assert_called_once_with(prop)

    def test_list_by_concept(self, mock_db):
        """Test listing properties for a concept."""
        concept_id = uuid4()
        props = [
            Property(concept_id=concept_id, property_code="prop1", property_name="Property 1", data_type="string"),
            Property(concept_id=concept_id, property_code="prop2", property_name="Property 2", data_type="number"),
        ]
        mock_db.query.return_value.filter.return_value.all.return_value = props
        
        repo = PropertyRepository(mock_db)
        result = repo.list_by_concept(concept_id)
        
        assert len(result) == 2


class TestVocabularyRepository:
    """Tests for VocabularyRepository."""

    def test_create_vocabulary(self, mock_db):
        """Test creating a vocabulary."""
        repo = VocabularyRepository(mock_db)
        vocab = repo.create("conservation_status")
        
        assert vocab.vocab_code == "conservation_status"
        mock_db.add.assert_called_once_with(vocab)

    def test_get_by_code(self, mock_db):
        """Test getting vocabulary by code."""
        vocab = Vocabulary(vocab_code="conservation_status")
        mock_db.query.return_value.filter.return_value.first.return_value = vocab
        
        repo = VocabularyRepository(mock_db)
        result = repo.get_by_code("conservation_status")
        
        assert result == vocab


class TestVocabularyTermRepository:
    """Tests for VocabularyTermRepository."""

    def test_create_term(self, mock_db):
        """Test creating a vocabulary term."""
        vocab_id = uuid4()
        repo = VocabularyTermRepository(mock_db)
        term = repo.create(vocab_id, "EN", "Endangered")
        
        assert term.term_code == "EN"
        mock_db.add.assert_called_once_with(term)

    def test_list_by_vocabulary(self, mock_db):
        """Test listing terms for a vocabulary."""
        vocab_id = uuid4()
        terms = [
            VocabularyTerm(vocab_id=vocab_id, term_code="EN", term_label="Endangered"),
            VocabularyTerm(vocab_id=vocab_id, term_code="CR", term_label="Critically Endangered"),
        ]
        mock_db.query.return_value.filter.return_value.all.return_value = terms
        
        repo = VocabularyTermRepository(mock_db)
        result = repo.list_by_vocabulary(vocab_id)
        
        assert len(result) == 2
