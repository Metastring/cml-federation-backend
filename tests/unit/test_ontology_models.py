"""Unit tests for ontology models."""
import pytest
from uuid import uuid4
from datetime import datetime
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from app.models import (
    Dataset,
    Domain,
    OntologyVersion,
    Concept,
    Property,
    PropertyConstraint,
    Vocab,
    VocabTerm,
    DatasetFieldMapping,
    DatasetConcept,
    OntologyChangeLog,
    StatusEnum,
    ChangeTypeEnum,
)


class TestDomain:
    """Tests for Domain model."""
    
    def test_domain_creation(self):
        """Test creating a domain instance."""
        domain = Domain(
            domain_code="biodiversity",
            domain_name="Biodiversity",
            description="Biodiversity data domain",
            is_active=True
        )
        
        assert domain.domain_code == "biodiversity"
        assert domain.domain_name == "Biodiversity"
        assert domain.is_active is True
        assert isinstance(domain.domain_id, type(uuid4()))
    
    def test_domain_defaults(self):
        """Test domain default values."""
        domain = Domain(
            domain_code="test",
            domain_name="Test"
        )
        
        assert domain.is_active is True
        assert isinstance(domain.created_at, datetime)


class TestOntologyVersion:
    """Tests for OntologyVersion model."""
    
    def test_ontology_version_creation(self):
        """Test creating an ontology version."""
        domain_id = uuid4()
        version = OntologyVersion(
            domain_id=domain_id,
            version_code="v1.0",
            status=StatusEnum.ACTIVE,
            description="Version 1.0"
        )
        
        assert version.domain_id == domain_id
        assert version.version_code == "v1.0"
        assert version.status == StatusEnum.ACTIVE
    
    def test_ontology_version_status_enum(self):
        """Test ontology version status enum values."""
        assert StatusEnum.DRAFT.value == "draft"
        assert StatusEnum.ACTIVE.value == "active"
        assert StatusEnum.DEPRECATED.value == "deprecated"


class TestConcept:
    """Tests for Concept model."""
    
    def test_concept_creation(self):
        """Test creating a concept."""
        domain_id = uuid4()
        version_id = uuid4()
        
        concept = Concept(
            domain_id=domain_id,
            ontology_version_id=version_id,
            concept_code="Species",
            concept_name="Species",
            description="Biological species",
            is_abstract=False
        )
        
        assert concept.concept_code == "Species"
        assert concept.is_abstract is False
        assert concept.domain_id == domain_id
        assert concept.ontology_version_id == version_id
    
    def test_concept_with_parent(self):
        """Test concept with parent concept."""
        parent_id = uuid4()
        concept = Concept(
            domain_id=uuid4(),
            ontology_version_id=uuid4(),
            concept_code="Subspecies",
            concept_name="Subspecies",
            parent_concept_id=parent_id,
            is_abstract=False
        )
        
        assert concept.parent_concept_id == parent_id


class TestProperty:
    """Tests for Property model."""
    
    def test_property_creation(self):
        """Test creating a property."""
        concept_id = uuid4()
        
        prop = Property(
            concept_id=concept_id,
            property_code="scientificName",
            property_name="Scientific Name",
            data_type="string",
            is_required=True,
            is_multivalued=False
        )
        
        assert prop.property_code == "scientificName"
        assert prop.data_type == "string"
        assert prop.is_required is True
        assert prop.is_multivalued is False
    
    def test_property_data_types(self):
        """Test various data types for properties."""
        data_types = ["string", "number", "date", "geometry", "boolean"]
        concept_id = uuid4()
        
        for dt in data_types:
            prop = Property(
                concept_id=concept_id,
                property_code=f"test_{dt}",
                property_name=f"Test {dt}",
                data_type=dt
            )
            assert prop.data_type == dt


class TestPropertyConstraint:
    """Tests for PropertyConstraint model."""
    
    def test_constraint_range(self):
        """Test range constraint."""
        property_id = uuid4()
        
        constraint = PropertyConstraint(
            property_id=property_id,
            constraint_type="range",
            constraint_value={"min": -90, "max": 90}
        )
        
        assert constraint.constraint_type == "range"
        assert constraint.constraint_value == {"min": -90, "max": 90}
    
    def test_constraint_unit(self):
        """Test unit constraint."""
        property_id = uuid4()
        
        constraint = PropertyConstraint(
            property_id=property_id,
            constraint_type="unit",
            constraint_value={"unit": "mm"}
        )
        
        assert constraint.constraint_type == "unit"
        assert constraint.constraint_value["unit"] == "mm"
    
    def test_constraint_vocab(self):
        """Test vocabulary constraint."""
        property_id = uuid4()
        
        constraint = PropertyConstraint(
            property_id=property_id,
            constraint_type="vocab",
            constraint_value={"vocab_code": "conservation_status"}
        )
        
        assert constraint.constraint_type == "vocab"


class TestVocab:
    """Tests for Vocab model."""
    
    def test_vocab_creation(self):
        """Test creating a vocabulary."""
        vocab = Vocab(
            vocab_code="conservation_status",
            description="Conservation status of species"
        )
        
        assert vocab.vocab_code == "conservation_status"
        assert vocab.description is not None


class TestVocabTerm:
    """Tests for VocabTerm model."""
    
    def test_vocab_term_creation(self):
        """Test creating a vocabulary term."""
        vocab_id = uuid4()
        
        term = VocabTerm(
            vocab_id=vocab_id,
            term_code="EN",
            term_label="Endangered"
        )
        
        assert term.term_code == "EN"
        assert term.term_label == "Endangered"
    
    def test_vocab_term_hierarchy(self):
        """Test hierarchical vocabulary terms."""
        vocab_id = uuid4()
        parent_id = uuid4()
        
        term = VocabTerm(
            vocab_id=vocab_id,
            term_code="VU",
            term_label="Vulnerable",
            parent_term_id=parent_id
        )
        
        assert term.parent_term_id == parent_id


class TestDataset:
    """Tests for Dataset model."""
    
    def test_dataset_creation(self):
        """Test creating a dataset."""
        dataset = Dataset(
            dataset_code="biodiversity_survey_2024",
            dataset_name="Biodiversity Survey 2024",
            description="Survey data for 2024",
            is_active=True
        )
        
        assert dataset.dataset_code == "biodiversity_survey_2024"
        assert dataset.dataset_name == "Biodiversity Survey 2024"
        assert dataset.is_active is True


class TestDatasetFieldMapping:
    """Tests for DatasetFieldMapping model."""
    
    def test_field_mapping_creation(self):
        """Test creating a field mapping."""
        dataset_id = uuid4()
        concept_id = uuid4()
        property_id = uuid4()
        
        mapping = DatasetFieldMapping(
            dataset_id=dataset_id,
            concept_id=concept_id,
            property_id=property_id,
            external_field_name="species_name",
            external_data_type="VARCHAR(255)",
            is_exposed=True
        )
        
        assert mapping.external_field_name == "species_name"
        assert mapping.is_exposed is True
    
    def test_field_mapping_with_transform(self):
        """Test field mapping with transformation rule."""
        mapping = DatasetFieldMapping(
            dataset_id=uuid4(),
            concept_id=uuid4(),
            property_id=uuid4(),
            external_field_name="rainfall_amount",
            transform_rule={"operation": "cast", "target_type": "float", "unit": "mm"}
        )
        
        assert mapping.transform_rule["operation"] == "cast"
        assert mapping.transform_rule["unit"] == "mm"


class TestDatasetConcept:
    """Tests for DatasetConcept model."""
    
    def test_dataset_concept_creation(self):
        """Test creating a dataset-concept association."""
        dataset_id = uuid4()
        concept_id = uuid4()
        
        dc = DatasetConcept(
            dataset_id=dataset_id,
            concept_id=concept_id
        )
        
        assert dc.dataset_id == dataset_id
        assert dc.concept_id == concept_id


class TestOntologyChangeLog:
    """Tests for OntologyChangeLog model."""
    
    def test_change_log_creation(self):
        """Test creating a change log entry."""
        version_id = uuid4()
        
        change = OntologyChangeLog(
            ontology_version_id=version_id,
            change_type=ChangeTypeEnum.ADD_PROPERTY,
            description="Added scientificName property",
            changed_by="admin@example.com"
        )
        
        assert change.change_type == ChangeTypeEnum.ADD_PROPERTY
        assert change.changed_by == "admin@example.com"
    
    def test_change_log_types(self):
        """Test all change log types."""
        change_types = [
            ChangeTypeEnum.ADD_PROPERTY,
            ChangeTypeEnum.REMOVE_PROPERTY,
            ChangeTypeEnum.DEPRECATE_CONCEPT,
            ChangeTypeEnum.ADD_CONCEPT,
            ChangeTypeEnum.MODIFY_PROPERTY,
            ChangeTypeEnum.ADD_CONSTRAINT,
        ]
        
        assert len(change_types) == 6


class TestModelRelationships:
    """Tests for model relationships."""
    
    def test_domain_has_ontology_versions(self):
        """Test domain relationship with ontology versions."""
        domain = Domain(
            domain_code="test",
            domain_name="Test Domain"
        )
        
        assert hasattr(domain, 'ontology_versions')
        assert isinstance(domain.ontology_versions, list)
    
    def test_ontology_version_has_concepts(self):
        """Test ontology version relationship with concepts."""
        version = OntologyVersion(
            domain_id=uuid4(),
            version_code="v1.0",
            status=StatusEnum.ACTIVE
        )
        
        assert hasattr(version, 'concepts')
        assert isinstance(version.concepts, list)
    
    def test_concept_has_properties(self):
        """Test concept relationship with properties."""
        concept = Concept(
            domain_id=uuid4(),
            ontology_version_id=uuid4(),
            concept_code="Species",
            concept_name="Species"
        )
        
        assert hasattr(concept, 'properties')
        assert isinstance(concept.properties, list)
    
    def test_property_has_constraints(self):
        """Test property relationship with constraints."""
        prop = Property(
            concept_id=uuid4(),
            property_code="test",
            property_name="Test",
            data_type="string"
        )
        
        assert hasattr(prop, 'constraints')
        assert isinstance(prop.constraints, list)
    
    def test_vocab_has_terms(self):
        """Test vocabulary relationship with terms."""
        vocab = Vocab(
            vocab_code="test_vocab",
            description="Test"
        )
        
        assert hasattr(vocab, 'terms')
        assert isinstance(vocab.terms, list)
    
    def test_dataset_has_mappings(self):
        """Test dataset relationship with field mappings."""
        dataset = Dataset(
            dataset_code="test",
            dataset_name="Test Dataset"
        )
        
        assert hasattr(dataset, 'field_mappings')
        assert isinstance(dataset.field_mappings, list)


class TestModelValidation:
    """Tests for model validation and constraints."""
    
    def test_domain_code_required(self):
        """Test that domain_code is required."""
        domain = Domain(domain_name="Test")
        assert domain.domain_code is None
    
    def test_concept_code_unique_per_version(self):
        """Test concept code uniqueness constraint."""
        domain_id = uuid4()
        version_id = uuid4()
        
        concept1 = Concept(
            domain_id=domain_id,
            ontology_version_id=version_id,
            concept_code="Species",
            concept_name="Species"
        )
        
        # Same code should be allowed in different versions
        concept2 = Concept(
            domain_id=domain_id,
            ontology_version_id=uuid4(),
            concept_code="Species",
            concept_name="Species"
        )
        
        assert concept1.concept_code == concept2.concept_code
    
    def test_property_code_unique_per_concept(self):
        """Test property code uniqueness per concept."""
        concept_id = uuid4()
        
        prop1 = Property(
            concept_id=concept_id,
            property_code="name",
            property_name="Name",
            data_type="string"
        )
        
        # Different concept allows same property code
        prop2 = Property(
            concept_id=uuid4(),
            property_code="name",
            property_name="Name",
            data_type="string"
        )
        
        assert prop1.property_code == prop2.property_code
