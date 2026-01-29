"""Integration tests for ontology models with real PostgreSQL."""
import pytest
from uuid import uuid4
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

from app.models import (
    Base,
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


@pytest.fixture
def in_memory_db():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine(
        'sqlite:///:memory:',
        connect_args={'check_same_thread': False},
        poolclass=StaticPool
    )
    
    # Enable foreign keys for SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
    
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    yield SessionLocal
    
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def db_session(in_memory_db):
    """Provide a database session for tests."""
    session = in_memory_db()
    yield session
    session.rollback()
    session.close()


class TestDomainIntegration:
    """Integration tests for Domain model."""
    
    def test_create_and_retrieve_domain(self, db_session):
        """Test creating and retrieving a domain."""
        domain = Domain(
            domain_code="biodiversity",
            domain_name="Biodiversity",
            description="Biodiversity domain",
            is_active=True
        )
        
        db_session.add(domain)
        db_session.commit()
        
        retrieved = db_session.query(Domain).filter_by(
            domain_code="biodiversity"
        ).first()
        
        assert retrieved is not None
        assert retrieved.domain_name == "Biodiversity"
        assert retrieved.is_active is True
    
    def test_domain_with_versions(self, db_session):
        """Test domain with ontology versions."""
        domain = Domain(
            domain_code="climate",
            domain_name="Climate"
        )
        
        version1 = OntologyVersion(
            domain_id=None,  # Will be set by relationship
            version_code="v1.0",
            status=StatusEnum.ACTIVE
        )
        
        version2 = OntologyVersion(
            domain_id=None,
            version_code="v2.0",
            status=StatusEnum.DRAFT
        )
        
        domain.ontology_versions.append(version1)
        domain.ontology_versions.append(version2)
        
        db_session.add(domain)
        db_session.commit()
        
        retrieved = db_session.query(Domain).filter_by(
            domain_code="climate"
        ).first()
        
        assert len(retrieved.ontology_versions) == 2
        assert retrieved.ontology_versions[0].version_code == "v1.0"


class TestOntologyVersionIntegration:
    """Integration tests for OntologyVersion model."""
    
    def test_version_status_transitions(self, db_session):
        """Test ontology version status transitions."""
        domain = Domain(domain_code="test", domain_name="Test")
        db_session.add(domain)
        db_session.flush()
        
        version = OntologyVersion(
            domain_id=domain.domain_id,
            version_code="v1.0",
            status=StatusEnum.DRAFT
        )
        
        db_session.add(version)
        db_session.commit()
        
        version.status = StatusEnum.ACTIVE
        db_session.commit()
        
        retrieved = db_session.query(OntologyVersion).filter_by(
            version_code="v1.0"
        ).first()
        
        assert retrieved.status == StatusEnum.ACTIVE
    
    def test_unique_version_per_domain(self, db_session):
        """Test unique constraint on domain + version_code."""
        domain = Domain(domain_code="test", domain_name="Test")
        db_session.add(domain)
        db_session.flush()
        
        v1 = OntologyVersion(
            domain_id=domain.domain_id,
            version_code="v1.0",
            status=StatusEnum.ACTIVE
        )
        db_session.add(v1)
        db_session.commit()
        
        # Attempting to add same version code for same domain should fail
        v2 = OntologyVersion(
            domain_id=domain.domain_id,
            version_code="v1.0",
            status=StatusEnum.DRAFT
        )
        db_session.add(v2)
        
        with pytest.raises(Exception):
            db_session.commit()


class TestConceptIntegration:
    """Integration tests for Concept model."""
    
    def test_create_concept_hierarchy(self, db_session):
        """Test creating a hierarchy of concepts."""
        domain = Domain(domain_code="bio", domain_name="Bio")
        version = OntologyVersion(
            version_code="v1.0",
            status=StatusEnum.ACTIVE
        )
        domain.ontology_versions.append(version)
        db_session.add(domain)
        db_session.flush()
        
        parent = Concept(
            domain_id=domain.domain_id,
            ontology_version_id=version.ontology_version_id,
            concept_code="Organism",
            concept_name="Organism",
            is_abstract=True
        )
        db_session.add(parent)
        db_session.flush()
        
        child = Concept(
            domain_id=domain.domain_id,
            ontology_version_id=version.ontology_version_id,
            concept_code="Species",
            concept_name="Species",
            parent_concept_id=parent.concept_id,
            is_abstract=False
        )
        db_session.add(child)
        db_session.commit()
        
        retrieved_child = db_session.query(Concept).filter_by(
            concept_code="Species"
        ).first()
        
        assert retrieved_child.parent_concept_id == parent.concept_id


class TestPropertyIntegration:
    """Integration tests for Property model."""
    
    def test_concept_with_properties(self, db_session):
        """Test concept with multiple properties."""
        domain = Domain(domain_code="bio", domain_name="Bio")
        version = OntologyVersion(version_code="v1.0", status=StatusEnum.ACTIVE)
        domain.ontology_versions.append(version)
        
        concept = Concept(
            domain_id=None,
            ontology_version_id=None,
            concept_code="Species",
            concept_name="Species"
        )
        version.concepts.append(concept)
        
        prop1 = Property(
            concept_id=None,
            property_code="scientificName",
            property_name="Scientific Name",
            data_type="string",
            is_required=True
        )
        prop2 = Property(
            concept_id=None,
            property_code="conservationStatus",
            property_name="Conservation Status",
            data_type="string"
        )
        
        concept.properties.append(prop1)
        concept.properties.append(prop2)
        
        db_session.add(domain)
        db_session.commit()
        
        retrieved = db_session.query(Concept).filter_by(
            concept_code="Species"
        ).first()
        
        assert len(retrieved.properties) == 2


class TestConstraintIntegration:
    """Integration tests for PropertyConstraint model."""
    
    def test_property_with_constraints(self, db_session):
        """Test property with multiple constraints."""
        domain = Domain(domain_code="test", domain_name="Test")
        version = OntologyVersion(version_code="v1.0", status=StatusEnum.ACTIVE)
        domain.ontology_versions.append(version)
        
        concept = Concept(
            domain_id=None,
            ontology_version_id=None,
            concept_code="Measurement",
            concept_name="Measurement"
        )
        version.concepts.append(concept)
        
        prop = Property(
            concept_id=None,
            property_code="latitude",
            property_name="Latitude",
            data_type="number"
        )
        concept.properties.append(prop)
        
        db_session.add(domain)
        db_session.flush()
        
        # Add constraints
        range_constraint = PropertyConstraint(
            property_id=prop.property_id,
            constraint_type="range",
            constraint_value={"min": -90, "max": 90}
        )
        db_session.add(range_constraint)
        db_session.commit()
        
        retrieved_prop = db_session.query(Property).filter_by(
            property_code="latitude"
        ).first()
        
        assert len(retrieved_prop.constraints) == 1
        assert retrieved_prop.constraints[0].constraint_value["min"] == -90


class TestVocabIntegration:
    """Integration tests for Vocab and VocabTerm models."""
    
    def test_vocab_with_hierarchical_terms(self, db_session):
        """Test vocabulary with hierarchical terms."""
        vocab = Vocab(
            vocab_code="conservation_status",
            description="Conservation Status"
        )
        
        # Create parent terms
        extinct = VocabTerm(
            vocab_id=None,
            term_code="EX",
            term_label="Extinct"
        )
        vocab.terms.append(extinct)
        
        endangered = VocabTerm(
            vocab_id=None,
            term_code="EN",
            term_label="Endangered"
        )
        vocab.terms.append(endangered)
        
        db_session.add(vocab)
        db_session.commit()
        
        retrieved = db_session.query(Vocab).filter_by(
            vocab_code="conservation_status"
        ).first()
        
        assert len(retrieved.terms) == 2
        assert any(t.term_code == "EN" for t in retrieved.terms)


class TestDatasetFieldMappingIntegration:
    """Integration tests for DatasetFieldMapping model."""
    
    def test_complete_mapping_scenario(self, db_session):
        """Test complete dataset to ontology mapping scenario."""
        # Create domain and version
        domain = Domain(domain_code="bio", domain_name="Biodiversity")
        version = OntologyVersion(version_code="v1.0", status=StatusEnum.ACTIVE)
        domain.ontology_versions.append(version)
        
        # Create concept and property
        concept = Concept(
            domain_id=None,
            ontology_version_id=None,
            concept_code="SpeciesObservation",
            concept_name="Species Observation"
        )
        version.concepts.append(concept)
        
        prop = Property(
            concept_id=None,
            property_code="scientificName",
            property_name="Scientific Name",
            data_type="string"
        )
        concept.properties.append(prop)
        
        # Create dataset
        dataset = Dataset(
            dataset_code="survey_2024",
            dataset_name="Survey 2024"
        )
        
        db_session.add(domain)
        db_session.add(dataset)
        db_session.flush()
        
        # Create mapping
        mapping = DatasetFieldMapping(
            dataset_id=dataset.dataset_id,
            concept_id=concept.concept_id,
            property_id=prop.property_id,
            external_field_name="species_name",
            external_data_type="VARCHAR(255)"
        )
        db_session.add(mapping)
        db_session.commit()
        
        retrieved_mapping = db_session.query(DatasetFieldMapping).filter_by(
            external_field_name="species_name"
        ).first()
        
        assert retrieved_mapping is not None
        assert retrieved_mapping.dataset_id == dataset.dataset_id


class TestChangeLogIntegration:
    """Integration tests for OntologyChangeLog model."""
    
    def test_track_ontology_changes(self, db_session):
        """Test tracking ontology changes."""
        domain = Domain(domain_code="test", domain_name="Test")
        version = OntologyVersion(version_code="v1.0", status=StatusEnum.DRAFT)
        domain.ontology_versions.append(version)
        
        db_session.add(domain)
        db_session.flush()
        
        # Log changes
        change1 = OntologyChangeLog(
            ontology_version_id=version.ontology_version_id,
            change_type=ChangeTypeEnum.ADD_CONCEPT,
            description="Added Species concept",
            changed_by="admin"
        )
        
        change2 = OntologyChangeLog(
            ontology_version_id=version.ontology_version_id,
            change_type=ChangeTypeEnum.ADD_PROPERTY,
            description="Added scientificName property",
            changed_by="admin"
        )
        
        db_session.add(change1)
        db_session.add(change2)
        db_session.commit()
        
        retrieved_version = db_session.query(OntologyVersion).filter_by(
            version_code="v1.0"
        ).first()
        
        assert len(retrieved_version.change_logs) == 2


class TestDatasetConceptIntegration:
    """Integration tests for DatasetConcept model."""
    
    def test_dataset_exposes_multiple_concepts(self, db_session):
        """Test dataset exposing multiple ontology concepts."""
        domain = Domain(domain_code="bio", domain_name="Bio")
        version = OntologyVersion(version_code="v1.0", status=StatusEnum.ACTIVE)
        domain.ontology_versions.append(version)
        
        concept1 = Concept(
            domain_id=None,
            ontology_version_id=None,
            concept_code="Species",
            concept_name="Species"
        )
        concept2 = Concept(
            domain_id=None,
            ontology_version_id=None,
            concept_code="Observation",
            concept_name="Observation"
        )
        version.concepts.append(concept1)
        version.concepts.append(concept2)
        
        dataset = Dataset(dataset_code="survey", dataset_name="Survey")
        
        db_session.add(domain)
        db_session.add(dataset)
        db_session.flush()
        
        # Associate dataset with concepts
        dc1 = DatasetConcept(
            dataset_id=dataset.dataset_id,
            concept_id=concept1.concept_id
        )
        dc2 = DatasetConcept(
            dataset_id=dataset.dataset_id,
            concept_id=concept2.concept_id
        )
        
        db_session.add(dc1)
        db_session.add(dc2)
        db_session.commit()
        
        retrieved_dataset = db_session.query(Dataset).filter_by(
            dataset_code="survey"
        ).first()
        
        assert len(retrieved_dataset.dataset_concepts) == 2
