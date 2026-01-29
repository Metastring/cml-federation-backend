"""Integration tests for ontology models and repositories."""
import pytest
from uuid import uuid4
from sqlalchemy.orm import Session

from app.models.ontology_model import (
    Domain,
    OntologyVersion,
    Concept,
    Property,
    PropertyConstraint,
    Vocabulary,
    VocabularyTerm,
)
from app.repositories.ontology_repository import (
    DomainRepository,
    OntologyVersionRepository,
    ConceptRepository,
    PropertyRepository,
    PropertyConstraintRepository,
    VocabularyRepository,
    VocabularyTermRepository,
)


@pytest.fixture
def db_session(test_config):
    """Create a test database session."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models.ontology_model import Base
    
    engine = create_engine(test_config["database_url"])
    
    # Create tables
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    session.close()
    Base.metadata.drop_all(engine)


class TestDomainIntegration:
    """Integration tests for Domain repository."""

    def test_create_and_retrieve_domain(self, db_session):
        """Test creating and retrieving domain."""
        repo = DomainRepository(db_session)
        
        domain = repo.create("biology", "Biology Domain", "Bio domain")
        db_session.commit()
        
        retrieved = repo.get_by_id(domain.domain_id)
        assert retrieved.domain_code == "biology"
        assert retrieved.is_active is True

    def test_list_domains(self, db_session):
        """Test listing domains."""
        repo = DomainRepository(db_session)
        
        repo.create("bio", "Biology")
        repo.create("chem", "Chemistry")
        db_session.commit()
        
        all_domains = repo.list_all()
        assert len(all_domains) >= 2


class TestOntologyVersionIntegration:
    """Integration tests for OntologyVersion."""

    def test_create_versions_for_domain(self, db_session):
        """Test creating multiple versions."""
        domain_repo = DomainRepository(db_session)
        version_repo = OntologyVersionRepository(db_session)
        
        domain = domain_repo.create("test", "Test Domain")
        db_session.commit()
        
        v1 = version_repo.create(domain.domain_id, "v1.0", "draft")
        v2 = version_repo.create(domain.domain_id, "v2.0", "draft")
        db_session.commit()
        
        versions = version_repo.list_by_domain(domain.domain_id)
        assert len(versions) == 2


class TestConceptHierarchy:
    """Integration tests for concept hierarchies."""

    def test_parent_child_concepts(self, db_session):
        """Test parent-child relationships."""
        domain_repo = DomainRepository(db_session)
        version_repo = OntologyVersionRepository(db_session)
        concept_repo = ConceptRepository(db_session)
        
        domain = domain_repo.create("hierarchy", "Hierarchy Test")
        version = version_repo.create(domain.domain_id, "v1.0", "draft")
        db_session.commit()
        
        parent = concept_repo.create(
            version.ontology_version_id,
            "Organism",
            "Organism",
            is_abstract=True
        )
        db_session.commit()
        
        child = concept_repo.create(
            version.ontology_version_id,
            "Animal",
            "Animal",
            parent_concept_id=parent.concept_id
        )
        db_session.commit()
        
        assert child.parent_concept_id == parent.concept_id


class TestPropertyConstraints:
    """Integration tests for constraints."""

    def test_multiple_constraints(self, db_session):
        """Test multiple constraints on property."""
        domain_repo = DomainRepository(db_session)
        version_repo = OntologyVersionRepository(db_session)
        concept_repo = ConceptRepository(db_session)
        property_repo = PropertyRepository(db_session)
        constraint_repo = PropertyConstraintRepository(db_session)
        
        domain = domain_repo.create("constraints", "Constraints Test")
        version = version_repo.create(domain.domain_id, "v1.0", "draft")
        concept = concept_repo.create(version.ontology_version_id, "Location", "Location")
        db_session.commit()
        
        prop = property_repo.create(concept.concept_id, "lat", "Latitude", "number")
        db_session.commit()
        
        c1 = constraint_repo.create(prop.property_id, "range", {"min": -90, "max": 90})
        c2 = constraint_repo.create(prop.property_id, "unit", {"unit": "degrees"})
        db_session.commit()
        
        constraints = constraint_repo.list_by_property(prop.property_id)
        assert len(constraints) == 2


class TestVocabularyHierarchy:
    """Integration tests for vocabulary hierarchies."""

    def test_vocabulary_with_terms(self, db_session):
        """Test vocabulary terms."""
        vocab_repo = VocabularyRepository(db_session)
        term_repo = VocabularyTermRepository(db_session)
        
        vocab = vocab_repo.create("status", "Status Vocabulary")
        db_session.commit()
        
        root = term_repo.create(vocab.vocab_id, "ROOT", "Root")
        child1 = term_repo.create(vocab.vocab_id, "CHILD1", "Child 1", parent_term_id=root.vocab_term_id)
        child2 = term_repo.create(vocab.vocab_id, "CHILD2", "Child 2", parent_term_id=root.vocab_term_id)
        db_session.commit()
        
        terms = term_repo.list_by_vocabulary(vocab.vocab_id)
        assert len(terms) == 3


class TestCascadeDelete:
    """Integration tests for cascade deletes."""

    def test_delete_domain_cascades(self, db_session):
        """Test cascade delete on domain."""
        domain_repo = DomainRepository(db_session)
        version_repo = OntologyVersionRepository(db_session)
        
        domain = domain_repo.create("cascade", "Cascade Test")
        db_session.commit()
        domain_id = domain.domain_id
        
        version = version_repo.create(domain_id, "v1.0", "draft")
        db_session.commit()
        version_id = version.ontology_version_id
        
        domain_repo.delete(domain_id)
        db_session.commit()
        
        # Version should be deleted
        retrieved = version_repo.get_by_id(version_id)
        assert retrieved is None
