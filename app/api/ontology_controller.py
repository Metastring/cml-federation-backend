"""Ontology API endpoints controller."""
from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.core.database import get_db, get_connection
from app.repositories.ontology_repository import (
    DomainRepository,
    OntologyVersionRepository,
    ConceptRepository,
    PropertyRepository,
    VocabularyRepository,
    VocabularyTermRepository,
    DatasetFieldMappingRepository,
    DatasetConceptRepository,
)
from app.schemas.ontology_pydantic import (
    DomainCreateSchema,
    DomainResponseSchema,
    OntologyVersionCreateSchema,
    OntologyVersionResponseSchema,
    ConceptCreateSchema,
    ConceptResponseSchema,
    PropertyCreateSchema,
    PropertyResponseSchema,
    VocabularyCreateSchema,
    VocabularyResponseSchema,
    VocabularyTermSchema,
)

router = APIRouter(prefix="/api/v1/ontology", tags=["ontology"])


# Legacy endpoint
@router.get("/ontology-list")
async def get_ontology_list():
    """
    Returns all unique ontology_mapping_to_display values from the dataset_mapping table.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ontology_mapping_to_display FROM dataset_mapping WHERE ontology_mapping_to_display IS NOT NULL")
        rows = cursor.fetchall()
        unique_ontologies = sorted({row[0] for row in rows if row[0]})
        cursor.close()
        conn.close()
        return JSONResponse(content={"ontology_list": unique_ontologies})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Domain Endpoints
@router.post("/domains", response_model=DomainResponseSchema, status_code=status.HTTP_201_CREATED)
def create_domain(domain_request: DomainCreateSchema, db: Session = Depends(get_db)):
    """Create a new domain."""
    repo = DomainRepository(db)
    
    # Check if domain code already exists
    existing = repo.get_by_code(domain_request.domain_code)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Domain with code '{domain_request.domain_code}' already exists"
        )
    
    domain = repo.create(
        domain_code=domain_request.domain_code,
        domain_name=domain_request.domain_name,
        description=domain_request.description
    )
    db.commit()
    
    return domain


@router.get("/domains", response_model=List[DomainResponseSchema])
def list_domains(db: Session = Depends(get_db)):
    """List all domains."""
    repo = DomainRepository(db)
    return repo.list_all()


@router.get("/domains/{domain_id}", response_model=DomainResponseSchema)
def get_domain(domain_id: UUID, db: Session = Depends(get_db)):
    """Get domain by ID."""
    repo = DomainRepository(db)
    domain = repo.get_by_id(domain_id)
    
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain with id {domain_id} not found"
        )
    
    return domain


@router.get("/domains/code/{code}", response_model=DomainResponseSchema)
def get_domain_by_code(code: str, db: Session = Depends(get_db)):
    """Get domain by code."""
    repo = DomainRepository(db)
    domain = repo.get_by_code(code)
    
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain with code '{code}' not found"
        )
    
    return domain


@router.patch("/domains/{domain_id}", response_model=DomainResponseSchema)
def update_domain(domain_id: UUID, domain_request: DomainCreateSchema, db: Session = Depends(get_db)):
    """Update a domain."""
    repo = DomainRepository(db)
    
    domain = repo.get_by_id(domain_id)
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain with id {domain_id} not found"
        )
    
    updated = repo.update(
        domain_id,
        domain_name=domain_request.domain_name,
        description=domain_request.description
    )
    db.commit()
    
    return updated


@router.delete("/domains/{domain_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_domain(domain_id: UUID, db: Session = Depends(get_db)):
    """Delete a domain."""
    repo = DomainRepository(db)
    
    domain = repo.get_by_id(domain_id)
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain with id {domain_id} not found"
        )
    
    repo.delete(domain_id)
    db.commit()


# Ontology Version Endpoints
@router.post("/domains/{domain_id}/versions", response_model=OntologyVersionResponseSchema, status_code=status.HTTP_201_CREATED)
def create_ontology_version(
    domain_id: UUID,
    version_request: OntologyVersionCreateSchema,
    db: Session = Depends(get_db)
):
    """Create a new ontology version for a domain."""
    # Verify domain exists
    domain_repo = DomainRepository(db)
    domain = domain_repo.get_by_id(domain_id)
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain with id {domain_id} not found"
        )
    
    version_repo = OntologyVersionRepository(db)
    version = version_repo.create(
        domain_id=domain_id,
        version_code=version_request.version_code,
        description=version_request.description
    )
    db.commit()
    
    return version


@router.get("/domains/{domain_id}/versions", response_model=List[OntologyVersionResponseSchema])
def list_domain_versions(domain_id: UUID, db: Session = Depends(get_db)):
    """List all versions for a domain."""
    domain_repo = DomainRepository(db)
    if not domain_repo.get_by_id(domain_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain with id {domain_id} not found"
        )
    
    version_repo = OntologyVersionRepository(db)
    return version_repo.list_by_domain(domain_id)


@router.get("/domains/{domain_id}/versions/active", response_model=OntologyVersionResponseSchema)
def get_active_version(domain_id: UUID, db: Session = Depends(get_db)):
    """Get active version for a domain."""
    domain_repo = DomainRepository(db)
    if not domain_repo.get_by_id(domain_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Domain with id {domain_id} not found"
        )
    
    version_repo = OntologyVersionRepository(db)
    version = version_repo.get_active_version(domain_id)
    
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active version found for domain {domain_id}"
        )
    
    return version


@router.get("/versions/{version_id}", response_model=OntologyVersionResponseSchema)
def get_version(version_id: UUID, db: Session = Depends(get_db)):
    """Get ontology version by ID."""
    repo = OntologyVersionRepository(db)
    version = repo.get_by_id(version_id)
    
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version with id {version_id} not found"
        )
    
    return version


# Concept Endpoints
@router.post("/versions/{version_id}/concepts", response_model=ConceptResponseSchema, status_code=status.HTTP_201_CREATED)
def create_concept(
    version_id: UUID,
    concept_request: ConceptCreateSchema,
    db: Session = Depends(get_db)
):
    """Create a new concept."""
    # Verify version exists
    version_repo = OntologyVersionRepository(db)
    version = version_repo.get_by_id(version_id)
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ontology version with id {version_id} not found"
        )
    
    concept_repo = ConceptRepository(db)
    concept = concept_repo.create(
        ontology_version_id=version_id,
        concept_code=concept_request.concept_code,
        concept_name=concept_request.concept_name,
        description=concept_request.description,
        parent_concept_id=concept_request.parent_concept_id,
        is_abstract=concept_request.is_abstract
    )
    db.commit()
    
    return concept


@router.get("/versions/{version_id}/concepts", response_model=List[ConceptResponseSchema])
def list_concepts(version_id: UUID, db: Session = Depends(get_db)):
    """List all concepts for a version."""
    version_repo = OntologyVersionRepository(db)
    if not version_repo.get_by_id(version_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ontology version with id {version_id} not found"
        )
    
    concept_repo = ConceptRepository(db)
    return concept_repo.list_by_version(version_id)


@router.get("/concepts/{concept_id}", response_model=ConceptResponseSchema)
def get_concept(concept_id: UUID, db: Session = Depends(get_db)):
    """Get concept by ID."""
    repo = ConceptRepository(db)
    concept = repo.get_by_id(concept_id)
    
    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with id {concept_id} not found"
        )
    
    return concept


# Property Endpoints
@router.post("/concepts/{concept_id}/properties", response_model=PropertyResponseSchema, status_code=status.HTTP_201_CREATED)
def create_property(
    concept_id: UUID,
    property_request: PropertyCreateSchema,
    db: Session = Depends(get_db)
):
    """Create a new property for a concept."""
    # Verify concept exists
    concept_repo = ConceptRepository(db)
    concept = concept_repo.get_by_id(concept_id)
    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with id {concept_id} not found"
        )
    
    property_repo = PropertyRepository(db)
    prop = property_repo.create(
        concept_id=concept_id,
        property_code=property_request.property_code,
        property_name=property_request.property_name,
        data_type=property_request.data_type,
        is_required=property_request.is_required,
        is_multivalued=property_request.is_multivalued
    )
    db.commit()
    
    return prop


@router.get("/concepts/{concept_id}/properties", response_model=List[PropertyResponseSchema])
def list_properties(concept_id: UUID, db: Session = Depends(get_db)):
    """List all properties for a concept."""
    concept_repo = ConceptRepository(db)
    if not concept_repo.get_by_id(concept_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with id {concept_id} not found"
        )
    
    property_repo = PropertyRepository(db)
    return property_repo.list_by_concept(concept_id)


@router.get("/properties/{property_id}", response_model=PropertyResponseSchema)
def get_property(property_id: UUID, db: Session = Depends(get_db)):
    """Get property by ID."""
    repo = PropertyRepository(db)
    prop = repo.get_by_id(property_id)
    
    if not prop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with id {property_id} not found"
        )
    
    return prop


# Vocabulary Endpoints
@router.post("/vocabularies", response_model=VocabularyResponseSchema, status_code=status.HTTP_201_CREATED)
def create_vocabulary(vocab_request: VocabularyCreateSchema, db: Session = Depends(get_db)):
    """Create a new vocabulary."""
    repo = VocabularyRepository(db)
    
    # Check if vocab code already exists
    existing = repo.get_by_code(vocab_request.vocab_code)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Vocabulary with code '{vocab_request.vocab_code}' already exists"
        )
    
    vocab = repo.create(
        vocab_code=vocab_request.vocab_code,
        description=vocab_request.description
    )
    db.commit()
    
    return vocab


@router.get("/vocabularies", response_model=List[VocabularyResponseSchema])
def list_vocabularies(db: Session = Depends(get_db)):
    """List all vocabularies."""
    repo = VocabularyRepository(db)
    return repo.list_all()


@router.get("/vocabularies/{vocab_id}", response_model=VocabularyResponseSchema)
def get_vocabulary(vocab_id: UUID, db: Session = Depends(get_db)):
    """Get vocabulary by ID."""
    repo = VocabularyRepository(db)
    vocab = repo.get_by_id(vocab_id)
    
    if not vocab:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Vocabulary with id {vocab_id} not found"
        )
    
    return vocab


@router.get("/vocabularies/code/{code}", response_model=VocabularyResponseSchema)
def get_vocabulary_by_code(code: str, db: Session = Depends(get_db)):
    """Get vocabulary by code."""
    repo = VocabularyRepository(db)
    vocab = repo.get_by_code(code)
    
    if not vocab:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Vocabulary with code '{code}' not found"
        )
    
    return vocab


# Vocabulary Term Endpoints
@router.post("/vocabularies/{vocab_id}/terms", response_model=VocabularyTermSchema, status_code=status.HTTP_201_CREATED)
def create_vocabulary_term(
    vocab_id: UUID,
    term_request: VocabularyTermSchema,
    db: Session = Depends(get_db)
):
    """Create a new vocabulary term."""
    # Verify vocabulary exists
    vocab_repo = VocabularyRepository(db)
    vocab = vocab_repo.get_by_id(vocab_id)
    if not vocab:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Vocabulary with id {vocab_id} not found"
        )
    
    term_repo = VocabularyTermRepository(db)
    term = term_repo.create(
        vocab_id=vocab_id,
        term_code=term_request.term_code,
        term_label=term_request.term_label,
        description=term_request.description,
        parent_term_id=term_request.parent_term_id
    )
    db.commit()
    
    return term


@router.get("/vocabularies/{vocab_id}/terms", response_model=List[VocabularyTermSchema])
def list_vocabulary_terms(vocab_id: UUID, db: Session = Depends(get_db)):
    """List all terms for a vocabulary."""
    vocab_repo = VocabularyRepository(db)
    if not vocab_repo.get_by_id(vocab_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Vocabulary with id {vocab_id} not found"
        )
    
    term_repo = VocabularyTermRepository(db)
    return term_repo.list_by_vocabulary(vocab_id)


@router.get("/terms/{term_id}", response_model=VocabularyTermSchema)
def get_vocabulary_term(term_id: UUID, db: Session = Depends(get_db)):
    """Get vocabulary term by ID."""
    repo = VocabularyTermRepository(db)
    term = repo.get_by_id(term_id)
    
    if not term:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Vocabulary term with id {term_id} not found"
        )
    
    return term
