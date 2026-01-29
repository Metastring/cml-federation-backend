"""Initial ontology schema migration."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = '001_initial_ontology_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create initial ontology tables."""
    
    # Create cml_domain table
    op.create_table(
        'cml_domain',
        sa.Column('domain_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('domain_code', sa.String(50), unique=True, nullable=False, index=True),
        sa.Column('domain_name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('is_active', sa.Boolean, default=True, nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
    )
    
    # Create cml_ontology_version table
    op.create_table(
        'cml_ontology_version',
        sa.Column('ontology_version_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('domain_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_domain.domain_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('version_code', sa.String(50), nullable=False),
        sa.Column('status', sa.Enum('draft', 'active', 'deprecated', name='statusenum'), nullable=False, index=True),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('domain_id', 'version_code', name='uq_domain_version'),
    )
    
    # Create cml_concept table
    op.create_table(
        'cml_concept',
        sa.Column('concept_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('domain_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_domain.domain_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('ontology_version_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_ontology_version.ontology_version_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('concept_code', sa.String(100), nullable=False),
        sa.Column('concept_name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('parent_concept_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_concept.concept_id', ondelete='SET NULL'), nullable=True, index=True),
        sa.Column('is_abstract', sa.Boolean, default=False, nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('domain_id', 'ontology_version_id', 'concept_code', name='uq_concept_code_per_version'),
    )
    op.create_index('idx_concept_code', 'cml_concept', ['concept_code'])
    op.create_index('idx_concept_is_abstract', 'cml_concept', ['is_abstract'])
    
    # Create cml_property table
    op.create_table(
        'cml_property',
        sa.Column('property_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('concept_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_concept.concept_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('property_code', sa.String(100), nullable=False),
        sa.Column('property_name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('data_type', sa.String(50), nullable=False),
        sa.Column('is_required', sa.Boolean, default=False, nullable=False),
        sa.Column('is_multivalued', sa.Boolean, default=False, nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('concept_id', 'property_code', name='uq_property_code_per_concept'),
    )
    op.create_index('idx_property_code', 'cml_property', ['property_code'])
    op.create_index('idx_property_data_type', 'cml_property', ['data_type'])
    
    # Create cml_property_constraint table
    op.create_table(
        'cml_property_constraint',
        sa.Column('constraint_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('property_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_property.property_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('constraint_type', sa.String(50), nullable=False, index=True),
        sa.Column('constraint_value', sa.JSON, nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
    )
    op.create_index('idx_constraint_type', 'cml_property_constraint', ['constraint_type'])
    
    # Create cml_vocab table
    op.create_table(
        'cml_vocab',
        sa.Column('vocab_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('vocab_code', sa.String(100), unique=True, nullable=False, index=True),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
    )
    
    # Create cml_vocab_term table
    op.create_table(
        'cml_vocab_term',
        sa.Column('term_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('vocab_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_vocab.vocab_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('term_code', sa.String(100), nullable=False),
        sa.Column('term_label', sa.String(255), nullable=False),
        sa.Column('parent_term_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_vocab_term.term_id', ondelete='SET NULL'), nullable=True, index=True),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('vocab_id', 'term_code', name='uq_term_code_per_vocab'),
    )
    op.create_index('idx_vocab_term_code', 'cml_vocab_term', ['term_code'])
    
    # Create cml_dataset table
    op.create_table(
        'cml_dataset',
        sa.Column('dataset_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('dataset_code', sa.String(100), unique=True, nullable=False, index=True),
        sa.Column('dataset_name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text, nullable=True),
        sa.Column('is_active', sa.Boolean, default=True, nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
    )
    op.create_index('idx_dataset_is_active', 'cml_dataset', ['is_active'])
    
    # Create cml_dataset_field_mapping table
    op.create_table(
        'cml_dataset_field_mapping',
        sa.Column('mapping_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('dataset_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_dataset.dataset_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('concept_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_concept.concept_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('property_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_property.property_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('external_field_name', sa.String(255), nullable=False),
        sa.Column('external_data_type', sa.String(50), nullable=True),
        sa.Column('transform_rule', sa.JSON, nullable=True),
        sa.Column('is_exposed', sa.Boolean, default=True, nullable=False),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('dataset_id', 'external_field_name', name='uq_dataset_field_mapping'),
    )
    op.create_index('idx_dataset_field_mapping_dataset', 'cml_dataset_field_mapping', ['dataset_id'])
    op.create_index('idx_dataset_field_mapping_property', 'cml_dataset_field_mapping', ['property_id'])
    
    # Create cml_dataset_concept table
    op.create_table(
        'cml_dataset_concept',
        sa.Column('dataset_concept_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('dataset_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_dataset.dataset_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('concept_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_concept.concept_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('created_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('dataset_id', 'concept_id', name='uq_dataset_concept'),
    )
    op.create_index('idx_dataset_concept_concept', 'cml_dataset_concept', ['concept_id'])
    
    # Create cml_ontology_change_log table
    op.create_table(
        'cml_ontology_change_log',
        sa.Column('change_id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('ontology_version_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('cml_ontology_version.ontology_version_id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('change_type', sa.Enum('add_property', 'remove_property', 'deprecate_concept', 'add_concept', 'modify_property', 'add_constraint', name='changetypeenum'), nullable=False, index=True),
        sa.Column('description', sa.Text, nullable=False),
        sa.Column('changed_by', sa.String(255), nullable=True),
        sa.Column('changed_at', sa.DateTime, nullable=False, server_default=sa.func.now()),
    )
    op.create_index('idx_change_log_changed_at', 'cml_ontology_change_log', ['changed_at'])


def downgrade() -> None:
    """Drop all ontology tables."""
    
    op.drop_table('cml_ontology_change_log')
    op.drop_table('cml_dataset_concept')
    op.drop_table('cml_dataset_field_mapping')
    op.drop_table('cml_dataset')
    op.drop_table('cml_vocab_term')
    op.drop_table('cml_vocab')
    op.drop_table('cml_property_constraint')
    op.drop_table('cml_property')
    op.drop_table('cml_concept')
    op.drop_table('cml_ontology_version')
    op.drop_table('cml_domain')
    
    # Drop enums
    op.execute('DROP TYPE IF EXISTS statusenum CASCADE')
    op.execute('DROP TYPE IF EXISTS changetypeenum CASCADE')
