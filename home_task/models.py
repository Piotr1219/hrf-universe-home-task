from dataclasses import dataclass
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Table,
    Float,
)
from sqlalchemy.orm import registry

mapper_registry = registry()


class Model:
    pass


@mapper_registry.mapped
@dataclass
class StandardJobFamily(Model):
    __table__ = Table(
        "standard_job_family",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        schema="public",
    )

    id: str
    name: str


@mapper_registry.mapped
@dataclass
class StandardJob(Model):
    __table__ = Table(
        "standard_job",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        Column("standard_job_family_id", String, nullable=False),
        schema="public",
    )

    id: str
    name: str
    standard_job_family_id: str


@mapper_registry.mapped
@dataclass
class JobPosting(Model):
    __table__ = Table(
        "job_posting",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("title", String, nullable=False),
        Column("standard_job_id", String, nullable=False),
        Column("country_code", String, nullable=True),
        Column("days_to_hire", Integer, nullable=True),
        schema="public",
    )

    id: str
    title: str
    standard_job_id: str
    country_code: Optional[str] = None
    days_to_hire: Optional[int] = None


@mapper_registry.mapped
@dataclass
class DaysToHire(Model):
    __table__ = Table(
        "days_to_hire",
        mapper_registry.metadata,
        Column("id", Integer, nullable=False, primary_key=True),
        Column("country_code", String, nullable=False),
        Column("standard_job_id", String, nullable=False),
        Column("average_days_to_hire", Float, nullable=True),
        Column("minimum_days_to_hire", Float, nullable=True),
        Column("maximum_days_to_hire", Float, nullable=True),
        Column("number_of_job_postings", Integer, nullable=True),
        schema="public",
    )

    id: int
    country_code: str
    standard_job_id: str
    average_days_to_hire: Optional[float] = None
    minimum_days_to_hire: Optional[float] = None
    maximum_days_to_hire: Optional[float] = None
    number_of_job_postings: Optional[int] = None
