from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import text
from db import get_session

"""
In order to use this API, first navigate to home_task directory and run command:
uvicorn main_api:app --reload

Then the requests can be sent.
eg.
GET http://localhost:8000/days_to_hire?standard_job_id=c83e576e-fa9a-4aef-afb3-f495fca9a6bb&country_code=DE
"""

app = FastAPI()

class DaysToHireResponse(BaseModel):
    standard_job_id: str
    country_code: str
    min_days: float
    avg_days: float
    max_days: float
    job_postings_number: int


@app.get("/days_to_hire", response_model=DaysToHireResponse)
def get_days_to_hire(
    standard_job_id: str = Query(..., description="Standard Job ID"),
    country_code: Optional[str] = Query(None, description="Country code (optional, defaults to world)"),
):
    session = get_session()
    connection = session.connection()

    try:
        query = text("""
            SELECT
                standard_job_id,
                country_code,
                minimum_days_to_hire,
                average_days_to_hire,
                maximum_days_to_hire,
                number_of_job_postings
            FROM public.days_to_hire
            WHERE standard_job_id = :job_id
              AND country_code = :country_code
            LIMIT 1
        """)

        target_country = country_code if country_code is not None else 'WORLD'
        result = connection.execute(query, {
            "job_id": standard_job_id,
            "country_code": target_country
        }).first()

        if not result:
            raise HTTPException(status_code=404, detail="Statistics not found")

        return DaysToHireResponse(
            standard_job_id=result.standard_job_id,
            country_code=result.country_code,
            min_days=result.minimum_days_to_hire,
            avg_days=result.average_days_to_hire,
            max_days=result.maximum_days_to_hire,
            job_postings_number=result.number_of_job_postings,
        )

    finally:
        session.close()
