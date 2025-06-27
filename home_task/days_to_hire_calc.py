from sqlalchemy import text
from home_task.db import get_session


def calculate_days_to_hire(min_postings=5):
    session = get_session()
    connection = session.connection()

    try:
        session.execute(text("DELETE FROM public.days_to_hire"))

        # all job-country combinations
        key_rows = connection.execute(text("""
            SELECT DISTINCT standard_job_id, country_code
            FROM public.job_posting
            WHERE days_to_hire IS NOT NULL AND country_code IS NOT NULL
        """))

        world_keys = connection.execute(text("""
            SELECT DISTINCT standard_job_id
            FROM public.job_posting
            WHERE days_to_hire IS NOT NULL
        """))

        all_keys = list(key_rows) + [(row[0], None) for row in world_keys]

        insert_id = 0
        inserted = 0

        for standard_job_id, country_code in all_keys:
            # percentiles
            if country_code is None:
                threshold_query = text("""
                    SELECT
                        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY days_to_hire) AS p10,
                        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_to_hire) AS p90
                    FROM public.job_posting
                    WHERE standard_job_id = :job_id
                      AND days_to_hire IS NOT NULL
                """)
                filter_params = {'job_id': standard_job_id}
                target_country = 'WORLD'
            else:
                threshold_query = text("""
                    SELECT
                        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY days_to_hire) AS p10,
                        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_to_hire) AS p90
                    FROM public.job_posting
                    WHERE standard_job_id = :job_id
                      AND country_code = :country
                      AND days_to_hire IS NOT NULL
                """)
                filter_params = {'job_id': standard_job_id, 'country': country_code}
                target_country = country_code

            result = connection.execute(threshold_query, filter_params).first()
            if result is None or result.p10 is None or result.p90 is None:
                continue

            p10, p90 = result.p10, result.p90

            # calculate statistics
            if country_code is None:
                agg_query = text("""
                    SELECT
                        COUNT(*) AS count,
                        AVG(days_to_hire) AS avg,
                        MIN(days_to_hire) AS min,
                        MAX(days_to_hire) AS max
                    FROM public.job_posting
                    WHERE standard_job_id = :job_id
                      AND days_to_hire BETWEEN :p10 AND :p90
                """)
                agg_params = {'job_id': standard_job_id, 'p10': p10, 'p90': p90}
            else:
                agg_query = text("""
                    SELECT
                        COUNT(*) AS count,
                        AVG(days_to_hire) AS avg,
                        MIN(days_to_hire) AS min,
                        MAX(days_to_hire) AS max
                    FROM public.job_posting
                    WHERE standard_job_id = :job_id
                      AND country_code = :country
                      AND days_to_hire BETWEEN :p10 AND :p90
                """)
                agg_params = {**filter_params, 'p10': p10, 'p90': p90}

            agg_result = connection.execute(agg_query, agg_params).first()

            if agg_result.count < min_postings:
                continue

            session.execute(text("""
                INSERT INTO public.days_to_hire (
                    id, country_code, standard_job_id,
                    average_days_to_hire, minimum_days_to_hire,
                    maximum_days_to_hire, number_of_job_postings
                ) VALUES (
                    :id, :country_code, :standard_job_id,
                    :avg, :min, :max, :count
                )
            """), {
                'id': insert_id,
                'country_code': target_country,
                'standard_job_id': standard_job_id,
                'avg': agg_result.avg,
                'min': agg_result.min,
                'max': agg_result.max,
                'count': agg_result.count,
            })

            insert_id += 1
            inserted += 1

        session.commit()
        print(f"Inserted {inserted} rows into public.days_to_hire.")

    except Exception as e:
        session.rollback()
        print("Error occurred:", e)
        raise
    finally:
        session.close()




if __name__ == '__main__':
    calculate_days_to_hire()
