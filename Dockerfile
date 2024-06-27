FROM quay.io/astronomer/astro-runtime:11.4.0

USER root
COPY ./dbt_sneakers_sa ./dbt_sneakers_sa
COPY --chown=astro:0 . .

USER astro
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt_sneakers_sa/dbt_requirements.txt && \
    source dbt_sneakers_sa/dbt.env && \
    deactivate