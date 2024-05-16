import time
from snowflake.snowpark import Session


def load_raw_table(session, tname=None, s3dir=None, year=None, schema=None):
    session.use_schema(schema)
    if year is None:
        location = "@external.ev_raw_stage/{}/{}".format(s3dir, tname)
    else:
        print("\tLoading year {}".format(year))
        location = "@external.ev_raw_stage/{}/{}/year={}".format(s3dir, tname, year)

    # we can infer schema using the parquet read option
    df = session.read.option("format", "json").json(location)

    df.copy_into_table("{}".format(tname))


if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        load_raw_table(session, tname="ev_raw_data", s3dir="data.json", schema="raw_ev")
#        validate_raw_tables(session)
