from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from numpy._typing import NDArray
    from polars import DataFrame

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    np = None

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None

class Car(Enum):
    V22A = "postgres"
    V24A = "penelope24a"
    V25A = "penelope25a"

class NerDbClient():
    """
    NER DB client for getting data

    There are 3 supported return types for data
    1. Raw tuples, where the first item is time, and following are datas.
    2. Indexed Polars DataFrame.  The index is time.
    3. Numpy array.  The first column is time.
    4. Raw CursorResult.  Useful for custom and efficient data conversion.

    This behavior defaults to 1. and 2. or 3. can be selected by installing with [numpy] or [polars], 4. can be selected with the raw param.
    """

    def __init__(self, user: str, password: str, car: Car, server="server.finishlinebyner.com:59021", raw=False):
        """
        user -> username for the db
        password -> password for the db
        car -> The vehicle to get data from
        server -> The server to connect to, defaults to NERs
        raw -> Whether to bypass data prettying and instead return the raw CursorResult
        """
        self.raw = raw
        self._car = car
        self._engine = create_async_engine(f"postgresql+asyncpg://{user}:{password}@{server}/{car.value}", pool_pre_ping=False)

    async def _execute(self, sql, args) -> CursorResult:
        async with self._engine.connect() as conn:
            stmt = text(sql)
            result = await conn.execute(stmt, args)
            return result


    async def close(self) -> None:
        await self._engine.dispose()


    async def select_datatypes(self) -> CursorResult | list[tuple] | NDArray | DataFrame:
        """
        Select datatypes, getting all datatypes for this vehicle.
        """
        sql = f"""
            SELECT * FROM "{"dataType" if self._car == Car.V22A else "data_type"}";
        """
        res = await self._execute(sql, None)

        if self.raw:
            return res

        if HAS_POLARS:
            return pl.DataFrame(res.all(),schema=list(res.keys()))
        elif HAS_NUMPY:
            return np.array(res.fetchall())
        else:
            return res.fetchall()

    async def select_runs_by_time(self, time_start: datetime, time_end: datetime, id=None) -> CursorResult | list[tuple] | NDArray | DataFrame:
        """
        Select runs by the time, and optionally the run ID.

        Note run ID is not unique at NER so time is required.
        """
        sql = f"""
            SELECT * FROM "run" WHERE time BETWEEN :s AND :e {"AND \"runId\" = :id" if id is not None else ""};
        """
        res = await self._execute(sql, {"s": time_start, "e": time_end, "id" : id})

        if self.raw:
            return res

        if HAS_POLARS:
            return pl.DataFrame(res.all(),schema=list(res.keys()))
        elif HAS_NUMPY:
            return np.array(res.fetchall())
        else:
            return res.fetchall()

    async def select_data_by_datatypename(self, data_typename: str, time_start: datetime, time_end: datetime, multi_topic=False) -> CursorResult | list[tuple] | NDArray | DataFrame:
        """
        Select data by time and name.  To select multiple data, merge it yourself or use the normalized function to compare it.

        multi_topic --> Return a list of floats, useful for multi-data topics like IMU and Gyroscopic data
        """
        sql = f"""
            SELECT "time", values{"" if multi_topic else "[1]"} FROM data WHERE "dataTypeName" = :dtn AND time BETWEEN :s AND :e;
        """
        res = await self._execute(sql, { "dtn" : data_typename, "s": time_start, "e": time_end})

        if self.raw:
            return res

        if HAS_POLARS:
            return pl.DataFrame(res.all(),schema=list(res.keys()))
        elif HAS_NUMPY:
            return np.array(res.fetchall())
        else:
            return res.fetchall()

    async def select_data_normalized(self, data_typenames: list[str], time_start: datetime, time_end: datetime, frequency: timedelta):
        """
        Selects data from multiple topics and normalizes it to a certain time frequency.
        This allows mathematical point to point comparision on data.
        Just provide the data types and time, as well as the bucket time freqeuncy.

        **Note this function can hallucinate data** as it interpolates and averages to match data between timestamps.
        """
        params =  {"all_metrics" : data_typenames, "s": time_start, "e": time_end, "bucket": frequency}

        select_lines = []
        for i, name in enumerate(data_typenames):
                    pname = f"m_{i}"
                    params[pname] = name
                    filter_clause = f"FILTER (WHERE \"dataTypeName\" = :{pname})"
                    select_lines.append(f" interpolate(avg(values[1]) {filter_clause}) AS \"{name}\"")

        select_block = ",\n".join(select_lines)

        sql = f"""
        SELECT
                        time_bucket_gapfill(
                            :bucket, time, CAST(:s AS timestamptz), CAST(:e AS timestamptz)
                        ),
                        {select_block}
                    FROM data
                    WHERE time BETWEEN :s AND :e
                      AND "dataTypeName" = ANY(:all_metrics)
                    GROUP BY 1
                    ORDER BY 1
        """

        res = await self._execute(sql, params)


        if self.raw:
            return res

        if HAS_POLARS:
            return pl.DataFrame(res.all(),schema=list(res.keys()))
        elif HAS_NUMPY:
            return np.array(res.fetchall())
        else:
            return res.fetchall()
