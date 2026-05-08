import asyncio
from datetime import datetime, timedelta

from ner_penelope import NerDbClient, Car

async def main():
    res = NerDbClient("XXX", "XXX", Car.V24A)
    b1 = datetime.fromisoformat("2025-05-01T15:14:41.618Z")
    e = datetime.fromisoformat("2025-05-01T15:14:49.834Z")
    #r2 = await res.select_data_by_datatypename("EM/Measurement/Voltage", b1, e, multi_topic=False)
    #print(r2)

    r3 = await res.select_datatypes()
    print(r3)

    b = datetime.fromisoformat("2025-04-20T15:14:48.618Z")
    e = datetime.fromisoformat("2025-05-01T15:14:49.834Z")
    r4 = await res.select_runs_by_time(b, e)
    print(r4)


    r5 = await res.select_data_normalized(["DTI/Power/DC_Current", "DTI/Power/AC_Current"], b1, e, timedelta(milliseconds=25))
    print(r5)
    r5.to_pandas().plot(x="time_bucket_gapfill", y=["DTI/Power/DC_Current", "DTI/Power/AC_Current"])
    import matplotlib.pyplot as plt
    plt.show()


if __name__ == "__main__":
    asyncio.run(main())
