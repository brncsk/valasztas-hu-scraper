from os import environ

import re
import json

import asyncio
import aiohttp

from shapely.geometry import shape, mapping, Polygon
from geojson import Feature, FeatureCollection
from openpyxl import load_workbook

from typing import AsyncGenerator, Dict, Mapping
from mypy_extensions import TypedDict
from enum import Enum, IntEnum

import logging

logging.basicConfig(level=environ.get("LOGLEVEL", "INFO"))
logger = logging.getLogger(__name__)


class WorksheetColumns(IntEnum):
    COUNTY = 1
    SETTLEMENT = 2
    STATION_NO = 3

    NOMINAL_VOTER_COUNT = 4
    ACTUAL_VOTER_COUNT = 5

    BALLOTS_WITHOUT_STAMP = 6
    BALLOTS_STAMPED = 7
    BALLOT_TO_ACTUAL_VOTERS_DIFF = 8

    INVALID_BALLOTS = 9
    VALID_BALLOTS = 10

    BALLOT_COUNT_MSZP_PARBESZED = 11
    BALLOT_COUNT_MKKP = 12
    BALLOT_COUNT_JOBBIK = 13
    BALLOT_COUNT_FIDESZ = 14
    BALLOT_COUNT_MOMENTUM = 15
    BALLOT_COUNT_DK = 16
    BALLOT_COUNT_MI_HAZANK = 17
    BALLOT_COUNT_MUNKASPART = 18
    BALLOT_COUNT_LMP = 19


class PollingStationApiParamNames(Enum):
    SETTLEMENT_CODE = "_onkszavazokorieredmenyek_WAR_nvinvrportlet_telepulesKod"
    COUNTY_CODE = "_onkszavazokorieredmenyek_WAR_nvinvrportlet_megyeKod"
    STATION_NO = "_onkszavazokorieredmenyek_WAR_nvinvrportlet_szavkorSorszam"


PollingStation = TypedDict(
    "PollingStation", {"api_params": Mapping[str, str], "properties": Mapping[str, int]}
)


WORKBOOK_FILE = "EP_2019_szavazóköri_eredmény.xlsx"
API_BASE_URL = "https://www.valasztas.hu/szavazokorok_onk2019"

API_COMMON_PARAMS = dict(
    p_p_lifecycle="2",
    p_p_state="maximized",
    p_p_mode="view",
    p_p_cacheability="cacheLevelPage",
    _onkszavazokorieredmenyek_WAR_nvinvrportlet_vlId="294",
    _onkszavazokorieredmenyek_WAR_nvinvrportlet_vltId="687",
)

API_GET_MAP_DATA_PARAMS = dict(
    **API_COMMON_PARAMS,
    p_p_id="onkszavazokorieredmenyek_WAR_nvinvrportlet",
    p_p_resource_id="resourceIdGetElectionMapData",
    _onkszavazokorieredmenyek_WAR_nvinvrportlet_tabId="tab2",
)

API_SEARCH_PARAMS = dict(
    **API_COMMON_PARAMS,
    p_p_id="onkszavazokorok_WAR_nvinvrportlet",
    p_p_resource_id="resourceIdGetTelepulesOrMegye",
)

SETTLEMENT_CODE_CACHE: Dict[str, str] = {}


async def get_settlement_code(settlement_name: str) -> str:
    try:
        return SETTLEMENT_CODE_CACHE[settlement_name]
    except KeyError:
        async with aiohttp.ClientSession() as session:
            params = {
                **API_SEARCH_PARAMS,
                "_onkszavazokorok_WAR_nvinvrportlet_keywords": settlement_name,
            }

            async with session.get(API_BASE_URL, params=params) as response:
                try:
                    settlement_code = next(
                        match["telepulesKod"]
                        for match in await response.json()
                        if re.compile(f"^{settlement_name}(?: .+)?$").match(
                            match["telepulesNeve"]
                        )
                    )

                    logger.debug(f"{settlement_name} azonosítója: {settlement_code}")

                    SETTLEMENT_CODE_CACHE[settlement_name] = settlement_code
                    return settlement_code
                except Exception as e:
                    raise Exception(
                        f"Nem sikerült lekérdezni a településazonosítót: {settlement_name}: {str(e)}, {params}"
                    )


async def polling_stations() -> AsyncGenerator[PollingStation, None]:
    workbook = load_workbook(WORKBOOK_FILE)

    for county_index, worksheet in enumerate(workbook.worksheets):
        _, *rows = worksheet.values
        logger.info(f"Megye: {worksheet.title}")

        for row in rows:
            if row[0] is None:
                continue

            station_no = str(int(row[WorksheetColumns.STATION_NO]))
            county_code = f"{county_index + 1:02}"
            settlement_code = await get_settlement_code(
                row[WorksheetColumns.SETTLEMENT]
            )

            yield {
                "api_params": {
                    PollingStationApiParamNames.STATION_NO.value: station_no,
                    PollingStationApiParamNames.SETTLEMENT_CODE.value: settlement_code,
                    PollingStationApiParamNames.COUNTY_CODE.value: county_code,
                },
                "properties": {
                    field.name: row[field.value] for field in [*WorksheetColumns]
                },
            }


def get_polling_station_repr(ps: PollingStation) -> str:
    return f"""\
Megye: {ps["properties"][WorksheetColumns.COUNTY.name]}, \
Település: {ps["properties"][WorksheetColumns.SETTLEMENT.name]}, \
Szavkör: {ps["properties"][WorksheetColumns.STATION_NO.name]}"""


async def fetch_polling_station_geometries() -> AsyncGenerator[Feature, None]:
    async with aiohttp.ClientSession() as session:
        async for polling_station in polling_stations():
            station_repr = get_polling_station_repr(polling_station)
            logger.debug(station_repr)

            params = {**API_GET_MAP_DATA_PARAMS, **polling_station["api_params"]}

            async with session.get(API_BASE_URL, params=params) as response:
                try:
                    data = await response.json()
                    paths = json.loads(data["polygon"]["paths"])
                    geometry = mapping(
                        Polygon((point["lng"], point["lat"]) for point in paths)
                    )
                except Exception as exc:
                    geometry = None
                    logger.error(f"Sikertelen a geometria lekérdezése: {station_repr}")
                    # logger.error(
                    #     str(
                    #         {**API_GET_MAP_DATA_PARAMS, **polling_station["api_params"]}
                    #     )
                    # )
                    # logger.error(str(exc))
                    pass

                yield Feature(
                    geometry=geometry, properties=polling_station["properties"]
                )


async def run():
    print(
        (
            FeatureCollection(
                features=[
                    feature async for feature in fetch_polling_station_geometries()
                ]
            )
        )
    )


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(run())
