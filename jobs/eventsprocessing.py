from pyspark.sql.types import *
from dependencies.Spark import get_spark
from dependencies.eventsdict import *
from pyspark.sql.functions import udf


def customSchema():
    schema = (StructType().
              add("id_odsp", StringType()).add("id_event", StringType()).add("sort_order", IntegerType()).
              add("time", IntegerType()).add("text", StringType()).add("event_type", IntegerType()).
              add("event_type2", IntegerType()).add("side", IntegerType()).add("event_team", StringType()).
              add("opponent", StringType()).add("player", StringType()).add("player2", StringType()).
              add("player_in", StringType()).add("player_out", StringType()).add("shot_place", IntegerType()).
              add("shot_outcome", IntegerType()).add("is_goal", IntegerType()).add("location", IntegerType()).
              add("bodypart", IntegerType()).add("assist_method", IntegerType()).add("situation", IntegerType()).
              add("fast_break", IntegerType())
              )

    return schema


def read_events(schema):
    eventsDf = (
        get_spark().read.csv("/Users/ramakrishnaaddanki/Downloads/football-events/events.csv",
                             schema=schema, header=True,
                             ignoreLeadingWhiteSpace=True,
                             ignoreTrailingWhiteSpace=True,
                             nullValue='NA'))

    eventsDf = eventsDf.na.fill({'player': 'NA', 'event_team': 'NA', 'opponent': 'NA',
                                 'event_type': 99, 'event_type2': 99, 'shot_place': 99,
                                 'shot_outcome': 99, 'location': 99, 'bodypart': 99,
                                 'assist_method': 99, 'situation': 99})

    return eventsDf


def read_agg_events_info():
    gameInfDf = (
        get_spark().read.csv("/Users/ramakrishnaaddanki/Downloads/football-events/ginf.csv",
                             inferSchema=True, header=True,
                             ignoreLeadingWhiteSpace=True,
                             ignoreTrailingWhiteSpace=True,
                             nullValue="NA"))
    return gameInfDf


def mapKeyToVal(mapping):
    def mapKeyToVal_(col):
        return mapping.get(col)
    return udf(mapKeyToVal_, StringType())


def transformationEvents(gameInfDf, eventsDf):
    gameInfDf = gameInfDf.withColumn("country_code", mapKeyToVal(countryCodeMap)("country"))
    eventsDf = (
        eventsDf.
            withColumn("event_type_str", mapKeyToVal(evtTypeMap)("event_type")).
            withColumn("event_type2_str", mapKeyToVal(evtTyp2Map)("event_type2")).
            withColumn("side_str", mapKeyToVal(sideMap)("side")).
            withColumn("shot_place_str", mapKeyToVal(shotPlaceMap)("shot_place")).
            withColumn("shot_outcome_str", mapKeyToVal(shotOutcomeMap)("shot_outcome")).
            withColumn("location_str", mapKeyToVal(locationMap)("location")).
            withColumn("bodypart_str", mapKeyToVal(bodyPartMap)("bodypart")).
            withColumn("assist_method_str", mapKeyToVal(assistMethodMap)("assist_method")).
            withColumn("situation_str", mapKeyToVal(situationMap)("situation"))
    )

    joinedDf = (
        eventsDf.join(gameInfDf, eventsDf.id_odsp == gameInfDf.id_odsp, 'inner').
            select(eventsDf.id_odsp, eventsDf.id_event, eventsDf.sort_order, eventsDf.time, eventsDf.event_type,
                   eventsDf.event_type_str, eventsDf.event_type2, eventsDf.event_type2_str, eventsDf.side,
                   eventsDf.side_str, eventsDf.event_team, eventsDf.opponent, eventsDf.player, eventsDf.player2,
                   eventsDf.player_in, eventsDf.player_out, eventsDf.shot_place, eventsDf.shot_place_str,
                   eventsDf.shot_outcome, eventsDf.shot_outcome_str, eventsDf.is_goal, eventsDf.location,
                   eventsDf.location_str, eventsDf.bodypart, eventsDf.bodypart_str, eventsDf.assist_method,
                   eventsDf.assist_method_str, eventsDf.situation, eventsDf.situation_str, gameInfDf.country_code)
    )

    joinedDf.repartition(1).write.csv("/Users/ramakrishnaaddanki/output.csv",
                                      sep=",")

    return joinedDf


def main():
    cs = customSchema()
    edf = read_events(cs)
    adf = read_agg_events_info()
    tse = transformationEvents(adf, edf)
    tse.show()
    return None
