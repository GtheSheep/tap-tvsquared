"""Stream type classes for tap-tvsquared."""

import copy
import datetime
from typing import Any, Dict, Optional, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_tvsquared.client import TVSquaredStream


class InventoryStream(TVSquaredStream):
    name = "attribution_inventory"
    path = "/attribution/{partner_domain}"
    primary_keys = ["id", "type"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("title", th.StringType),
        th.Property("api", th.StringType),
        th.Property("portal", th.StringType),
        th.Property("brands", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("title", th.StringType),
            )
        )),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        response_json = response.json()
        partners = [dict(partner, **{'type': 'partner'}) for partner in response_json['partners']]
        clients = [dict(partner, **{'type': 'client'}) for partner in response_json['clients']]
        yield from partners + clients

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "brand_ids": [brand["id"] for brand in record.get('brands', [])],
            "inventory_type": record["type"],
        }


class BrandsStream(TVSquaredStream):
    name = "attribution_brands"
    parent_stream_type = InventoryStream
    ignore_parent_replication_keys = True
    path = "/attribution/{partner_domain}/{brand_id}"
    primary_keys = ["partner_domain", "brand_id"]
    state_partitioning_keys = ["partner_domain", "brand_id"]
    replication_key = None
    records_jsonpath = "$."
    schema = th.PropertiesList(
        th.Property("partner_domain", th.StringType),
        th.Property("brand_id", th.StringType),
        th.Property("tz", th.StringType),
        th.Property("tsgrans", th.ArrayType(th.StringType)),
        th.Property("tsrefs", th.ArrayType(th.StringType)),
        th.Property("tsrtmtypes", th.ArrayType(th.StringType)),
        th.Property("tsrefs", th.ArrayType(th.StringType)),
        th.Property("tsumsplits", th.ObjectType(
            th.Property("medium", th.ArrayType(th.StringType)),
            th.Property("model", th.ArrayType(th.StringType)),
            th.Property("region", th.ArrayType(th.StringType)),
            th.Property("ref", th.ArrayType(th.StringType)),
            th.Property("recency", th.ArrayType(th.StringType)),
            th.Property("product", th.ArrayType(th.StringType)),
            th.Property("device", th.ArrayType(th.StringType)),
            th.Property("lag", th.ArrayType(th.StringType)),
            th.Property("promo", th.ArrayType(th.StringType)),
        )),
        th.Property("smetrics", th.ArrayType(th.StringType)),
        th.Property("umetrics", th.ArrayType(th.StringType)),
        th.Property("uactions", th.ArrayType(th.StringType)),
        th.Property("media", th.ArrayType(th.StringType)),
        th.Property("spots", th.ObjectType(
            th.Property("min", th.DateTimeType),
            th.Property("max", th.DateTimeType),
            th.Property("count", th.NumberType),
        )),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if context["inventory_type"] != "client":
            return []
        else:
            for brand_id in context["brand_ids"]:
                context["brand_id"] = brand_id
                for record in self.request_records(context):
                    transformed_record = self.post_process(record, context)
                    if transformed_record is None:
                        # Record filtered out during post_process()
                        continue
                    yield transformed_record

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["partner_domain"] = self.config["partner_domain"]
        row["brand_id"] = context["brand_id"]
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        yield from [response.json()]


class SpotsStream(TVSquaredStream):
    name = "attribution_spots"
    parent_stream_type = InventoryStream
    ignore_parent_replication_keys = True
    path = "/attribution/{partner_domain}/{brand_id}/spotsjson/{year}/{month}/{day}"
    primary_keys = ["partner_domain", "brand_id", "datetime", "spotid"]
    state_partitioning_keys = ["partner_domain", "brand_id", "datetime", "spotid"]
    replication_key = "datetime"
    records_jsonpath = "$.spots[*]"
    schema = th.PropertiesList(
        th.Property("partner_domain", th.StringType),
        th.Property("brand_id", th.StringType),
        th.Property("aggchannel", th.StringType),
        th.Property("country", th.StringType),
        th.Property("region", th.StringType),
        th.Property("length", th.NumberType),
        th.Property("saleshouse", th.StringType),
        th.Property("datetime", th.DateTimeType),
        th.Property("spotid", th.NumberType),
        th.Property("daypart", th.StringType),
        th.Property("genre", th.StringType),
        th.Property("programme", th.StringType),
        th.Property("clocknumber", th.StringType),
        th.Property("channel", th.StringType),
        th.Property("um", th.ObjectType(
            th.Property("all response", th.ObjectType(
                th.Property("m", th.ObjectType(
                    th.Property("session", th.NumberType)
                ))
            ))
        )),
        th.Property("sm", th.ObjectType(
            th.Property("audience", th.ObjectType(th.Property("v", th.NumberType))),
            th.Property("cost", th.ObjectType(th.Property("v", th.NumberType))),
            th.Property("spots", th.ObjectType(th.Property("v", th.NumberType))),
        )),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if context["inventory_type"] != "client":
            return []
        else:
            for brand_id in context["brand_ids"]:
                context["brand_id"] = brand_id
                for record in self.request_records(context):
                    transformed_record = self.post_process(record, context)
                    if transformed_record is None:
                        # Record filtered out during post_process()
                        continue
                    yield transformed_record

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["datetime"] = datetime.datetime.strptime(context["datetime"], '%Y/%m/%d')
        row["partner_domain"] = self.config["partner_domain"]
        row["brand_id"] = context["brand_id"]
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        date = datetime.datetime.strptime('/'.join(response.request.url.split('/')[-3:]), '%Y/%m/%d')
        if date.date() < datetime.date.today():
            next_page_token = date + datetime.timedelta(days=1)
        else:
            next_page_token = None
        return next_page_token

    @staticmethod
    def _add_date_to_context(context, date):
        context["year"] = date.year
        context["month"] = date.month
        context["day"] = date.day
        context["datetime"] = date.strftime('%Y/%m/%d')

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)
        start_date = self.get_starting_timestamp(context)
        self._add_date_to_context(context=context, date=start_date)
        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            if next_page_token:
                self._add_date_to_context(context=context, date=next_page_token)
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token


class SpotColumnsStream(TVSquaredStream):
    name = "attribution_spot_columns"
    parent_stream_type = InventoryStream
    ignore_parent_replication_keys = True
    path = "/attribution/{partner_domain}/{brand_id}/spotheaders/{year}/{month}/{day}/{next_year}/{next_month}/{next_day}"
    primary_keys = ["partner_domain", "brand_id", "date_from", "date_to"]
    state_partitioning_keys = ["partner_domain", "brand_id", "date_from", "date_to"]
    replication_key = "date_from"
    records_jsonpath = "$.[*]"
    schema = th.PropertiesList(
        th.Property("partner_domain", th.StringType),
        th.Property("brand_id", th.StringType),
        th.Property("date_from", th.DateTimeType),
        th.Property("date_to", th.DateTimeType),
        th.Property("headers", th.ArrayType(th.StringType)),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if context["inventory_type"] != "client":
            return []
        else:
            for brand_id in context["brand_ids"]:
                context["brand_id"] = brand_id
                for record in self.request_records(context):
                    transformed_record = self.post_process(record, context)
                    if transformed_record is None:
                        # Record filtered out during post_process()
                        continue
                    yield transformed_record

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["date_from"] = datetime.datetime.strptime(context["date_from"], '%Y/%m/%d')
        row["date_to"] = datetime.datetime.strptime(context["date_to"], '%Y/%m/%d')
        row["partner_domain"] = self.config["partner_domain"]
        row["brand_id"] = context["brand_id"]
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        date = datetime.datetime.strptime('/'.join(response.request.url.split('/')[-6:-3]), '%Y/%m/%d')
        if date.date() < datetime.date.today():
            next_page_token = date + datetime.timedelta(days=1)
        else:
            next_page_token = None
        return next_page_token

    @staticmethod
    def _add_date_to_context(context, date):
        context["year"] = date.year
        context["month"] = date.month
        context["day"] = date.day
        context["date_from"] = date.strftime('%Y/%m/%d')
        next_date = date + datetime.timedelta(days=1)
        context["next_year"] = next_date.year
        context["next_month"] = next_date.month
        context["next_day"] = next_date.day
        context["date_to"] = next_date.strftime('%Y/%m/%d')

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)
        start_date = self.get_starting_timestamp(context)
        self._add_date_to_context(context=context, date=start_date)
        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            if next_page_token:
                self._add_date_to_context(context=context, date=next_page_token)
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "headers": record["headers"],
            "brand_ids": context["brand_ids"],
            "inventory_type": context["inventory_type"],
            "start_date": self.get_starting_timestamp(context),
        }


class SpotsArrayStream(TVSquaredStream):
    name = "attribution_spots_array"
    parent_stream_type = SpotColumnsStream
    ignore_parent_replication_keys = True
    rest_method = "POST"
    path = "/attribution/{partner_domain}/{brand_id}/spotsarray/{year}/{month}/{day}"
    primary_keys = ["partner_domain", "brand_id", "datetime"]
    state_partitioning_keys = ["partner_domain", "brand_id", "datetime"]
    replication_key = "datetime"
    schema = th.PropertiesList(
        th.Property("partner_domain", th.StringType),
        th.Property("brand_id", th.StringType),
        th.Property("datetime", th.DateTimeType),
        th.Property("spots", th.ArrayType(th.StringType)),
        th.Property("headers", th.ObjectType(
            th.Property("_geolevel", th.NumberType),
            th.Property("_supplieddatetime", th.NumberType),
            th.Property("_suppliedlocaldatetime", th.NumberType),
            th.Property("aggchannel", th.NumberType),
            th.Property("audience_unit", th.NumberType),
            th.Property("campaign", th.NumberType),
            th.Property("channel", th.NumberType),
            th.Property("chgenre", th.NumberType),
            th.Property("clockgroup", th.NumberType),
            th.Property("clocknumber", th.NumberType),
            th.Property("country", th.NumberType),
            th.Property("datagroup", th.NumberType),
            th.Property("datetime", th.NumberType),
            th.Property("daypart", th.NumberType),
            th.Property("genre", th.NumberType),
            th.Property("length", th.NumberType),
            th.Property("pip", th.NumberType),
            th.Property("programme", th.NumberType),
            th.Property("region", th.NumberType),
            th.Property("saleshouse", th.NumberType),
            th.Property("sm.audience.v", th.NumberType),
            th.Property("sm.cost.v", th.NumberType),
            th.Property("sm.spots.v", th.NumberType),
            th.Property("spotid", th.NumberType),
            th.Property("ts.web", th.NumberType),
            th.Property("um.all response.m.count", th.NumberType),
        )),
        th.Property("daterange", th.ObjectType(
            th.Property("min", th.DateTimeType),
            th.Property("max", th.DateTimeType),
        )),
        th.Property("ordinals", th.ObjectType(
            th.Property("genre", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("entity", th.StringType),
                th.Property("entityfield", th.StringType),
                th.Property("idordinal", th.StringType),
                th.Property("s3resolvable", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("programme", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("entity", th.StringType),
                th.Property("entityfield", th.StringType),
                th.Property("idordinal", th.StringType),
                th.Property("s3resolvable", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("channel", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("entity", th.StringType),
                th.Property("entityfield", th.StringType),
                th.Property("idordinal", th.StringType),
                th.Property("s3resolvable", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("aggchannel", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("entity", th.StringType),
                th.Property("entityfield", th.StringType),
                th.Property("idordinal", th.StringType),
                th.Property("s3resolvable", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("clocknumber", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("entity", th.StringType),
                th.Property("entityfield", th.StringType),
                th.Property("idordinal", th.StringType),
                th.Property("s3resolvable", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("daypart", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(
                    th.ObjectType(
                        th.Property("to", th.NumberType),
                        th.Property("from", th.NumberType),
                        th.Property("title", th.StringType),
                        th.Property("day0", th.BooleanType),
                        th.Property("day1", th.BooleanType),
                        th.Property("day2", th.BooleanType),
                        th.Property("day3", th.BooleanType),
                        th.Property("day4", th.BooleanType),
                        th.Property("day5", th.BooleanType),
                        th.Property("day6", th.BooleanType),
                        th.Property("order", th.NumberType),
                    )
                )),
            )),
            th.Property("length", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("s3metadata", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("region", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("entity", th.StringType),
                th.Property("entityfield", th.StringType),
                th.Property("idordinal", th.StringType),
                th.Property("s3resolvable", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("_geolevel", th.ObjectType(
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("saleshouse", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("s3metadata", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("pip", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("s3metadata", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("country", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("s3metadata", th.BooleanType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
            th.Property("datagroup", th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("date", th.BooleanType),
                th.Property("entity", th.StringType),
                th.Property("entityfield", th.StringType),
                th.Property("idordinal", th.StringType),
                th.Property("name", th.StringType),
                th.Property("values", th.ArrayType(th.StringType)),
            )),
        )),
        th.Property("uactions", th.ArrayType(th.StringType)),
        th.Property("umetrics", th.ArrayType(th.StringType)),
        th.Property("smetrics", th.ArrayType(th.StringType)),
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        return {"headers": context["headers"]}

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if context["inventory_type"] != "client":
            return []
        else:
            for brand_id in context["brand_ids"]:
                context["brand_id"] = brand_id
                for record in self.request_records(context):
                    transformed_record = self.post_process(record, context)
                    if transformed_record is None:
                        # Record filtered out during post_process()
                        continue
                    yield transformed_record

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        return {
            "partner_domain": self.config["partner_domain"],
            "brand_id": context["brand_id"],
            "datetime": datetime.datetime.strptime(context["datetime"], '%Y/%m/%d'),
            "spots": [str(spot) for spot in row["spots"]],
            "headers": row["headers"],
            "daterange": row.get("daterange", {}),
        }

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        date = datetime.datetime.strptime('/'.join(response.request.url.split('/')[-3:]), '%Y/%m/%d')
        if date.date() < datetime.date.today():
            next_page_token = date + datetime.timedelta(days=1)
        else:
            next_page_token = None
        return next_page_token

    @staticmethod
    def _add_date_to_context(context, date):
        context["year"] = date.year
        context["month"] = date.month
        context["day"] = date.day
        context["datetime"] = date.strftime('%Y/%m/%d')

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)
        self._add_date_to_context(context=context, date=context["start_date"])
        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            if next_page_token:
                self._add_date_to_context(context=context, date=next_page_token)
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token
