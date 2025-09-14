import jmespath
from datetime import datetime

class BaseMapper:
    def __init__(self, raw: dict):
        self.__raw: dict = raw
        self.__results: dict = {}

    def _mapper(self):
        main_table = {
            "subdistrict_code": jmespath.search("subdistrict_code", self.__raw),
            "district_code": jmespath.search("district_code", self.__raw),
            "city_code": jmespath.search("city_code", self.__raw),
            "province_code": jmespath.search("province_code", self.__raw),
            "province_name": jmespath.search("province", self.__raw),
            "district_name": jmespath.search("district", self.__raw),
            "city_name": jmespath.search("city", self.__raw),
            "subdistrict_name": jmespath.search("subdistrict", self.__raw),
            "administration_name": jmespath.search("administration_name", self.__raw),
            "latitude": jmespath.search("latitude", self.__raw),
            "longitude": jmespath.search("longitude", self.__raw)
        }
        self.__results.setdefault("table", []).append(main_table)

    def get_results(self):
        self._mapper()
        return self.__results