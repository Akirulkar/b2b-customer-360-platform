from api.services.gold_service import GoldDataService, gold_service


def get_gold_service() -> GoldDataService:
    return gold_service