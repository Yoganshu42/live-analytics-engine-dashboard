# services/analytics/__init__.py

from services.analytics.base_engine import BaseAnalyticsEngine
from services.analytics.samsung_engine import SamsungAnalyticsEngine
from services.analytics.reliance_engine import RelianceAnalyticsEngine
from services.analytics.goodrej_engine import GodrejAnalyticsEngine

ENGINE_REGISTRY: dict[str, type[BaseAnalyticsEngine]] = {
    "samsung": SamsungAnalyticsEngine,
    "reliance": RelianceAnalyticsEngine,
    "godrej": GodrejAnalyticsEngine,
}
