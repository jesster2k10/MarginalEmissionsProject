from enum import Enum

DATETIME_FORMAT    = "%d-%b-%Y %H:%M:%S"
DASHBOARD_ENDPOINT = "https://smartgriddashboard.com/DashboardService.svc/data"
Area = Enum('Area', [
    'fuelmix',
    'generationactual',
    'generationforecast',
    'windactual',
    'windforecast',
    'interconnection',
    'frequency',
    'co2emission',
    'co2intensity',
    'demandactual',
    'demandforecast',
    'SnspAll'
])