# Конфигурация Superset для локального учебного стенда (наружу не публикуется)
SECRET_KEY = "q18-local-superset-secret-key-change-me"
# CSRF и Talisman отключены: стенд локальный, а настройка датасетов/графиков идёт через REST API скриптом
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
ROW_LIMIT = 50000
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True}
