from datetime import datetime, timedelta, timezone

TZ_EST = timezone(timedelta(hours=-5), 'EST')

TODAY = datetime.now(TZ_EST).date()

TOMORROW = TODAY + timedelta(days=1)

LATEST_WEEKDAY = TODAY if TODAY.weekday() < 5 else TODAY - \
    timedelta(days=(TODAY.weekday() - 4))
