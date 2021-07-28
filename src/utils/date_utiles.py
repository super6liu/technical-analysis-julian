from datetime import date, timedelta

class DateUtils:
    @staticmethod
    def latest_weekday():
        t = date.today()
        d = t.weekday()
        if (d < 5):
            return t

        return t - timedelta(days= (d - 4))