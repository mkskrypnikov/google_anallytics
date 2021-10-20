import mytools
#https://ga-dev-tools.web.app/dimensions-metrics-explorer/

def get_reports():
    ga_id = 'ххххх'
    start_dates = '2021-10-11'
    end_dates = '2021-10-17'
    metrics_ga = 'ga:sessions,ga:goal17Completions'
    dimmensions_ga = 'ga:date,ga:sourceMedium'
    filters_ga = 'ga:medium!~test;ga:source=~yandex'

    need_auth = 'yes'
    LastDate = 0
    replacement = 'no'

    mytools.import_ga(start_dates,end_dates,dimmensions_ga,metrics_ga,filters_ga,ga_id,need_auth,LastDate,replacement)


def main():
    try:
        get_reports()
    except
        get_reports()

if __name__ == '__main__':
    main()