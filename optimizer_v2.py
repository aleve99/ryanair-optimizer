import grequests
import requests
import pandas as pd
import locale

from argparse import ArgumentParser, ArgumentError
from datetime import date, datetime, timedelta
from pathlib import Path
from itertools import cycle
from time import perf_counter_ns
from src.utils import *

SUPPORTED = list(CURRENCIES.keys())
proxies = None
converter = lambda str_date: date.fromisoformat(str_date) 

def exception_handler(request: grequests.AsyncRequest, exception):
    logger.warning(f"Request failed. Exception type = {type(exception)}")

    if isinstance(exception, requests.exceptions.ProxyError):
        logger.warning(f"Proxy failed: {request.kwargs['proxies']['https']}")
    
    logger.debug(f"Details: {exception}")
    logger.warning("Retrying with another proxy")

    params = request.kwargs['params']
    headers = request.kwargs['headers']
    cookies = request.kwargs['cookies']

    while True:
        proxy = next(proxies)
        try:
            res = requests.get(request.url, params=params, headers=headers, cookies=cookies, proxies=proxy)
            if res.ok:
                logger.warning("Request with another proxy succeded")
                logger.warning(proxy['https'])
                return res
            else:
                logger.warning(f"Request without proxies failed, STATUS_CODE: <{res.status_code}>")
                logger.warning("Retrying with another proxy")
        except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError) as error:
            res = requests.get(request.url, params=params, headers=headers, cookies=cookies)
            logger.warning(f"Proxy Error encountered: {type(error)}. Retrying without proxies")

            if res.ok:
                logger.warning("Request without proxies succedeed")
                return res
            else:
                break

def main():
    global proxies
    parser = ArgumentParser("Ryanair fares finder", description="Find the cheapest fares from an airport origin")

    ref_arg_ori = parser.add_argument("--origin", required=True, type=str, help="The origin airport (IATA code)")
    ref_arg_dest = parser.add_argument("--dest", default=None, type=str, help="The destination airport (IATA code). If missing search across all destinations")
    ref_arg_curr = parser.add_argument("--currency", default=None, type=str, help=f"The currency symbol to display fares, default is using local currency. Supported currencies {SUPPORTED}")
    ref_arg_min_nights = parser.add_argument("--min-nights", default=1, type=int, help="The minimum nights of vacation, must be >= 0")
    ref_arg_max_nights = parser.add_argument("--max-nights", required=True, type=int, help="The maximum nights of vacation, must be >= 0")
    parser.add_argument("--from-date", default=date.today(), type=converter, help="The first date (yyyy-mm-dd) to search for flights, default is today")
    parser.add_argument("--to-date", default=None, type=converter, help="The last date (yyyy-mm-dd) to search for flights, default is ryanair max range")
    parser.add_argument("--cfg-path", default=Path("src/config.toml"), type=Path, help="Toml file for the proxy and other configurations")
    parser.add_argument("--no-proxy", default=False, action='store_true', help="Use the proxy specified in the config.toml file")

    args = parser.parse_args()

    if not args.cfg_path.is_file() or args.cfg_path.name.split('.')[-1] != "toml":
        raise FileNotFoundError(f"{args.cfg_path.absolute()} is not a .toml file or doesn't exist")
    
    if args.min_nights < 0:
        raise ArgumentError(ref_arg_min_nights, f"must be positive")
    if args.max_nights < 0:
        raise ArgumentError(ref_arg_max_nights, f"must be positive")

    config = parse_toml(args.cfg_path)
    ports = range(config['proxy']['first_port'], config['proxy']['first_port'] + config['proxy']['num_ports'])
    
    proxies = cycle({
        'http': f'http://{config["proxy"]["username"]}:{config["proxy"]["password"]}@{config["proxy"]["host"]}:{port}',
        'https': f'https://{config["proxy"]["username"]}:{config["proxy"]["password"]}@{config["proxy"]["host"]}:{port}',
    } for port in ports)

    pool_size = config['network']['pool_size']

    if args.currency is None:
        loc = locale.getlocale()[0].split("_")
        language, country = loc[0].lower(), loc[1].lower()
    elif args.currency in SUPPORTED:
        language = CURRENCIES[args.currency]['langcode']
        country = CURRENCIES[args.currency]['country']
    else:
        raise ArgumentError(ref_arg_curr, f"{args.currency} is not supported {SUPPORTED}")
    
    cookies = {
        "rid": config["cookies"]["rid"],
        "rid.sig": config["cookies"]["rid.sig"],
        "mkt": f"/{country}/{language}/"
    }

    res = requests.get(ACTIVE_AIRPORTS)
    airports = map(lambda el: el['code'], res.json())
    res.raise_for_status()
    
    if args.origin not in airports:
        raise ArgumentError(ref_arg_ori, f"Origin {args.origin} is not a valid airport (IATA code)")

    logger.info(f"Retrieving available destinations from {args.origin}")
    res = requests.get(DESTINATIONS(args.origin))
    destinations = res.json()
    res.raise_for_status()

    if not args.dest:
        destination_codes = tuple(dest['arrivalAirport']['code'] for dest in destinations)
    elif args.dest not in tuple(dest['arrivalAirport']['code'] for dest in destinations):
        raise ArgumentError(ref_arg_dest, f"Destination not available from selected origin airport {args.origin}")
    else:
        destination_codes = tuple(args.dest)
        
    fares = []

    flex_days = 6
    tot_requests = 0
    t_start_all = perf_counter_ns()
    for destination in destination_codes:
        t_start = perf_counter_ns()
        logger.info(f"Scanning fares for {args.origin}-{destination}")

        res = requests.get(AVAILABLE_DATES(args.origin, destination))
        res.raise_for_status()

        str_dates = res.json()
        to_date = date.fromisoformat(str_dates[-1]) if not args.to_date else args.to_date
        
        reqs = list()

        d = args.from_date

        while d <= to_date:
            fd = flex_days if (to_date - d).days >= flex_days else (to_date - d).days
            params = get_payload(args.origin, destination, d, d, fd).to_dict()
            reqs.append(grequests.get(
                url=AVAILABILITY(language, country),
                params=params,
                headers=headers,
                cookies=cookies,
                timeout=config["network"]["timeout"],
                proxies={} if args.no_proxy else next(proxies),
                hooks={'response': hook}
            ))

            d = d + timedelta(days=flex_days+1)
            
        resps = grequests.map(reqs, size=pool_size, exception_handler=exception_handler)

        for req_num, res in enumerate(resps):
            if res and req_num == 0:
                json_res = res.json()
                trips = json_res['trips']
                currency = json_res['currency']
            elif res and req_num != 0:
                json_res = res.json()
                for j in range(2):
                    trips[j]['dates'].extend(
                        json_res['trips'][j]['dates']
                    )
            else:
                logger.warning(f"Request {res} is None")
                logger.warning(f"{res.url}")
                logger.warning(f"{res.text}")
                continue
        
        for trip_date_out in trips[0]['dates']:
            date_out = date.fromisoformat(trip_date_out['dateOut'][:10])

            for outbound_flight in trip_date_out['flights']:
                if outbound_flight['faresLeft'] != 0:
                    for i in range(len(trips[1]['dates'])):
                        trip_date_in = trips[1]['dates'][i]
                        date_in = date.fromisoformat(trip_date_in['dateOut'][:10])

                        if date_in < date_out:
                            i = (date_out - date_in).days
                            continue
                        elif args.min_nights <= (date_in - date_out).days <= args.max_nights:
                            for return_flight in trip_date_in['flights']:
                                if return_flight['faresLeft'] != 0:
                                    fares.append({
                                        'outbound_dep_time': datetime.fromisoformat(outbound_flight['time'][0]),
                                        'outbound_arr_time': datetime.fromisoformat(outbound_flight['time'][1]),
                                        'return_dep_time': datetime.fromisoformat(return_flight['time'][0]),
                                        'return_arr_time': datetime.fromisoformat(return_flight['time'][1]),
                                        'origin': trips[0]['origin'],
                                        'destination': trips[0]['destination'],
                                        'outbound_fare': outbound_flight['regularFare']['fares'][0]['amount'],
                                        'outbound_left': outbound_flight['faresLeft'],
                                        'return_fare': return_flight['regularFare']['fares'][0]['amount'],
                                        'return_left': return_flight['faresLeft'],
                                        'currency': currency
                                    })
                        elif date_in != date_out:
                            break
                            
        t_end = perf_counter_ns()
        round_time = round(1e-9 * (t_end - t_start), 4)
        total_time = round(1e-9 * (t_end - t_start_all), 4)
        perc = round(100* (destination_codes.index(destination) + 1) / len(destination_codes), 2)

        logger.info(f"{args.origin}-{destination} done, {perc}%, time: {round_time}s, total time: {total_time}s")
        logger.info(f"Requests done: {req_num + 1}")
        tot_requests += (req_num + 1)

    t_end_all = perf_counter_ns()

    df = pd.DataFrame(fares)
    if not df.empty:
        df['round_trip_fare'] = (df["outbound_fare"] + df["return_fare"]).round(decimals=2)
        df = df.sort_values(by="round_trip_fare", ascending=True).reset_index(drop=True)
        columns = df.columns.to_list()
        df = df[columns[:-3] + [df.columns[-1], df.columns[-2]]]
    
    print(df)

    df.to_csv("fares.csv", index=False)
    logger.info(f"Scraping done in {(t_end_all - t_start_all) * 1e-9}s")
    logger.info(f"Total requests: {tot_requests}")

if __name__ == "__main__":
    main()