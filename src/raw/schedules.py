import argparse
import datetime
import time

import schedule

import contents


def execute_last_days(days, save_type):
    date = datetime.datetime.now() + datetime.timedelta(days=-days)
    contents.process_new_until_date(date, save_type)


def execute_history(save_type):
    contents.process_new_until_date("2022-01-01", save_type)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--save_type",
        default="",
        choices=["firehose", "spark", ""],
        help="Modo de savar os dados coletados.",
    )
    parser.add_argument(
        "--delay",
        default=7,
        help="Tamanho do delay em dias para janela de coleta.",
        type=int,
    )
    parser.add_argument(
        "--interval",
        default=30,
        help="Intervalo de tempo para nova coleta de dados.",
        type=int,
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Flag para agendar full-load tambem.",
    )

    args = parser.parse_args()

    schedule.every(args.interval).minutes.do(
        execute_last_days, args.delay, args.save_type
    )

    if args.history:
        schedule.every(1).day.do(execute_history, args.save_type)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
