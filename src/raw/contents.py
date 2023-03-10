import argparse
import datetime
import json
import requests
import time

import pandas as pd
import boto3


def get_data(**kwargs):
    url = "https://tabnews.com.br/api/v1/contents"
    resp = requests.get(url, params=kwargs)
    return resp


def save_with_spark(data):
    df = spark.createDataFrame(pd.DataFrame(data).astype(str))
    (
        df.coalesce(1)
        .write.format("parquet")
        .mode("append")
        .save("/mnt/datalake/raw/tabnews/contents")
    )
    return None


def save_with_firehose(data, firehose_client):

    data = [[i] for i in data]

    d = json.dumps(data)[1:-1].replace("], [", "]\n[") + "\n"

    firehose_client.put_record(
        DeliveryStreamName="tabnews-contents",
        Record={"Data": d},
    )

    return None


def process_new_until_date(date, save_type):
    date_finish = pd.Timestamp(date).date()
    date = datetime.datetime.now().date()

    firehose = boto3.client("firehose", region_name="us-east-1")

    params = {"page": 1, "per_page": 100, "strategy": "new"}

    print("Iniciando processo...")
    while date >= date_finish:

        try:
            resp = get_data(**params)
            data = resp.json()
        except Exception:
            print(resp.text)
            break

        if len(data) == 0:
            break

        if save_type == "spark":
            save_with_spark(data)

        elif save_type == "firehose":
            save_with_firehose(data, firehose)
        else:
            pass

        date = pd.Timestamp(min([i["created_at"] for i in data])).date()
        print(date, params["page"])
        params["page"] += 1
        if params["page"] % 10 == 0:
            print("Calma, respira...")
            time.sleep(5)
            print("pronto, vamos la!")
    print("Processo finalizado!")


def main():

    default_date = datetime.datetime.now() + datetime.timedelta(days=-7)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=default_date, help="Data limite para busca (mais antiga): YYYY-MM-DD")
    parser.add_argument("--save_type", type=str, default="", choices=["firehose", "spark", ""], help="Modo de salvar os dados")
    args = parser.parse_args()

    process_new_until_date(args.date, args.save_type)

if __name__ == "__main__":
    main()