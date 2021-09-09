"""
A store of really useful functions.
"""


def log_return(list_stock_prices):
    return np.log(list_stock_prices).diff()


def realized_volatility(series_log_return):
    return np.sqrt(np.sum(series_log_return ** 2))


def calculate_wap(df):
    """
    https://www.kaggle.com/konradb/we-need-to-go-deeper
    """
    #     a = df['bid_price1'] * df['ask_size1'] + df['ask_price1'] * df['bid_size1']
    #     b = df['bid_size1']+ df['ask_size1']

    a1 = df["bid_price1"] * df["ask_size1"] + df["ask_price1"] * df["bid_size1"]
    a2 = df["bid_price2"] * df["ask_size2"] + df["ask_price2"] * df["bid_size2"]
    b = df["bid_size1"] + df["ask_size1"] + df["bid_size2"] + df["ask_size2"]

    x = (a1 + a2) / b
    return x


def get_log_return_df_per_time_id(file_path):
    # df_book_data = pd.read_parquet(file_path)
    dataset = pq.ParquetDataset(file_path)
    book_dataset = dataset.read()
    df_book_data = book_dataset.to_pandas()

    df_book_data["wap"] = calculate_wap(df_book_data)
    df_book_data["log_return"] = df_book_data.groupby(["time_id"])["wap"].apply(
        log_return
    )
    df_book_data = df_book_data[~df_book_data["log_return"].isnull()]

    stock_id = file_path.split("=")[1]
    df_book_data["row_id"] = df_book_data["time_id"].apply(lambda x: f"{stock_id}-{x}")

    del dataset, book_dataset
    gc.collect()
    return df_book_data


def get_realized_volatility_df_per_time_id(file_path):
    # df_book_data = pd.read_parquet(file_path)
    dataset = pq.ParquetDataset(file_path)
    book_dataset = dataset.read()
    df_book_data = book_dataset.to_pandas()

    df_book_data["wap"] = calculate_wap(book_example)
    df_book_data["log_return"] = df_book_data.groupby(["time_id"])["wap"].apply(
        log_return
    )
    df_book_data = df_book_data[~df_book_data["log_return"].isnull()]

    df_book_data["realized_volatility"] = df_book_data.groupby(["time_id"])[
        "log_return"
    ].apply(realized_volatility)
    df_book_data = df_book_data[~df_book_data["realized_volatility"].isnull()]

    stock_id = file_path.split("=")[1]
    df_book_data["row_id"] = df_book_data["time_id"].apply(lambda x: f"{stock_id}-{x}")

    del dataset, book_dataset
    gc.collect()

    return df_book_data


def realized_volatility_per_time_id(file_path, prediction_column_name):
    df_book = pd.read_parquet(file_path)
    df_book["wap"] = calculate_wap(df_book)
    df_book["log_return"] = df_book.groupby(["time_id"])["wap"].apply(log_return)
    df_book = df_book[~df_book["log_return"].isnull()]
    df_realized_vol_per_stock = pd.DataFrame(
        df_book.groupby(["time_id"])["log_return"].agg(realized_volatility)
    ).reset_index()
    df_realized_vol_per_stock = df_realized_vol_per_stock.rename(
        columns={"log_return": prediction_column_name}
    )
    stock_id = file_path.split("=")[1]
    df_realized_vol_per_stock["row_id"] = df_realized_vol_per_stock["time_id"].apply(
        lambda x: f"{stock_id}-{x}"
    )
    return df_realized_vol_per_stock[["row_id", prediction_column_name]]


# %%
def feature_engineering(df, null_val=-9999):
    for n in range(1, 3):
        p1 = df[f"bid_price{n}"]
        p2 = df[f"ask_price{n}"]
        s1 = df[f"bid_size{n}"]
        s2 = df[f"ask_size{n}"]
        df["WAP"] = (p1 * s2 + p2 * s1) / (s1 + s2)

        df["log_wap"] = df["WAP"].log()
        df["log_wap_shifted"] = (
            df[["time_id", "log_wap"]]
            .groupby("time_id", method="cudf")
            .apply_grouped(
                cutran.get_cu_shift_transform(shift_by=1, null_val=null_val),
                incols={"log_wap": "x"},
                outcols=dict(y_out=cp.float32),
                tpb=32,
            )["y_out"]
        )
        df = df[df["log_wap_shifted"] != null_val]

        df["diff_log_wap"] = df["log_wap"] - df["log_wap_shifted"]
        df[f"diff_log_wap{n}"] = df["diff_log_wap"] ** 2

    df["c"] = 1

    sum_df = (
        df.groupby("time_id")
        .agg({"diff_log_wap1": {"sum", "std"}, "diff_log_wap2": "sum", "c": "sum"})
        .reset_index()
    )

    def f(x):
        if x[1] == "":
            return x[0]
        return x[0] + "_" + x[1]

    sum_df.columns = [f(x) for x in sum_df.columns]
    sum_df["volatility1"] = (sum_df["diff_log_wap1_sum"]) ** 0.5
    sum_df["volatility2"] = (sum_df["diff_log_wap2_sum"]) ** 0.5
    sum_df["c"] = sum_df["c_sum"].values
    sum_df["vol_std"] = sum_df["diff_log_wap1_std"].fillna(0).values
    sum_df["volatility_rate"] = (sum_df["volatility1"] / sum_df["volatility2"]).fillna(
        0
    )
    return sum_df[["time_id", "volatility1", "volatility_rate", "c", "vol_std"]]

