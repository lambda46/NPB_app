import pandas as pd
import polars as pl
import numpy as np
import sqlalchemy as sa
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy import text as q_text

def my_round(x, decimals=0):
    return np.floor(x * 10**decimals + 0.5) / 10**decimals

def partial_match_merge(batting_df, stealing_df, on_batting, on_stealing):
    # スタート時点でSB, CS列を追加
    merged_df = batting_df.with_columns([
        pl.lit(0).alias("SB"),
        pl.lit(0).alias("CS")
    ])
    
    # 条件に基づいたフィルタリングと列の更新
    for stealing_row in stealing_df.iter_rows(named=True):
        stealing_team = stealing_row["Team"]
        runner_name = stealing_row[on_stealing]
        
        # 盗塁データと部分一致するバッティングデータをフィルタ
        matched_rows = merged_df.filter(
            (pl.col("Team") == stealing_team) & 
            (pl.col(on_batting).str.starts_with(runner_name))
        )
        
        # 該当する行にSBとCSを更新
        if matched_rows.shape[0] > 0:
            merged_df = merged_df.with_columns([
                pl.when(
                    (pl.col("Team") == stealing_team) & 
                    (pl.col(on_batting).str.starts_with(runner_name))
                ).then(stealing_row["SB"]).otherwise(pl.col("SB")).alias("SB"),
                pl.when(
                    (pl.col("Team") == stealing_team) & 
                    (pl.col(on_batting).str.starts_with(runner_name))
                ).then(stealing_row["CS"]).otherwise(pl.col("CS")).alias("CS")
            ])
    
    return merged_df

def cal_RE24(PA_df):
    # STATEがNEW.STATEと異なる、もしくはruns_scoredが0以上の行をフィルタリング
    PA_df = PA_df.filter(
        (pl.col("STATE") != pl.col("NEW.STATE")) | (pl.col("runs_scored") > 0)
    )

    # Outs_Inningが3の行をフィルタリング
    PA_dfC = PA_df.filter(pl.col("Outs_Inning") == 3)

    # STATEごとにRUNS.ROIの平均を計算
    RUNS = PA_dfC.group_by("STATE").agg(
        pl.col("RUNS.ROI").mean().alias("Mean")
    )

    # STATE列の文字列処理
    RUNS = RUNS.with_columns([
        pl.col("STATE").str.slice(-1).alias("Outs"),  # STATEの最後の文字（アウト）を取得
        pl.col("STATE").str.slice(0, 3).alias("RUNNER")  # STATEの最初の3文字（ランナー状態）を取得
    ])

    # Outs列で並べ替え
    RUNS = RUNS.sort("Outs")
    return RUNS

def cal_PF(PA_df, league_type):
    if league_type == "1軍":
        pf_PA_df = PA_df.filter(
            ((pl.col("home_team") == "巨人") & (pl.col("stadium") == "東京ドーム")) |
            ((pl.col("home_team") == "阪神") & (pl.col("stadium") == "甲子園")) |
            ((pl.col("home_team") == "ヤクルト") & (pl.col("stadium") == "神宮")) |
            ((pl.col("home_team") == "DeNA") & (pl.col("stadium") == "横浜")) |
            ((pl.col("home_team") == "広島") & (pl.col("stadium") == "マツダスタジアム")) |
            ((pl.col("home_team") == "中日") & (pl.col("stadium") == "バンテリンドーム")) |
            ((pl.col("home_team") == "オリックス") & (pl.col("stadium") == "京セラD大阪")) |
            ((pl.col("home_team") == "ソフトバンク") & ((pl.col("stadium") == "PayPayドーム")|(pl.col("stadium") == "みずほPayPay"))) |
            ((pl.col("home_team") == "ロッテ") & (pl.col("stadium") == "ZOZOマリン")) |
            ((pl.col("home_team") == "日本ハム") & (pl.col("stadium") == "エスコンF")) |
            ((pl.col("home_team") == "楽天") & (pl.col("stadium") == "楽天モバイル")) |
            ((pl.col("home_team") == "西武") & (pl.col("stadium") == "ベルーナドーム"))
        ).filter(
            (pl.col("game_type") == "パ・リーグ") | (pl.col("game_type") == "セ・リーグ")
        )

    else:
        pf_PA_df = PA_df.filter(
            ((pl.col("home_team") == "巨人") & (pl.col("stadium") == "ジャイアンツ")) |
            ((pl.col("home_team") == "阪神") & (pl.col("stadium") == "鳴尾浜")) |
            ((pl.col("home_team") == "ヤクルト") & (pl.col("stadium") == "戸田")) |
            ((pl.col("home_team") == "DeNA") & ((pl.col("stadium") == "横須賀")|(pl.col("stadium") == "平塚"))) |
            ((pl.col("home_team") == "広島") & (pl.col("stadium") == "由宇")) |
            ((pl.col("home_team") == "中日") & (pl.col("stadium") == "ナゴヤ球場")) |
            ((pl.col("home_team") == "オリックス") & (pl.col("stadium") == "杉本商事BS")) |
            ((pl.col("home_team") == "ソフトバンク") & (pl.col("stadium") == "タマスタ筑後")) |
            ((pl.col("home_team") == "ロッテ") & (pl.col("stadium") == "ロッテ")) |
            ((pl.col("home_team") == "日本ハム") & (pl.col("stadium") == "鎌スタ")) |
            ((pl.col("home_team") == "楽天") & (pl.col("stadium") == "森林どり泉")) |
            ((pl.col("home_team") == "西武") & ((pl.col("stadium") == "カーミニーク") | (pl.col("stadium") == "ベルーナドーム"))) |
            ((pl.col("home_team") == "くふうハヤテ") & (pl.col("stadium") == "ちゅ～る")) |
            ((pl.col("home_team") == "オイシックス") & ((pl.col("stadium") == "ハードオフ新潟")|(pl.col("stadium") == "新潟みどり森")|(pl.col("stadium") == "長岡悠久山")))
        ).filter(
            (pl.col("game_type") == "イ・リーグ") | (pl.col("game_type") == "ウ・リーグ")
        )

    ev_away = (pf_PA_df.group_by(["away_league", "away_team"]).agg(
                pl.n_unique("game_date").alias("away_g"),
                pl.count().alias("away_pa"),
                pl.sum("runs_scored").alias("away_score"),
                (pl.col("description") == "hit_into_play").sum().alias("away_con"),
                (pl.col("events") == "home_run").sum().alias("away_hr"),
                (pl.col("events") == "single").sum().alias("away_1b"),
                (pl.col("events") == "double").sum().alias("away_2b"),
                (pl.col("events") == "triple").sum().alias("away_3b"),
                ((pl.col("events") == "walk")|(pl.col("events") == "intentional_walk")).sum().alias("away_bb"),
                ((pl.col("events") == "strike_out")|(pl.col("events") == "uncaught_third_strike")).sum().alias("away_k"),
                (pl.col("events") == "hit_by_pitch").sum().alias("away_hbp"),
                ((pl.col("events") == "sac_fly")|(pl.col("events") == "sac_fly_error")).sum().alias("away_sf"),
                ((pl.col("events") == "sac_bunt")|(pl.col("events") == "bunt_error")|(pl.col("events") == "bunt_fielders_choice")).sum().alias("away_sh"),
                (pl.col("events") == "obstruction").sum().alias("away_obstruction"),
                (pl.col("events") == "interference").sum().alias("away_interference"),
            ).with_columns(
                [(pl.col("away_1b") + pl.col("away_2b") + pl.col("away_3b") + pl.col("away_hr")).alias("away_h")]
            ).with_columns([
                (pl.col("away_score")/pl.col("away_g")).alias("away_score/g"),
                (pl.col("away_hr")/pl.col("away_g")).alias("away_hr/g"),
                (pl.col("away_1b")/pl.col("away_g")).alias("away_1b/g"),
                (pl.col("away_2b")/pl.col("away_g")).alias("away_2b/g"),
                (pl.col("away_3b")/pl.col("away_g")).alias("away_3b/g"),
                (pl.col("away_h")/pl.col("away_g")).alias("away_h/g"),
                (pl.col("away_bb")/pl.col("away_g")).alias("away_bb/g"),
                (pl.col("away_k")/pl.col("away_g")).alias("away_k/g")
            ])
            .rename(
                {"away_league": "League", "away_team": "Team"}
            ))

    ev_home = (pf_PA_df.group_by(["home_league", "home_team"]).agg(
                pl.n_unique("game_date").alias("home_g"),
                pl.count().alias("home_pa"),
                pl.sum("runs_scored").alias("home_score"),
                (pl.col("description") == "hit_into_play").sum().alias("home_con"),
                (pl.col("events") == "home_run").sum().alias("home_hr"),
                (pl.col("events") == "single").sum().alias("home_1b"),
                (pl.col("events") == "double").sum().alias("home_2b"),
                (pl.col("events") == "triple").sum().alias("home_3b"),
                ((pl.col("events") == "walk")|(pl.col("events") == "intentional_walk")).sum().alias("home_bb"),
                ((pl.col("events") == "strike_out")|(pl.col("events") == "uncaught_third_strike")).sum().alias("home_k"),
                (pl.col("events") == "hit_by_pitch").sum().alias("home_hbp"),
                ((pl.col("events") == "sac_fly")|(pl.col("events") == "sac_fly_error")).sum().alias("home_sf"),
                ((pl.col("events") == "sac_bunt")|(pl.col("events") == "bunt_error")|(pl.col("events") == "bunt_fielders_choice")).sum().alias("home_sh"),
                (pl.col("events") == "obstruction").sum().alias("home_obstruction"),
                (pl.col("events") == "interference").sum().alias("home_interference"),
            ).with_columns([
                (pl.col("home_1b") + pl.col("home_2b") + pl.col("home_3b") + pl.col("home_hr")).alias("home_h")
            ]).with_columns([
                (pl.col("home_score")/pl.col("home_g")).alias("home_score/g"),
                (pl.col("home_hr")/pl.col("home_g")).alias("home_hr/g"),
                (pl.col("home_1b")/pl.col("home_g")).alias("home_1b/g"),
                (pl.col("home_2b")/pl.col("home_g")).alias("home_2b/g"),
                (pl.col("home_3b")/pl.col("home_g")).alias("home_3b/g"),
                (pl.col("home_h")/pl.col("home_g")).alias("home_h/g"),
                (pl.col("home_bb")/pl.col("home_g")).alias("home_bb/g"),
                (pl.col("home_k")/pl.col("home_g")).alias("home_k/g")
            ])
            .rename(
                {"home_league": "League", "home_team": "Team"}
            ))

    pf = ev_away.join(ev_home, on=["League", "Team"], how="left")
    league_counts = pf['League'].value_counts()
    league_counts.columns = ['League', 'League_Count']
    pf = pf.join(league_counts, on="League", how="left")

    #ev_compare["pf"] = ev_compare["home_hr_event"]/(ev_compare["home_hr_event"] * ev_compare["away/home"] + ev_compare["away_hr_event"] * (1 - ev_compare["away/home"]))
    pf = (pf.with_columns([
            (pl.col("home_hr/g") / ((pl.col("away_hr/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_hr/g") * 1 / pl.col("League_Count")))).alias("hr_pf"),
            (pl.col("home_1b/g") / ((pl.col("away_1b/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_1b/g") * 1 / pl.col("League_Count")))).alias("1b_pf"),
            (pl.col("home_2b/g") / ((pl.col("away_2b/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_2b/g") * 1 / pl.col("League_Count")))).alias("2b_pf"),
            (pl.col("home_3b/g") / ((pl.col("away_3b/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_3b/g") * 1 / pl.col("League_Count")))).alias("3b_pf"),
            (pl.col("home_h/g") / ((pl.col("away_h/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_h/g") * 1 / pl.col("League_Count")))).alias("h_pf"),
            (pl.col("home_bb/g") / ((pl.col("away_bb/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_bb/g") * 1 / pl.col("League_Count")))).alias("bb_pf"),
            (pl.col("home_k/g") / ((pl.col("away_k/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_k/g") * 1 / pl.col("League_Count")))).alias("k_pf"),
            (pl.col("home_score/g") / ((pl.col("away_score/g") * (pl.col("League_Count") - 1) / pl.col("League_Count")) + (pl.col("home_score/g") * 1 / pl.col("League_Count")))).alias("runs_pf")
        ]).with_columns([
            (my_round(pl.col("hr_pf"), 3)).alias("HR_PF"),
            (my_round(pl.col("1b_pf"), 3)).alias("1B_PF"),
            (my_round(pl.col("2b_pf"), 3)).alias("2B_PF"),
            (my_round(pl.col("3b_pf"), 3)).alias("3B_PF"),
            (my_round(pl.col("h_pf"), 3)).alias("H_PF"),
            (my_round(pl.col("bb_pf"), 3)).alias("BB_PF"),
            (my_round(pl.col("k_pf"), 3)).alias("K_PF"),
            (my_round(pl.col("runs_pf"), 3)).alias("RUNS_PF")
        ])
        ).with_columns([
                    (
                (pl.col("runs_pf") * pl.col("home_g") / (pl.col("home_g") + pl.col("away_g"))) + 
                ((pl.col("League_Count") - pl.col("runs_pf")) / (pl.col("League_Count") - 1)) * pl.col("away_g") / (pl.col("home_g") + pl.col("away_g"))
            ).alias("bpf/100")
        ])
    
    return pf

def cal_pa_count(data):
    pa_count_list = []
    count_list = []
    event_df_list = []
    for i in range(len(data)):
        count = data["B-S"][i]
        des = data["description"][i]
        event = data["events"][i]
        # イベントがNaNでない場合は、打席結果の行であるかどうかを確認
        if not pd.isnull(event):
            pa_count_list.append(count)
            count_list.append(pa_count_list)
            event_df_list.append(data.loc[i])
            if (event != "pickoff_1b") and (event != "pickoff_2b") and (event != "pickoff_catcher") and (event != "caught_stealing") and (event != "stolen_base") and (event != "wild_pitch") and (event != "balk") and (event != "passed_ball") and (event != "caught_stealing") and (event != "pickoff_1b"):
                # 打席結果がある場合、カウントのリストを保存し、新しい打席のカウントを初期化
                pa_count_list = []
            else:
                pass
        else:
            # イベントがNaNの場合は、カウントを打席のカウントリストに追加
            pa_count_list.append(count)

    count_str_list = []
    for c in count_list:
        count_str = "|".join(c)
        count_str_list.append(count_str)
    
    return count_str_list

def connection_db():
    """DB接続"""
    SQLALCHEMY_DATABASE_URL = "sqlite:///hoge.db"
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
    )
    return engine

def batting_stats(
        season = 2025, stats_type = 0, bat_league = None, bat_team = None, fld_team = None, batter_pos = None, bat_side = None,
        min_PA = "Q", game_type = "レギュラーシーズン", pitch_type = None, start_date = None, end_date = None, pitch_side=None,
        lineup = None, runners = None, out_counts=None
        ):
    engine = connection_db()
    user = st.secrets["mysql"]["user"]     # ユーザ名
    password = st.secrets["mysql"]["password"] # パスワード
    host = st.secrets["mysql"]["host"]    # ホスト名 or IP
    db = st.secrets["mysql"]["database"]       # データベース
    port = st.secrets["mysql"]["port"]         # ポート

    url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8'

    # engine作成
    engine = sa.create_engine(url, echo=False)
    team_en_dict = {
        "オリックス": "B", "ロッテ": "M", "ソフトバンク": "H", "楽天": "E", "西武": "L", "日本ハム": "F", 
        "阪神": "T", "広島": "C", "DeNA": "DB", "巨人": "G", "ヤクルト": "S", "中日": "D",
        "オイシックス": "A", "くふうハヤテ": "V"
    }

    where_list = []
    if season:
        where_list.append(f'game_year = {season}')
    if bat_league != "All Leagues":
        where_list.append(f'bat_league = "{bat_league}"')
    if bat_team != "All Teams":
        where_list.append(f'bat_team = "{bat_team}"')
    if fld_team != "All Teams":
        where_list.append(f'fld_team = "{fld_team}"')
    pos_en_dict = {
        "P": "投", "C": "捕", "1B": "一", "2B": "二", "3B": "三", "SS": "遊",
        "LF": "左", "CF": "中", "RF": "右", "DH": "指", "PH": "打", "PR": "走"
    }
    if batter_pos != "All":
        if batter_pos == "IF":
            where_list.append(f'batter_pos IN ("一", "二", "三", "遊")')
        elif batter_pos == "OF":
            where_list.append(f'batter_pos IN ("左", "中", "右")')
        elif batter_pos == "NP":
            where_list.append(f'batter_pos != "投"')
        elif batter_pos == "SM":
            where_list.append(f'StL = 1')
        else:
            where_list.append(f'batter_pos = "{pos_en_dict[batter_pos]}"')
    side_dict = {
        "Right": "右", "Left": "左"
    }
    if bat_side != "Both":
        where_list.append(f'stand = "{side_dict[bat_side]}"')

    if pitch_side != "Both":
        where_list.append(f'p_throw = "{side_dict[pitch_side]}"')

    if game_type == "レギュラーシーズン":
        where_list.append(f'game_type IN ("セ・リーグ", "パ・リーグ", "セ・パ交流戦")')
    elif game_type == "交流戦":
        where_list.append(f'game_type = "セ・パ交流戦"')
    else:
        where_list.append(f'game_type IN ("セ・リーグ", "パ・リーグ")')
    if start_date:
        where_list.append(f'game_date >= "{start_date}"')
    if end_date:
        where_list.append(f'game_date <= "{end_date}"')
    if pitch_type:
        where_list.append(f'pitch_type = "{pitch_type}"')
    if lineup == "All":
        pass
    elif lineup == "Batting 1st~3rd":
        pass
    elif "~" in lineup:
        lineup_s = lineup.split(" ")[-1][0]
        lineup_e = lineup.split(" ")[-1].split("~")[-1][0]
        where_list.append(f'`order` >= {lineup_s} AND `order` <= {lineup_e}')
    else:
        lineup_no = lineup.split(" ")[-1][0]
        where_list.append(f'`order` = {lineup_no}')

    runner_dict = {
        "Bases Empty": "000", "Bases Loaded": "111", "Runner at 1st": "100", 
        "Runners at 1st & 2nd": "110", "Runners at 1st & 3rd": "101", 
        "Runner at 2nd": "010", "Runners at 2nd and 3rd": "011", "Runner at 3rd": "001"
    }
    if runners == "No Split":
        pass
    elif runners == "Runners on Base":
        where_list.append(f'runner_id != "000"')
    elif runners == "Runners on Scoring":
        where_list.append(f'runner_id IN ("010", "001", "101", "011", "111")')
    else:
        runner_id = runner_dict[runners]
        where_list.append(f'runner_id = "{runner_id}"')

    if out_counts == "No Split":
        pass
    else:
        where_list.append(f'out_count = {out_counts[0]}')
    if where_list:
        where_q = "WHERE " + " AND ".join(where_list) + " "
    else:
        where_q = ""

    if stats_type == 0:
        group_q = "bat_league, bat_team, batter_id"
    elif stats_type == 1:
        group_q = "bat_league, bat_team"
    else:
        group_q = "bat_league"

    q = q_text(f"""
                SELECT 
                    {group_q},
                    COUNT(DISTINCT game_id) AS game_count,
                    SUM(PA_event) AS PA_count,
                    SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS hr_count,
                    SUM(CASE WHEN events = 'single' THEN 1 ELSE 0 END) AS single_count,
                    SUM(CASE WHEN events = 'double' THEN 1 ELSE 0 END) AS double_count,
                    SUM(CASE WHEN events = 'triple' THEN 1 ELSE 0 END) AS triple_count,
                    SUM(CASE WHEN events IN ('sac_bunt', 'bunt_error', 'bunt_fielders_choice') THEN 1 ELSE 0 END) AS sh_count,
                    SUM(CASE WHEN events IN ('sac_fly', 'sac_fly_error') THEN 1 ELSE 0 END) AS sf_count,
                    SUM(CASE WHEN events = 'double_play' THEN 1 ELSE 0 END) AS gdp_count,
                    SUM(CASE WHEN events IN ('strike_out', 'uncaught_third_strike') THEN 1 ELSE 0 END) AS so_count,
                    SUM(CASE WHEN events IN ('walk', 'intentional_walk') THEN 1 ELSE 0 END) AS bb_count,
                    SUM(CASE WHEN events = 'intentional_walk' THEN 1 ELSE 0 END) AS ibb_count,
                    SUM(CASE WHEN events = 'hit_by_pitch' THEN 1 ELSE 0 END) AS hbp_count,
                    SUM(CASE WHEN events = 'obstruction' THEN 1 ELSE 0 END) AS obstruction_count,
                    SUM(CASE WHEN events = 'interference' THEN 1 ELSE 0 END) AS interference_count,
                    SUM(GB) AS gb_count,
                    SUM(FB) AS fb_count,
                    SUM(IFFB) AS iffb_count,
                    SUM(OFFB) AS offb_count,
                    SUM(LD) AS ld_count,
                    SUM(Pull) AS pull_count,
                    SUM(Center) AS cent_count,
                    SUM(Opposite) AS oppo_count,
                    SUM(IFH) AS ifh_count
                FROM all2425
                {where_q}
                GROUP BY {group_q}
                ORDER BY 
                    PA_count DESC, hr_count DESC, single_count DESC, double_count DESC, triple_count DESC, 
                    game_count DESC, sh_count DESC, sf_count DESC, gdp_count DESC, so_count DESC, bb_count DESC, ibb_count DESC,
                    hbp_count DESC, obstruction_count DESC, interference_count DESC, gb_count DESC, fb_count DESC, iffb_count DESC,
                    offb_count DESC, ld_count DESC, pull_count DESC, cent_count DESC, oppo_count DESC, ifh_count DESC;         
            """
        )
    df = pd.read_sql(sql=q, con=engine.connect())
    lg_sb_q = q_text(f"""
                    SELECT *
                    FROM sb_data
                    WHERE game_year = {season};        
                """
            )
    lg_sb_df = pd.read_sql(sql=lg_sb_q, con=engine.connect())
    
    value_q = q_text(f"""
                    SELECT *
                    FROM value;        
                """
            )
    value_df = pd.read_sql(sql=value_q, con=engine.connect())
    pf_q = q_text(f"""
                    SELECT League, Team, bpf100
                    FROM parkfactor;        
                """
            )
    pf = pd.read_sql(sql=pf_q, con=engine.connect())
    rpw_q = q_text(f"""
                    SELECT League, RPW
                    FROM rpw
                    WHERE game_year = {season};        
                """
            )
    rpw_df = pd.read_sql(sql=rpw_q, con=engine.connect())
    bb_value = value_df[value_df["events"] == "bb"].iloc[0]["values"]
    bb_sum = value_df[value_df["events"] == "bb"].iloc[0]["sum"]
    hbp_value = value_df[value_df["events"] == "hbp"].iloc[0]["values"]
    hbp_sum = value_df[value_df["events"] == "hbp"].iloc[0]["sum"]
    single_value = value_df[value_df["events"] == "single"].iloc[0]["values"]
    single_sum = value_df[value_df["events"] == "single"].iloc[0]["sum"]
    double_value = value_df[value_df["events"] == "double"].iloc[0]["values"]
    double_sum = value_df[value_df["events"] == "double"].iloc[0]["sum"]
    triple_value = value_df[value_df["events"] == "triple"].iloc[0]["values"]
    triple_sum = value_df[value_df["events"] == "triple"].iloc[0]["sum"]
    hr_value = value_df[value_df["events"] == "hr"].iloc[0]["values"]
    hr_sum = value_df[value_df["events"] == "hr"].iloc[0]["sum"]
    sf_sum = value_df[value_df["events"] == "sf"].iloc[0]["sum"]
    ab_sum = value_df[value_df["events"] == "ab"].iloc[0]["sum"]
    h_sum = single_sum+double_sum+triple_sum+hr_sum
    mean_woba = (bb_value * bb_sum + hbp_value * hbp_sum + single_value * single_sum + double_value * double_sum + triple_value * triple_sum + hr_value * hr_sum)/(ab_sum + bb_sum + hbp_sum + sf_sum)
    mean_obp = (h_sum + bb_sum + hbp_sum)/(ab_sum + bb_sum + hbp_sum + sf_sum)
    wOBA_scale = mean_obp/mean_woba

    league_data_q = q_text(f"""
                SELECT 
                    bat_league,
                    COUNT(DISTINCT game_id) AS game_count,
                    SUM(runs_scored) AS R_count,
                    SUM(PA_event) AS PA_count,
                    SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS hr_count,
                    SUM(CASE WHEN events = 'single' THEN 1 ELSE 0 END) AS single_count,
                    SUM(CASE WHEN events = 'double' THEN 1 ELSE 0 END) AS double_count,
                    SUM(CASE WHEN events = 'triple' THEN 1 ELSE 0 END) AS triple_count,
                    SUM(CASE WHEN events IN ('sac_bunt', 'bunt_error', 'bunt_fielders_choice') THEN 1 ELSE 0 END) AS sh_count,
                    SUM(CASE WHEN events IN ('sac_fly', 'sac_fly_error') THEN 1 ELSE 0 END) AS sf_count,
                    SUM(CASE WHEN events = 'double_play' THEN 1 ELSE 0 END) AS gdp_count,
                    SUM(CASE WHEN events IN ('strike_out', 'uncaught_third_strike') THEN 1 ELSE 0 END) AS so_count,
                    SUM(CASE WHEN events IN ('walk', 'intentional_walk') THEN 1 ELSE 0 END) AS bb_count,
                    SUM(CASE WHEN events = 'intentional_walk' THEN 1 ELSE 0 END) AS ibb_count,
                    SUM(CASE WHEN events = 'hit_by_pitch' THEN 1 ELSE 0 END) AS hbp_count,
                    SUM(CASE WHEN events = 'obstruction' THEN 1 ELSE 0 END) AS obstruction_count,
                    SUM(CASE WHEN events = 'interference' THEN 1 ELSE 0 END) AS interference_count,
                    SUM(GB) AS gb_count,
                    SUM(FB) AS fb_count,
                    SUM(IFFB) AS iffb_count,
                    SUM(OFFB) AS offb_count,
                    SUM(LD) AS ld_count,
                    SUM(Pull) AS pull_count,
                    SUM(Center) AS cent_count,
                    SUM(Opposite) AS oppo_count,
                    SUM(IFH) AS ifh_count
                FROM all2425
                WHERE game_year = {season}
                GROUP BY bat_league
                ORDER BY 
                    PA_count DESC, hr_count DESC, single_count DESC, double_count DESC, triple_count DESC, 
                    game_count DESC, sh_count DESC, sf_count DESC, gdp_count DESC, so_count DESC, bb_count DESC, ibb_count DESC,
                    hbp_count DESC, obstruction_count DESC, interference_count DESC, gb_count DESC, fb_count DESC, iffb_count DESC,
                    offb_count DESC, ld_count DESC, pull_count DESC, cent_count DESC, oppo_count DESC, ifh_count DESC;         
            """
        )
    league_bat_data = pd.read_sql(sql=league_data_q, con=engine.connect())
    league_bat_data = league_bat_data.set_axis(["League", "g_1", "R", "PA", "HR", "1B", "2B", "3B", "SH", "SF", "GDP", "SO", "BB", "IBB", "HBP", 
                        "obstruction", "interference", "GB", "FB", "IFFB", "OFFB", "LD", "Pull", "Cent", "Oppo", "IFH"], axis=1)
    league_bat_data = pl.from_pandas(league_bat_data).with_columns([
        (pl.col("PA") - (pl.col("BB") + pl.col("HBP") + pl.col("SH") + pl.col("SF") + pl.col("obstruction"))).alias("AB"),
        (pl.col("1B") + pl.col("2B") + pl.col("3B") + pl.col("HR")).alias("H")
    ]).with_columns([
        (pl.col("H")/pl.col("AB")).alias("avg"),
        ((pl.col("H") + pl.col("BB") + pl.col("HBP"))/(pl.col("AB") + pl.col("BB") + pl.col("HBP") + pl.col("SF"))).alias("obp"),
        ((pl.col("1B") + 2*pl.col("2B") + 3*pl.col("3B") + 4*pl.col("HR"))/pl.col("AB")).alias("slg"),
        (wOBA_scale*(bb_value*(pl.col("BB") - pl.col("IBB")) + hbp_value*pl.col("HBP") + single_value*pl.col("1B") + double_value*pl.col("2B") + triple_value*pl.col("3B") + hr_value*pl.col("HR"))/(pl.col("AB") + pl.col("BB") - pl.col("IBB") + pl.col("HBP") + pl.col("SF"))
        ).alias("woba"),
        (pl.col("SO")/pl.col("AB")).alias("k%"),
        (pl.col("BB")/pl.col("AB")).alias("bb%"),
        (pl.col("GB")/(pl.col("GB") + pl.col("FB") + pl.col("LD"))).alias("gb%"),
        (pl.col("FB")/(pl.col("GB") + pl.col("FB") + pl.col("LD"))).alias("fb%"),
        (pl.col("LD")/(pl.col("GB") + pl.col("FB") + pl.col("LD"))).alias("ld%"),
        (pl.col("Pull")/(pl.col("Pull") + pl.col("Cent") + pl.col("Oppo"))).alias("pull%"),
        (pl.col("Cent")/(pl.col("Pull") + pl.col("Cent") + pl.col("Oppo"))).alias("cent%"),
        (pl.col("Oppo")/(pl.col("Pull") + pl.col("Cent") + pl.col("Oppo"))).alias("oppo%"),
        ((pl.col("H") - pl.col("HR"))/(pl.col("AB") - pl.col("SO") - pl.col("HR") + pl.col("SF"))).alias("babip")
    ]).with_columns([
        (pl.col("obp") + pl.col("slg")).alias("ops"),
        (pl.col("slg") - pl.col("avg")).alias("iso")
    ])

    runner = ["100", "010", "001", "110", "101", "011", "111"]
    sb_data_list = []
    lg_sb_df = pl.from_pandas(lg_sb_df)
    for r in runner:
        sb_data = lg_sb_df.filter(pl.col("runner_id") == r)
        
        if r[0] == "1":
            sb_1b = sb_data.select(["bat_league", "bat_team", "on_1b", "des"])
            if sb_1b.height > 0:
                sb_1b = sb_1b.with_columns([
                    (pl.col("des").str.contains("盗塁成功") & pl.col("des").str.contains(pl.col("on_1b"))).cast(pl.Int64).alias("StolenBase"),
                    (pl.col("des").str.contains("盗塁失敗") & pl.col("des").str.contains(pl.col("on_1b"))).cast(pl.Int64).alias("CaughtStealing")
                ])
                sb_data_1 = sb_1b.group_by(["bat_league", "bat_team", "on_1b"]).agg([
                    pl.col("StolenBase").sum().alias("SB"),
                    pl.col("CaughtStealing").sum().alias("CS")
                ]).rename({"on_1b": "runner_name"})
                sb_data_list.append(sb_data_1)
        
        if r[1] == "1":
            sb_2b = sb_data.select(["bat_league", "bat_team", "on_2b", "des"])
            if sb_2b.height > 0:
                sb_2b = sb_2b.with_columns([
                    (pl.col("des").str.contains("盗塁成功") & pl.col("des").str.contains(pl.col("on_2b"))).cast(pl.Int64).alias("StolenBase"),
                    (pl.col("des").str.contains("盗塁失敗") & pl.col("des").str.contains(pl.col("on_2b"))).cast(pl.Int64).alias("CaughtStealing")
                ])
                sb_data_2 = sb_2b.group_by(["bat_league", "bat_team", "on_2b"]).agg([
                    pl.col("StolenBase").sum().alias("SB"),
                    pl.col("CaughtStealing").sum().alias("CS")
                ]).rename({"on_2b": "runner_name"})
                sb_data_list.append(sb_data_2)
        
        if r[2] == "1":
            sb_3b = sb_data.select(["bat_league", "bat_team", "on_3b", "des"])
            if sb_3b.height > 0:
                sb_3b = sb_3b.with_columns([
                    (pl.col("des").str.contains("盗塁成功") & pl.col("des").str.contains(pl.col("on_3b"))).cast(pl.Int64).alias("StolenBase"),
                    (pl.col("des").str.contains("盗塁失敗") & pl.col("des").str.contains(pl.col("on_3b"))).cast(pl.Int64).alias("CaughtStealing")
                ])
                sb_data_3 = sb_3b.group_by(["bat_league", "bat_team", "on_3b"]).agg([
                    pl.col("StolenBase").sum().alias("SB"),
                    pl.col("CaughtStealing").sum().alias("CS")
                ]).rename({"on_3b": "runner_name"})
                sb_data_list.append(sb_data_3)

    # データフレームを連結
    sb_data = pl.concat(sb_data_list)

    runner_df = sb_data.group_by(["bat_league"]).agg(
        pl.col("SB").sum().alias("SB"),
        pl.col("CS").sum().alias("CS"),
    ).sort("SB", descending=True)
    runner_df = runner_df.rename({"bat_league": "League"})
    league_bat_data = league_bat_data.join(runner_df,on=["League"], how="left")

    league_wrc_mean = league_bat_data[["League", "woba", "R", "PA", "SB", "CS", "1B", "BB", "HBP", "IBB", 
                                       "avg", "obp", "slg", "ops", "k%", "bb%", "iso",
                                       "gb%", "ld%", "fb%", "pull%", "cent%", "oppo%", "babip"]].rename({
        "woba": "woba_league", "R": "R_league", "PA": "PA_league", "SB": "SB_league", "CS": "CS_league",
        "1B": "1B_league", "BB": "BB_league", "HBP": "HBP_league", "IBB": "IBB_league", "avg": "avg_league",
        "obp": "obp_league", "slg": "slg_league", "ops": "ops_league", "k%": "k%_league", "bb%": "bb%_league", "iso": "iso_league",
        "gb%": "gb%_league", "ld%": "ld%_league", "fb%": "fb%_league", "pull%": "pull%_league", "cent%": "cent%_league", "oppo%": "oppo%_league",
        "babip": "babip_league"
        })
    
    league_game_q = q_text(f"""
                    SELECT game_type,
                        COUNT(DISTINCT game_id) AS game_count
                    FROM all2425
                    WHERE game_year = {season}
                    GROUP BY game_type;        
                """
            )
    league_game = pd.read_sql(sql=league_game_q, con=engine.connect()).rename(columns={"game_type": "League"})
    try:
        kouryusen = league_game[league_game["League"] == "セ・パ交流戦"].iloc[0]["game_count"]
    except:
        kouryusen = 0

    if stats_type == 0:
        pa_q = q_text(f"""
                SELECT batter_id, Q
                FROM pa{season};        
            """
        )
        season_pa = pd.read_sql(sql=pa_q, con=engine.connect())

        df = pd.merge(df, season_pa, on="batter_id", how="left")

        player_q = q_text(f"""
                        SELECT 
                            Player,
                            ID
                        FROM people{season};        
                    """
                )
        player_df = pd.read_sql(sql=player_q, con=engine.connect())
        player_df = player_df.set_axis(["batter_name", "batter_id"], axis=1)
        df = pd.merge(df, player_df, on=["batter_id"])
        df = df.set_axis(["League", "Team", "BatterID", "G", "PA", "HR", "1B", "2B", "3B", "SH", "SF", "GDP", "SO", "BB", "IBB", "HBP", 
                        "obstruction", "interference", "GB", "FB", "IFFB", "OFFB", "LD", "Pull", "Cent", "Oppo", "IFH", "Q", "Player"], axis=1)
        if min_PA == "Qualified":
            df = df[df["Q"] == 1].reset_index(drop=True)
        else:
            df = df[df["PA"] >= int(min_PA)].reset_index(drop=True)

        stats_cols = ["Team", "Player", "Q"]
    elif stats_type == 1:
        df = df.set_axis(["League", "Team", "G", "PA", "HR", "1B", "2B", "3B", "SH", "SF", "GDP", "SO", "BB", "IBB", "HBP", 
                        "obstruction", "interference", "GB", "FB", "IFFB", "OFFB", "LD", "Pull", "Cent", "Oppo", "IFH"], axis=1)
        stats_cols = ["League", "Team"]
    else:
        df = df.set_axis(["League", "G", "PA", "HR", "1B", "2B", "3B", "SH", "SF", "GDP", "SO", "BB", "IBB", "HBP", 
                        "obstruction", "interference", "GB", "FB", "IFFB", "OFFB", "LD", "Pull", "Cent", "Oppo", "IFH"], axis=1)
        df["G"] = df["G"]*2 - kouryusen
        stats_cols = ["League"]

    df = pl.from_pandas(df)
    df = (df.with_columns([
                (pl.col("PA") - (pl.col("BB") + pl.col("HBP") + pl.col("SH") + pl.col("SF") + pl.col("obstruction"))).alias("AB"),
                (pl.col("1B") + pl.col("2B") + pl.col("3B") + pl.col("HR")).alias("H"),
            ]).with_columns([
                (pl.col("H")/pl.col("AB")).alias("avg"),
                ((pl.col("H") + pl.col("BB") + pl.col("HBP"))/(pl.col("AB") + pl.col("BB") + pl.col("HBP") + pl.col("SF"))).alias("obp"),
                ((pl.col("1B") + 2 * pl.col("2B") + 3 * pl.col("3B") + 4 * pl.col("HR"))/pl.col("AB")).alias("slg"),
                ((pl.col("H") - pl.col("HR"))/(pl.col("AB") - pl.col("SO") - pl.col("HR") + pl.col("SF"))).alias("babip"),
                (pl.col("SO")/pl.col("PA")).alias("k%"),
                (pl.col("BB")/pl.col("PA")).alias("bb%"),
                (pl.col("BB")/pl.col("SO")).alias("bb/k"),
                (pl.col("GB")/pl.col("FB")).alias("gb/fb"),
                (pl.col("GB")/(pl.col("GB") + pl.col("FB") + pl.col("LD"))).alias("gb%"),
                (pl.col("FB")/(pl.col("GB") + pl.col("FB") + pl.col("LD"))).alias("fb%"),
                (pl.col("LD")/(pl.col("GB") + pl.col("FB") + pl.col("LD"))).alias("ld%"),
                (pl.col("IFFB")/pl.col("FB")).alias("iffb%"),
                (pl.col("HR")/pl.col("FB")).alias("hr/fb"),
                (pl.col("Pull")/(pl.col("Pull") + pl.col("Cent") + pl.col("Oppo"))).alias("pull%"),
                (pl.col("Cent")/(pl.col("Pull") + pl.col("Cent") + pl.col("Oppo"))).alias("cent%"),
                (pl.col("Oppo")/(pl.col("Pull") + pl.col("Cent") + pl.col("Oppo"))).alias("oppo%"),
                (pl.col("IFH")/pl.col("GB")).alias("ifh%"),
                (wOBA_scale * (bb_value * (pl.col("BB") - pl.col("IBB")) + hbp_value * pl.col("HBP") + single_value * pl.col("1B") + double_value * pl.col("2B") + triple_value * pl.col("3B") + hr_value * pl.col("HR"))/(pl.col("AB") + pl.col("BB") - pl.col("IBB") + pl.col("HBP") + pl.col("SF"))).alias("woba")
            ]).with_columns([
                (pl.col("obp") + pl.col("slg")).alias("ops"),
                (pl.col("slg") - pl.col("avg")).alias("iso")
            ]).with_columns([
                (my_round(pl.col("avg"), 3)).alias("AVG"),
                (my_round(pl.col("obp"), 3)).alias("OBP"),
                (my_round(pl.col("slg"), 3)).alias("SLG"),
                (my_round(pl.col("ops"), 3)).alias("OPS"),
                (my_round(pl.col("iso"), 3)).alias("ISO"),
                (my_round(pl.col("babip"), 3)).alias("BABIP"),
                (my_round(pl.col("k%"), 3)).alias("K%"),
                (my_round(pl.col("bb%"), 3)).alias("BB%"),
                (my_round(pl.col("bb/k"), 3)).alias("BB/K"),
                (my_round(pl.col("gb/fb"), 3)).alias("GB/FB"),
                (my_round(pl.col("gb%"), 3)).alias("GB%"),
                (my_round(pl.col("fb%"), 3)).alias("FB%"),
                (my_round(pl.col("ld%"), 3)).alias("LD%"),
                (my_round(pl.col("iffb%"), 3)).alias("IFFB%"),
                (my_round(pl.col("hr/fb"), 3)).alias("HR/FB"),
                (my_round(pl.col("pull%"), 3)).alias("Pull%"),
                (my_round(pl.col("cent%"), 3)).alias("Cent%"),
                (my_round(pl.col("oppo%"), 3)).alias("Oppo%"),
                (my_round(pl.col("ifh%"), 3)).alias("IFH%"),
                (my_round(pl.col("woba"), 3)).alias("wOBA")
            ]).join(
                league_wrc_mean, on="League", how="left"
            ).with_columns([
                (100*pl.col("avg")/pl.col("avg_league")).alias("avg+"),
                (100*pl.col("obp")/pl.col("obp_league")).alias("obp+"),
                (100*pl.col("slg")/pl.col("slg_league")).alias("slg+"),
                (100*pl.col("k%")/pl.col("k%_league")).alias("k%+"),
                (100*pl.col("bb%")/pl.col("bb%_league")).alias("bb%+"),
                (100*pl.col("iso")/pl.col("iso_league")).alias("iso+"),
                (100*pl.col("fb%")/pl.col("fb%_league")).alias("fb%+"),
                (100*pl.col("ld%")/pl.col("ld%_league")).alias("ld%+"),
                (100*pl.col("gb%")/pl.col("gb%_league")).alias("gb%+"),
                (100*pl.col("pull%")/pl.col("pull%_league")).alias("pull%+"),
                (100*pl.col("oppo%")/pl.col("oppo%_league")).alias("oppo%+"),
                (100*pl.col("cent%")/pl.col("cent%_league")).alias("cent%+"),
                (100*pl.col("babip")/pl.col("babip_league")).alias("babip+"),
                (((pl.col("woba") - pl.col("woba_league"))/wOBA_scale) * pl.col("PA")).alias("wraa"),
                (((pl.col("woba") - pl.col("woba_league")*0.88)/wOBA_scale) * pl.col("PA")).alias("wrar"),
                ((((pl.col("woba") - pl.col("woba_league"))/wOBA_scale) + pl.col("R_league")/pl.col("PA_league")) * pl.col("PA")).alias("wrc")
            ]).with_columns([
                (my_round(pl.col("avg+"))).alias("AVG+"),
                (my_round(pl.col("obp+"))).alias("OBP+"),
                (my_round(pl.col("slg+"))).alias("SLG+"),
                (my_round(pl.col("iso+"))).alias("ISO+"),
                (my_round(pl.col("k%+"))).alias("K%+"),
                (my_round(pl.col("bb%+"))).alias("BB%+"),
                (my_round(pl.col("gb%+"))).alias("GB%+"),
                (my_round(pl.col("fb%+"))).alias("FB%+"),
                (my_round(pl.col("ld%+"))).alias("LD%+"),
                (my_round(pl.col("pull%+"))).alias("Pull%+"),
                (my_round(pl.col("oppo%+"))).alias("Oppo%+"),
                (my_round(pl.col("cent%+"))).alias("Cent%+"),
                (my_round(pl.col("babip+"))).alias("BABIP+"),
                (my_round(pl.col("wraa"), 1)).alias("wRAA"),
                (my_round(pl.col("wrar"), 1)).alias("wRAR"),
                (my_round(pl.col("wrc"))).alias("wRC")
            ]).join(
                pl.from_pandas(rpw_df), on="League", how="left"
            )
            )

    if stats_type == 2:
        df = df.with_columns([
            (100*(pl.col("wrc")/pl.col("PA"))/(pl.col("R_league")/pl.col("PA_league"))).alias("wrc+"),
            (100*((pl.col("obp")/pl.col("obp_league")) + (pl.col("slg")/pl.col("slg_league")) -1)).alias("ops+"),
            (pl.col("wrar")/pl.col("RPW")).alias("batwar")
        ])
    else:
        df = (df.join(pl.from_pandas(pf[["Team", "bpf100"]]), on="Team", how="left")
            .with_columns([
                (pl.col("wrc") + (1 - pl.col("bpf100"))*pl.col("PA")*(pl.col("R_league")/pl.col("PA_league"))/pl.col("bpf100")).alias("wrc_pf"),
                (((pl.col("woba") - pl.col("woba_league")*pl.col("bpf100")*0.88)/wOBA_scale) * pl.col("PA")).alias("wrar_pf"),
                (100*((pl.col("obp")/pl.col("obp_league")) + (pl.col("slg")/pl.col("slg_league")) -1)/pl.col("bpf100")).alias("ops+")
            ])
            .with_columns([
                (100*(pl.col("wrc_pf")/pl.col("PA"))/(pl.col("R_league")/pl.col("PA_league"))).alias("wrc+"),
                (pl.col("wrar")/pl.col("RPW")).alias("batwar")
            ])
        )

    df = df.with_columns([
        (my_round(pl.col("wrc+"))).alias("wRC+"),
        (my_round(pl.col("batwar"), 1)).alias("batWAR"),
        (my_round(pl.col("ops+"))).alias("OPS+")
    ])

    plate_q = q_text(f"""
                SELECT 
                    {group_q},
                    COUNT(*) AS n_count,
                    SUM(swing) AS swing_count,
                    SUM(contact) AS contact_count,
                    SUM(CASE WHEN description = 'swing_strike' THEN 1 ELSE 0 END) AS swstr_count,
                    SUM(CASE WHEN description = 'called_strike' THEN 1 ELSE 0 END) AS cstr_count,
                    SUM(CASE WHEN Zone = 'In' THEN 1 ELSE 0 END) AS z_count,
                    SUM(CASE WHEN Zone = 'In' AND swing = 1 THEN 1 ELSE 0 END) AS z_swing_count,
                    SUM(CASE WHEN Zone = 'In' AND contact = 1 THEN 1 ELSE 0 END) AS z_contact_count,
                    SUM(CASE WHEN Zone = 'Out' THEN 1 ELSE 0 END) AS o_count,
                    SUM(CASE WHEN Zone = 'Out' AND swing = 1 THEN 1 ELSE 0 END) AS o_swing_count,
                    SUM(CASE WHEN Zone = 'Out' AND contact = 1 THEN 1 ELSE 0 END) AS o_contact_count,
                    SUM(CASE WHEN ab_pitch_number = 1 THEN 1 ELSE 0 END) AS f_count,
                    SUM(CASE WHEN ab_pitch_number = 1 AND Zone = 'In' THEN 1 ELSE 0 END) AS f_zone_count,
                    SUM(CASE WHEN strikes = 2 THEN 1 ELSE 0 END) AS t_count,
                    SUM(CASE WHEN strikes = 2 AND events = 'strike_out' THEN 1 ELSE 0 END) AS t_so_count
                FROM all2425
                {where_q}
                GROUP BY {group_q};         
            """
        )
    plate_discipline = pd.read_sql(sql=plate_q, con=engine.connect())
    plate_discipline = pl.from_pandas(plate_discipline).with_columns([
        (pl.col("swing_count")/pl.col("n_count")).alias("swing%"),
        (pl.col("contact_count")/pl.col("swing_count")).alias("contact%"),
        (pl.col("z_count")/pl.col("n_count")).alias("zone%"),
        (pl.col("swstr_count")/pl.col("n_count")).alias("swstr%"),
        (pl.col("cstr_count")/pl.col("n_count")).alias("cstr%"),
        (pl.col("swstr_count")/pl.col("swing_count")).alias("whiff%"),
        ((pl.col("cstr_count") + pl.col("swstr_count"))/pl.col("n_count")).alias("csw%"),
    ]).with_columns([
        (my_round(pl.col("swing%"), 3)).alias("Swing%"),
        (my_round(pl.col("contact%"), 3)).alias("Contact%"),
        (my_round(pl.col("zone%"), 3)).alias("Zone%"),
        (my_round(pl.col("swstr%"), 3)).alias("SwStr%"),
        (my_round(pl.col("cstr%"), 3)).alias("CStr%"),
        (my_round(pl.col("whiff%"), 3)).alias("Whiff%"),
        (my_round(pl.col("csw%"), 3)).alias("CSW%"),
    ]).with_columns([
        (pl.col("o_swing_count")/pl.col("o_count")).alias("o-swing%"),
        (pl.col("o_contact_count")/pl.col("o_count")).alias("o-contact%"),
    ]).with_columns([
        (my_round(pl.col("o-swing%"), 3)).alias("O-Swing%"),
        (my_round(pl.col("o-contact%"), 3)).alias("O-Contact%"),
    ]).with_columns([
        (pl.col("z_swing_count")/pl.col("z_count")).alias("z-swing%"),
        (pl.col("z_contact_count")/pl.col("z_count")).alias("z-contact%"),
    ]).with_columns([
        (my_round(pl.col("z-swing%"), 3)).alias("Z-Swing%"),
        (my_round(pl.col("z-contact%"), 3)).alias("Z-Contact%"),
    ]).with_columns([
        (pl.col("f_zone_count")/pl.col("f_count")).alias("f-strike%"),
    ]).with_columns([
        (my_round(pl.col("f-strike%"), 3)).alias("F-Strike%"),
    ]).with_columns([
        (pl.col("t_so_count")/pl.col("t_count")).alias("putaway%"),
    ]).with_columns([
        (my_round(pl.col("putaway%"), 3)).alias("PutAway%"),
    ])

    
    if stats_type == 0:
        plate_discipline = plate_discipline.rename({"bat_league": "League", "bat_team": "Team", "batter_id": "BatterID"})
        df = df.join(plate_discipline, on=["League", "Team", "BatterID"], how="left")
    if stats_type == 1:
        plate_discipline = plate_discipline.rename({"bat_league": "League", "bat_team": "Team"})
        df = df.join(plate_discipline, on=["League", "Team"], how="left")
    if stats_type == 2:
        plate_discipline = plate_discipline.rename({"bat_league": "League"})
        df = df.join(plate_discipline, on=["League"], how="left")

    pt_list = ["FA", "FT", "SL", "CT", "CB", "CH", "SF", "SI", "SP", "XX"]
    for p in pt_list:
        p_low = p.lower()
        fa_q = q_text(f"""
                SELECT 
                    {group_q},
                    COUNT(*) AS {p}_count,
                    AVG(velocity) AS {p}_v_avg,
                    SUM(RV) AS {p}_w_count
                FROM all2425
                {where_q} AND {p} = 1
                GROUP BY {group_q};         
            """
        )
        fa_v = pd.read_sql(sql=fa_q, con=engine.connect())
        fa_v = pl.from_pandas(fa_v)
        if stats_type == 0:
            fa_v = fa_v.with_columns(
                pl.col("batter_id").cast(pl.Float64)
            )
        fa_v = fa_v.with_columns([
            pl.col(f"{p}_count").cast(pl.Int64),
            pl.col(f"{p}_v_avg").cast(pl.Float64),
            pl.col(f"{p}_w_count").cast(pl.Float64)
            ]
        )
        if stats_type == 0:
            fa_v = fa_v.rename({"bat_league": "League", "bat_team": "Team", "batter_id": "BatterID"})
            df = df.join(fa_v, on=["League", "Team", "BatterID"], how="left")
        elif stats_type == 1:
            fa_v = fa_v.rename({"bat_league": "League", "bat_team": "Team"})
            df = df.join(fa_v, on=["League", "Team"], how="left")
        elif stats_type == 2:
            fa_v = fa_v.rename({"bat_league": "League"})
            df = df.join(fa_v, on=["League"], how="left")

        df = df.with_columns([
            (my_round(pl.col(f"{p}_count")/pl.col("n_count"), 3)).alias(f"{p}%"),
            (my_round(pl.col(f"{p}_v_avg"), 1)).alias(f"{p}v"),
            (my_round(pl.col(f"{p}_w_count"), 1)).alias(f"w{p}")
        ]).with_columns([
            (my_round((100*pl.col(f"{p}_w_count")/pl.col(f"{p}_count")), 1)).alias(f"w{p}/C")
        ])
    sb_q = q_text(f"""
                    SELECT *
                    FROM sb_data
                    {where_q};        
                """
            )
    sb_df = pd.read_sql(sql=sb_q, con=engine.connect())
    sb_df = pl.from_pandas(sb_df)
    runner = ["100", "010", "001", "110", "101", "011", "111"]
    sb_data_list = []
    sb_run = value_df[value_df["events"] == "sb"].iloc[0]["runs"]
    cs_run = value_df[value_df["events"] == "cs"].iloc[0]["runs"]
    for r in runner:
        sb_data = sb_df.filter(pl.col("runner_id") == r)

        if r[0] == "1":
            sb_1b = sb_data.select(["bat_league", "bat_team", "on_1b", "des"])
            if sb_1b.shape[0] > 0:
                sb_1b = sb_1b.with_columns([
                    pl.when(pl.col("des").str.contains("盗塁成功") & pl.col("des").str.contains(pl.col("on_1b")))
                    .then(1).otherwise(0).alias("StolenBase"),
                    pl.when(pl.col("des").str.contains("盗塁失敗") & pl.col("des").str.contains(pl.col("on_1b")))
                    .then(1).otherwise(0).alias("CaughtStealing")
                ])
                sb_data_1 = sb_1b.group_by(["bat_league", "bat_team", "on_1b"]).agg([
                    pl.sum("StolenBase").alias("SB"),
                    pl.sum("CaughtStealing").alias("CS")
                ])
                sb_data_1 = sb_data_1.rename({"on_1b": "runner_name"})
                sb_data_list.append(sb_data_1)

        if r[1] == "1":
            sb_2b = sb_data.select(["bat_league", "bat_team", "on_2b", "des"])
            if sb_2b.shape[0] > 0:
                sb_2b = sb_2b.with_columns([
                    pl.when(pl.col("des").str.contains("盗塁成功") & pl.col("des").str.contains(pl.col("on_2b")))
                    .then(1).otherwise(0).alias("StolenBase"),
                    pl.when(pl.col("des").str.contains("盗塁失敗") & pl.col("des").str.contains(pl.col("on_2b")))
                    .then(1).otherwise(0).alias("CaughtStealing")
                ])
                sb_data_2 = sb_2b.group_by(["bat_league", "bat_team", "on_2b"]).agg([
                    pl.sum("StolenBase").alias("SB"),
                    pl.sum("CaughtStealing").alias("CS")
                ])
                sb_data_2 = sb_data_2.rename({"on_2b": "runner_name"})
                sb_data_list.append(sb_data_2)

        if r[2] == "1":
            sb_3b = sb_data.select(["bat_league", "bat_team", "on_3b", "des"])
            if sb_3b.shape[0] > 0:
                sb_3b = sb_3b.with_columns([
                    pl.when(pl.col("des").str.contains("盗塁成功") & pl.col("des").str.contains(pl.col("on_3b")))
                    .then(1).otherwise(0).alias("StolenBase"),
                    pl.when(pl.col("des").str.contains("盗塁失敗") & pl.col("des").str.contains(pl.col("on_3b")))
                    .then(1).otherwise(0).alias("CaughtStealing")
                ])
                sb_data_3 = sb_3b.group_by(["bat_league", "bat_team", "on_3b"]).agg([
                    pl.sum("StolenBase").alias("SB"),
                    pl.sum("CaughtStealing").alias("CS")
                ])
                sb_data_3 = sb_data_3.rename({"on_3b": "runner_name"})
                sb_data_list.append(sb_data_3)

    try:
        sb_data = pl.concat(sb_data_list)

        if stats_type == 0:
            runner_g = ["bat_league", "bat_team", "runner_name"]
        if stats_type == 1:
            runner_g = ["bat_league", "bat_team"]
        if stats_type == 2:
            runner_g = ["bat_league"]

        runner_df =sb_data.group_by(runner_g).agg(
            pl.sum("SB").alias("SB"),
            pl.sum("CS").alias("CS")
        ).sort("SB", descending=True)
        if stats_type <= 1:
            runner_df = runner_df.rename({"bat_team": "Team", "bat_league": "League"})
        elif stats_type == 2:
            runner_df = runner_df.rename({"bat_league": "League"})

        if stats_type == 0:
            df = df.with_columns([
                (pl.col("Player").str.replace(" ", "")).alias("batter_name_no_space")
            ])
            df = partial_match_merge(df, runner_df, 'batter_name_no_space', 'runner_name')
        elif stats_type == 1:
            df = df.join(runner_df,on=["League", "Team"], how="left")
        elif stats_type == 2:
            df = df.join(runner_df,on="League", how="left")

        df = df.with_columns([
            pl.col("SB").fill_null(0).cast(pl.Int32),
            pl.col("CS").fill_null(0).cast(pl.Int32)
        ])
    except:
        df = df.with_columns([
            pl.lit(0).alias("SB"),
            pl.lit(0).alias("CS")
        ])


    # スタートする変数 (sb_run, cs_run) が既に定義されている前提
    df = df.with_columns([
        # wSB_A列の計算
        ((pl.col("SB") * sb_run) + (pl.col("CS") * cs_run)).alias("wSB_A"),
        
        # wSB_B列の計算
        (((pl.col("SB_league") * sb_run) + (pl.col("CS_league") * cs_run)) / 
        (pl.col("1B_league") + pl.col("BB_league") + pl.col("HBP_league") + pl.col("IBB_league"))).alias("wSB_B"),
        
        # wSB_C列の計算
        (pl.col("1B") + pl.col("BB") - pl.col("IBB") + pl.col("HBP")).alias("wSB_C"),
    ]).with_columns([
        # wsb列の計算
        pl.when((pl.col("SB") == 0) & (pl.col("CS") == 0))
        .then(None)
        .otherwise(pl.col("wSB_A") - pl.col("wSB_B") * pl.col("wSB_C")).alias("wsb")
    ])

    df = df.with_columns([
        my_round(pl.col("wsb"), 1).alias("wSB")
    ])



    #df = df[stats_cols + ["G", "PA", "HR", "1B", "2B", "3B", "SH", "SF", "GDP", "SO", "BB", "IBB", "HBP", 
    #                    "obstruction", "interference", "GB", "FB", "IFFB", "OFFB", "LD", "Pull", "Cent", "Oppo", "IFH"]]

    df = df.to_pandas()

    return df
    