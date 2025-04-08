import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.patches import Rectangle
import glob
from matplotlib.patches import Polygon
from datetime import datetime, timedelta, date
import datetime as dt
import time
import math
import requests
from bs4 import BeautifulSoup
from source.my_func import cal_RE24, cal_PF, my_round, connection_db, batting_stats
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import text as q_text

st.set_page_config(layout='wide')
st.title("NPB Stats New")

year_list = [2025, 2024]
game_type_list = ["レギュラーシーズン", "レギュラーシーズン(交流戦以外)", "交流戦"]

stats_type_list = ["Batting", "Pitching"]
game_type_list = ["レギュラーシーズン", "レギュラーシーズン(交流戦以外)", "交流戦"]

pl_list = ["ソフトバンク", "日本ハム", "ロッテ", "楽天", "オリックス", "西武"]
cl_list = ["巨人", "阪神", "DeNA", "広島", "ヤクルト", "中日"]
  
team_en_dict = {
    "オリックス": "B", "ロッテ": "M", "ソフトバンク": "H", "楽天": "E", "西武": "L", "日本ハム": "F", 
    "阪神": "T", "広島": "C", "DeNA": "DB", "巨人": "G", "ヤクルト": "S", "中日": "D",
    "オイシックス": "A", "くふうハヤテ": "V"
}
pos_en_dict = {
    "P": "投", "C": "捕", "1B": "一", "2B": "二", "3B": "三", "SS": "遊",
    "LF": "左", "CF": "中", "RF": "右", "DH": "指", "PH": "打", "PR": "走"
}


cols = st.columns(2)
with cols[0]:
    stats_type = option_menu(None,
    stats_type_list,
    menu_icon="cast", default_index=0, orientation="horizontal",
    styles={
        "container": {"padding": "0!important"},
        "icon": {"font-size": "15px"},
        "nav-link": {"font-size": "15px", "text-align": "left", "margin": "0px"},
    }
    )

leaderboard_list = ["Player Stats", "Team Stats", "League Stats"]

with cols[1]:
    mode = option_menu(None,
    leaderboard_list,
    menu_icon="cast", default_index=0, orientation="horizontal",
    styles={
        "container": {"padding": "0!important"},
        "icon": {"font-size": "15px"},
        "nav-link": {"font-size": "15px", "text-align": "left", "margin": "0px"},
    }
    )
group_index = leaderboard_list.index(mode)
columns_list = ["League", "Team", "batter_id"]
col_list = columns_list[:(3-group_index)]


columns_list_2 = ["League", "Team", "Player"]
if group_index == 0:
    c_list = columns_list_2[1:]
else:
    c_list = [columns_list_2[2-group_index]]


cols = st.columns(5)
with cols[0]:
    game_year = st.selectbox(
        "Season",
        year_list,
        index=0)
with cols[1]:
    game_type = st.selectbox(
        "Season Type",
        game_type_list,
        index=0)
    
with cols[2]:
    league_select = st.selectbox(
        "League",
        ["All Leagues", "セ・リーグ", "パ・リーグ"],
        index=0
    )
if league_select == "All Leagues":
    team_list = ["All Teams"] + cl_list + pl_list
    vs_team_list = ["All Teams"] + cl_list + pl_list
elif league_select == "セ・リーグ":
    team_list = ["All Teams"] + cl_list
    vs_team_list = ["All Teams"] + cl_list + pl_list
elif league_select == "パ・リーグ":
    team_list = ["All Teams"] + pl_list
    vs_team_list = ["All Teams"] + pl_list + cl_list
with cols[3]:
    if group_index <= 1:
        team_select = st.selectbox(
            "Team",
            team_list,
            index=0
        )
    else:
        team_select = st.selectbox(
            "Team",
            ["All Teams"],
            index=0
        )
with cols[4]:
    if group_index <= 1:
        if team_select != "All Teams":
            if team_select in pl_list:
                vs_team_list = ["All Teams"] + pl_list + cl_list
            else:
                vs_team_list = ["All Teams"] + cl_list + pl_list
            vs_team_list.remove(team_select)
        vs_team_select = st.selectbox(
            "vs Team",
            vs_team_list,
            index=0
        )
    else:
        vs_team_select = st.selectbox(
            "vs Team",
            ["All Teams"],
            index=0
        )
cols = st.columns(5)
if stats_type == "Batting":
    pos_list = ["All", "IF", "OF", "NP"] + list(pos_en_dict) + ["SM"]
elif stats_type == "Pitching":
    pos_list = ["All", "SP", "RP"]
elif stats_type == "Fielding":
    pos_list = ["All"] + list(pos_en_dict)[:9]
with cols[0]:
    pos_select = st.selectbox(
        "Positional Split",
        pos_list,
        index=0
    )

with cols[1]:
    side_select = st.selectbox(
        "Bat Side",
        ["Both", "Right", "Left"],
        index=0
    )
with cols[2]:
    p_throw_select = st.selectbox(
        "Pitch Side",
        ["Both", "Right", "Left"],
        index=0
    )
    

if stats_type == "Batting":
    split_list = ["No Splits", "Grounders", "Flies", "Liners",
         "Pull", "Center", "Opposite", "Home", "Away",
         "0-0経由", "0-1経由", "0-2経由", "1-0経由", "1-1経由", "1-2経由", "2-0経由", "2-1経由", "2-2経由", "3-0経由", "3-1経由", "3-2経由", 
         "ストレート", "ツーシーム", "カットボール", "スライダー", "カーブ", "フォーク", "チェンジアップ", "シンカー", "特殊級", "ストレート150以上", "ストレート140~149", "ストレート140未満"]
elif stats_type == "Pitching":
    split_list = ["No Splits", "Grounders", "Flies", "Liners",
         "Home", "Away", "Bases Empty", "Runners on Base", "Runners on Scoring",
         "リード時", "同点時", "ビハインド時", "1回", "2回", "3回", "4回", "5回", "6回", "7回", "8回", "9回", "延長",
         "0-0経由", "0-1経由", "0-2経由", "1-0経由", "1-1経由", "1-2経由", "2-0経由", "2-1経由", "2-2経由", "3-0経由", "3-1経由", "3-2経由"]
elif stats_type == "Fielding":
    split_list = ["No Splits"]
with cols[3]:
    split = st.selectbox(
        "Split",
        split_list,
        index=0
    )
if group_index == 0:
    if stats_type == "Batting":
        pa_list = ['Qualified', '0', '1', '5', '10', '20', '30', '40', '50', '60', '70', '80', '90', 
                   '100', '110', '120', '130', '140', '150', '160', '170', '180', '190', 
                   '200', '210', '220', '230', '240', '250', '300', '350', '400', '450',
                   '500'
                   ]
    elif stats_type == "Pitching":
        pa_list = ['Qualified', '0', '1', '5', '10', '20', '30', '40', '50', '60', '70', '80', '90', 
                   '100', '110', '120', '130', '140', '150', '160', '170', '180', '190', '200']
    elif stats_type == "Fielding":
        pa_list = ['Qualified', '0', '1', '5', '10', '20', '30', '40', '50', '60', '70', '80', '90', 
                   '100', '110', '120', '130', '140', '150', '160', '170', '180', '190', 
                   '200', '210', '220', '230', '240', '250', '300', '350', '400', '450',
                   '500', '550', '600', '650', '700', '750', '800', '850', '900', '950'
                   ]
else:
    pa_list = ["Qualified"]
if stats_type == "Batting":
    min_text = 'Min PA'
elif stats_type == "Pitching":
    min_text = 'Min IP'
elif stats_type == "Fielding":
    min_text = 'Min Inn'

with cols[4]:
    min_PA = st.selectbox(
        min_text,
        pa_list,
        index = 0)
    if min_PA == "Qualified":
        q = 'Q == 1'
    else:
        if stats_type == "Batting":
            q = 'PA >=' + min_PA
        elif stats_type == "Pitching":
            q = 'IP >=' + min_PA
        elif stats_type == "Fielding":
            q = 'Inn >=' + min_PA

cols = st.columns(5)

lineup_list = [
    "All", "Batting 1st", "Batting 2nd", "Batting 3rd", "Batting 4th", "Batting 5th", 
    "Batting 6th", "Batting 7th", "Batting 8th", "Batting 9th", 
    "Batting 1st~3rd", "Batting 4th~6th", "Batting 7th~9th", "Batting 3rd~5th", "Batting 1st~5th", "Batting 6th~9th"]
with cols[0]:
    lineup_select = st.selectbox(
        "Batting Lineup",
        lineup_list,
        index = 0)
    
runner_list = ["No Split", "Bases Empty", "Runners on Base", "Runners on Scoring", "Bases Loaded",
         "Runner at 1st", "Runner at 2nd", "Runner at 3rd", 
         "Runners at 1st & 2nd", "Runners at 1st & 3rd", "Runners at 2nd & 3rd",]

with cols[1]:
    outs_select = st.selectbox(
        "Out Counts",
        ["No Split", "0 Outs", "1 Out", "2 Outs"],
        index=0
    )

with cols[2]:
    runner_select = st.selectbox(
        "Runners",
        runner_list,
        index=0
    )

if game_year == 2024:
    min_date = dt.date(2024, 3, 29)
    max_date = dt.date(2024, 10, 9)
elif game_year == 2025:
    min_date = dt.date(2025, 3, 28)
    max_date = dt.date(2025, 3, 30)
with cols[3]:
    start_date = st.date_input('Start Date', min_date, min_value=min_date, max_value=max_date)
with cols[4]:
    end_date = st.date_input('End Date', max_date, min_value=min_date, max_value=max_date)

tabs_list = ["Dashboard", "Standard", "Advanced", "Batted Ball", "Plate Discipline", "Pitch Type", "Pitch Value", "\+ Stats"]
tab = st.tabs(tabs_list)

df = batting_stats(
    season=game_year, stats_type=group_index, bat_league=league_select, bat_team=team_select, batter_pos=pos_select, bat_side=side_select, 
    min_PA=min_PA, game_type=game_type, start_date=start_date, end_date=end_date, fld_team=vs_team_select, pitch_side=p_throw_select,
    lineup=lineup_select, runners=runner_select, out_counts=outs_select
    )
if group_index == 0:
    df["Team"] = df["Team"].replace(team_en_dict)
df = df.sort_values("wRC+", ascending=False).reset_index(drop=False)
with tab[0]:
    bat_cols = c_list + ["G", "PA", "HR", "SB", "BB%", "K%", "ISO", "BABIP", "AVG", "OBP", "SLG", "wOBA", "wRC+", "batWAR"]
    bat_0 = df[bat_cols]
    df_style = bat_0.style.format({
        'PA': '{:.0f}',
        'HR': '{:.0f}',
        'SB': '{:.0f}',
        'K%': '{:.1%}',
        'BB%': '{:.1%}',
        'SB': '{:.0f}',
        'BB/K': '{:.2f}',
        'AVG': '{:.3f}',
        'OBP': '{:.3f}',
        'SLG': '{:.3f}',
        'OPS': '{:.3f}',
        'ISO': '{:.3f}',
        'BABIP': '{:.3f}',
        'wOBA': '{:.3f}',
        'wRC': '{:.0f}',
        'wRAA': '{:.1f}',
        'batWAR': '{:.1f}',
        'wRC+': '{:.0f}',
        'GB%': '{:.1%}',
        'FB%': '{:.1%}',
        'LD%': '{:.1%}',
        'IFFB%': '{:.1%}',
        'HR/FB': '{:.1%}',
    })
    st.dataframe(df_style, use_container_width=True)

with tab[1]:
    bat_cols = c_list + ["G", "AB", "PA", "H", "1B", "2B", "3B", "HR", "BB", "IBB", "SO", "HBP", "SF", "SH", "GDP", "SB", "CS", "AVG"]
    bat_1 = df[bat_cols]
    df_style = bat_1.style.format({
        'PA': '{:.0f}',
        'HR': '{:.0f}',
        'SB': '{:.0f}',
        'CS': '{:.0f}',
        'AB': '{:.0f}',
        'H': '{:.0f}',
        '1B': '{:.0f}',
        '2B': '{:.0f}',
        '3B': '{:.0f}',
        'BB': '{:.0f}',
        'IBB': '{:.0f}',
        'HBP': '{:.0f}',
        'SO': '{:.0f}',
        'SF': '{:.0f}',
        'SH': '{:.0f}',
        'GDP': '{:.0f}',
        'K%': '{:.2f}',
        'BB%': '{:.2f}',
        'BB/K': '{:.2f}',
        'AVG': '{:.3f}',
        'OBP': '{:.3f}',
        'SB': '{:.0f}',
        'CS': '{:.0f}',
        'SLG': '{:.3f}',
        'OPS': '{:.3f}',
        'ISO': '{:.3f}',
        'BABIP': '{:.3f}',
        'wOBA': '{:.3f}',
        'wRC': '{:.0f}',
        'wRAA': '{:.1f}',
        'wRC+': '{:.0f}',
        'GB%': '{:.2f}',
        'FB%': '{:.2f}',
        'LD%': '{:.2f}',
        'IFFB%': '{:.2f}',
        'HR/FB': '{:.2f}',
    })
    st.dataframe(df_style, use_container_width=True)

with tab[2]:
    bat_cols = c_list + ["PA", "BB%", "K%", "BB/K", "AVG", "OBP", "SLG", "OPS", "ISO", "BABIP", "wSB", "wRC", "wRAA", "wOBA", "wRC+"]
    bat_2 = df[bat_cols]
    df_style = bat_2.style.format({
        'PA': '{:.0f}',
        'K%': '{:.1%}',
        'BB%': '{:.1%}',
        'BB/K': '{:.2f}',
        'AVG': '{:.3f}',
        'OBP': '{:.3f}',
        'SLG': '{:.3f}',
        'OPS': '{:.3f}',
        'ISO': '{:.3f}',
        'BABIP': '{:.3f}',
        'wOBA': '{:.3f}',
        'wSB': '{:.1f}',
        'wRC': '{:.0f}',
        'wRAA': '{:.1f}',
        'wRC+': '{:.0f}',
        'GB%': '{:.1%}',
        'FB%': '{:.1%}',
        'LD%': '{:.1%}',
        'IFFB%': '{:.1%}',
        'HR/FB': '{:.1%}',
    })
    st.dataframe(df_style, use_container_width=True)

with tab[3]:
    bat_cols = c_list + ["BABIP", "GB/FB", "LD%", "GB%", "FB%", "IFFB%", "HR/FB", "IFH", "IFH%", "Pull%", "Cent%", "Oppo%"]
    bat_3 = df[bat_cols]
    df_style = bat_3.style.format({
        'IFH': '{:.0f}',
        'IP': '{:.1f}',
        'K%': '{:.1%}',
        'BB%': '{:.1%}',
        'K-BB%': '{:.1%}',
        'HR%': '{:.1%}',
        'K/9': '{:.2f}',
        'BB/9': '{:.2f}',
        'K/BB': '{:.2f}',
        'HR/9': '{:.2f}',
        'AVG': '{:.3f}',
        'BABIP': '{:.3f}',
        'IFH%': '{:.1%}',
        'GB%': '{:.1%}',
        'FB%': '{:.1%}',
        'LD%': '{:.1%}',
        'Pull%': '{:.1%}',
        'Cent%': '{:.1%}',
        'Oppo%': '{:.1%}',
        'IFFB%': '{:.1%}',
        'GB/FB': '{:.2f}',
        'HR/FB': '{:.1%}',
    })
    st.dataframe(df_style, use_container_width=True)

with tab[4]:
    bat_cols = c_list + ["O-Swing%", "Z-Swing%", "Swing%", "O-Contact%", "Z-Contact%", "Contact%", 
                            "Zone%", "F-Strike%", "Whiff%", "PutAway%", "SwStr%", "CStr%", "CSW%"]
    bat_4 = df[bat_cols]
    df_style = bat_4.style.format({
        'O-Swing%': '{:.1%}',
        'Z-Swing%': '{:.1%}',
        'Swing%': '{:.1%}',
        'O-Contact%': '{:.1%}',
        'Z-Contact%': '{:.1%}',
        'Contact%': '{:.1%}',
        'Zone%': '{:.1%}',
        'F-Strike%': '{:.1%}',
        'Whiff%': '{:.1%}',
        'PutAway%': '{:.1%}',
        'SwStr%': '{:.1%}',
        'CStr%': '{:.1%}',
        'CSW%': '{:.1%}',
    })
    st.dataframe(df_style, use_container_width=True)

with tab[5]:
    bat_cols = c_list + ["FA%", "FAv", "FT%", "FTv", "SL%", "SLv", "CT%", "CTv", "CB%", "CBv", 
                            "CH%", "CHv", "SF%", "SFv", "SI%", "SIv", "SP%", "SPv", "XX%", "XXv"]
    bat_5 = df[bat_cols]
    df_style = bat_5.style.format({
        'FA%': '{:.1%}',
        'FT%': '{:.1%}',
        'SL%': '{:.1%}',
        'CT%': '{:.1%}',
        'CB%': '{:.1%}',
        'CH%': '{:.1%}',
        'SF%': '{:.1%}',
        'SI%': '{:.1%}',
        'SP%': '{:.1%}',
        'XX%': '{:.1%}',
        'FAv': '{:.1f}',
        'FTv': '{:.1f}',
        'SLv': '{:.1f}',
        'CTv': '{:.1f}',
        'CBv': '{:.1f}',
        'CHv': '{:.1f}',
        'SFv': '{:.1f}',
        'SIv': '{:.1f}',
        'SPv': '{:.1f}',
        'XXv': '{:.1f}'
    })
    st.dataframe(df_style, use_container_width=True)

with tab[6]:
    bat_cols = c_list + ["wFA", "wFT", "wSL", "wCT", "wCB", "wCH", "wSF", "wSI", "wSP", 
                        "wFA/C", "wFT/C", "wSL/C", "wCT/C", "wCB/C", "wCH/C", "wSF/C", "wSI/C", "wSP/C"]
    bat_6 = df[bat_cols]
    df_style = bat_6.style.format({
        'wFA': '{:.1f}',
        'wFT': '{:.1f}',
        'wSL': '{:.1f}',
        'wCT': '{:.1f}',
        'wCB': '{:.1f}',
        'wCH': '{:.1f}',
        'wSF': '{:.1f}',
        'wSI': '{:.1f}',
        'wSP': '{:.1f}',
        'wFA/C': '{:.1f}',
        'wFT/C': '{:.1f}',
        'wSL/C': '{:.1f}',
        'wCT/C': '{:.1f}',
        'wCB/C': '{:.1f}',
        'wCH/C': '{:.1f}',
        'wSF/C': '{:.1f}',
        'wSI/C': '{:.1f}',
        'wSP/C': '{:.1f}',
    })
    st.dataframe(df_style, use_container_width=True)
    

with tab[7]:
    bat_cols = c_list + ["PA", "BB%+", "K%+", "AVG+", "OBP+", "SLG+", "OPS+", "wRC+", "ISO+", "BABIP+", 
                            "LD%+", "GB%+", "FB%+", "Pull%+", "Cent%+", "Oppo%+"]
    bat_7 = df[bat_cols]
    df_style = bat_7.style.format({
        'PA': '{:.0f}',
        'wRC+': '{:.0f}',
        'AVG+': '{:.0f}',
        'OBP+': '{:.0f}',
        'SLG+': '{:.0f}',
        'OPS+': '{:.0f}',
        'ISO+': '{:.0f}',
        'K%+': '{:.0f}',
        'BB%+': '{:.0f}',
        'GB%+': '{:.0f}',
        'FB%+': '{:.0f}',
        'LD%+': '{:.0f}',
        'Pull%+': '{:.0f}',
        'Oppo%+': '{:.0f}',
        'Cent%+': '{:.0f}',
        'BABIP+': '{:.0f}',
    })
    st.dataframe(df_style, use_container_width=True)