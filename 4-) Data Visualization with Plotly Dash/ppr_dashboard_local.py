import sqlite3
from dash import Dash, html, dcc, Input, Output, dash_table
import plotly.express as px
import pandas as pd
from threading import Timer
import webbrowser
from datetime import date,datetime,timedelta
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)



# DATABASE CONNECTION
conn = sqlite3.connect("ppr_local.db")

#Returns Dataframe from sql query
def sqlToDf(query,df_list=None):
    if df_list==None:
        df = pd.DataFrame(pd.read_sql_query(query, conn))
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'], format="%Y-%m-%d")
        df['report_date'] = df['snapshot_date'] - timedelta(days=1)
        df = df.drop(["snapshot_date"], axis=1)  # FOR LINE GRAPH
    else:
        df = pd.DataFrame(pd.read_sql_query(query, conn))
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date'], format="%Y-%m-%d")
        df['report_date'] = df['snapshot_date'] - timedelta(days=1)
        df = df.drop(["snapshot_date"], axis=1)  # FOR LINE GRAPH
        df = df[df_list]

    if "id" in df.columns:
        df = df.drop(["id"], axis=1)
    df = df.reset_index(drop=True)
    return df

#P/L LINE GRAPH DATAFRAME
query_pl_result = f"SELECT snapshot_date,totalpl_thisyear FROM tbl_PPR_Total_Results ORDER BY snapshot_date ASC"
query_pl_result_b2b_ty = f"SELECT snapshot_date,b2bpl_thisyear FROM tbl_PPR_b2b_Results ORDER BY snapshot_date ASC"
query_pl_result_mass_ty = f"SELECT snapshot_date,masspl_thisyear FROM tbl_PPR_mass_Results ORDER BY snapshot_date ASC"
query_pl_result_fx_ty = f"SELECT snapshot_date,fxpl_thisyear FROM tbl_PPR_fx_Results ORDER BY snapshot_date ASC"
df_pl_result = sqlToDf(query_pl_result)
df_pl_result_b2b_ty = sqlToDf(query_pl_result_b2b_ty)
df_pl_result_mass_ty = sqlToDf(query_pl_result_mass_ty)
df_pl_result_fx_ty = sqlToDf(query_pl_result_fx_ty)
#P/L LINE GRAPH DATAFRAME NEXT YEAR
query_pl_result_ny = f"SELECT snapshot_date,totalpl_nextyear FROM tbl_PPR_Total_Results ORDER BY snapshot_date ASC"
query_pl_result_b2b_ny = f"SELECT snapshot_date,b2bpl_nextyear FROM tbl_PPR_b2b_Results ORDER BY snapshot_date ASC"
query_pl_result_mass_ny = f"SELECT snapshot_date,masspl_nextyear FROM tbl_PPR_mass_Results ORDER BY snapshot_date ASC"
query_pl_result_fx_ny = f"SELECT snapshot_date,fxpl_nextyear FROM tbl_PPR_fx_Results ORDER BY snapshot_date ASC"
df_pl_result_ny = sqlToDf(query_pl_result_ny)
df_pl_result_b2b_ny = sqlToDf(query_pl_result_ny)
df_pl_result_mass_ny = sqlToDf(query_pl_result_mass_ny)
df_pl_result_fx_ny = sqlToDf(query_pl_result_fx_ny)
#REPORT DATES FOR DROPDOWN
report_dates = df_pl_result["report_date"].dt.strftime('%Y-%m-%d') #FOR DROPDOWN

#REPORT DATES FOR LINE GRAPH
report_dates_thur = []
for date in list(df_pl_result["report_date"]):
    if date.strftime("%a") == "Mon":
        report_dates_thur.append(date.strftime('%Y-%m-%d'))
report_dates_thur
"""#DICT FOR SLIDER
report_dates_dict = {}
for i,item in enumerate(report_dates_thur):
    report_dates_dict[i] = item"""
#YEARS
years = [date.today().year,date.today().year+1]


#POSITION BARCHART
query_total_pp =  f"SELECT * FROM tbl_PPR_Total_this_year ORDER BY snapshot_date ASC"
df_total_position_peak = sqlToDf(query_total_pp,["report_date", "openpositionmwpeak", "openpositionmwpeakoffpeak"])
#POSITION BARCHART NEXT YEAR
query_total_pp_ny = f"SELECT * FROM tbl_PPR_Total_next_year ORDER BY snapshot_date ASC"
df_total_position_peak_ny = sqlToDf(query_total_pp_ny,["report_date", "openpositionmwpeak", "openpositionmwpeakoffpeak"])
#TOTAL TABLE
query_total = f"SELECT* FROM tbl_PPR_Total_This_Year ORDER BY snapshot_date ASC"
df_total = sqlToDf(query_total,["report_date", "plmtltotal","salesmwtotal","hedgemwtotal","openpositionmwbaseload"])
#TOTAL TABLE NEXT YEAR
query_total_ny = f"SELECT* FROM tbl_PPR_Total_Next_Year ORDER BY snapshot_date ASC"
df_total_ny = sqlToDf(query_total_ny,["report_date", "plmtltotal","salesmwtotal","hedgemwtotal","openpositionmwbaseload"])
#B2B TABLE
query_b2b = f"SELECT* FROM tbl_PPR_b2b_this_year ORDER BY snapshot_date ASC"
df_b2b = sqlToDf(query_b2b)
#B2B TABLE NEXT YEAR
query_b2b_ny = f"SELECT* FROM tbl_PPR_b2b_next_year ORDER BY snapshot_date ASC"
df_b2b_ny = sqlToDf(query_b2b_ny)
#B2B BAR CHART
query_b2b_op = f"SELECT* FROM tbl_PPR_b2b_op_this_year ORDER BY snapshot_date ASC"
df_b2b_op = sqlToDf(query_b2b_op)
#B2B BAR CHART NEXT YEAR
query_b2b_op_ny = f"SELECT* FROM tbl_PPR_b2b_op_next_year ORDER BY snapshot_date ASC"
df_b2b_op_ny = sqlToDf(query_b2b_op_ny)
#FX TABLE
query_fx = f"SELECT* FROM tbl_PPR_fx_this_year ORDER BY snapshot_date ASC"
df_fx = sqlToDf(query_fx)
#FX TABLE NEXT YEAR
query_fx_ny = f"SELECT* FROM tbl_PPR_fx_next_year ORDER BY snapshot_date ASC"
df_fx_ny = sqlToDf(query_fx_ny)
#FX BAR CHART B2B
query_fx_op = f"SELECT snapshot_date,openb2b,openmass FROM tbl_PPR_fx_op_this_year ORDER BY snapshot_date ASC"
df_fx_op = sqlToDf(query_fx_op)
#FX BAR CHART B2B NEXT YEAR
query_fx_op_ny = f"SELECT snapshot_date,openb2b,openmass FROM tbl_PPR_fx_op_next_year ORDER BY snapshot_date ASC"
df_fx_op_ny = sqlToDf(query_fx_op_ny)
#MASS TABLE
query_mass = f"SELECT* FROM tbl_PPR_mass_this_year ORDER BY snapshot_date ASC"
df_mass = sqlToDf(query_mass)
#MASS TABLE NEXT YEAR
query_mass_ny = f"SELECT* FROM tbl_PPR_mass_next_year ORDER BY snapshot_date ASC"
df_mass_ny = sqlToDf(query_mass_ny)
#MASS BAR CHART
query_mass_op = f"SELECT* FROM tbl_PPR_mass_op_this_year ORDER BY snapshot_date ASC"
df_mass_op = sqlToDf(query_mass_op)
#MASS BAR CHART NEXT YEAR
query_mass_op_ny = f"SELECT* FROM tbl_PPR_mass_op_next_year ORDER BY snapshot_date ASC"
df_mass_op_ny = sqlToDf(query_mass_op)
#PARAMETER TABLE THIS YEAR
query_param_ty = f"SELECT* FROM tbl_PPR_Parameters_This_Year ORDER BY snapshot_date ASC"
df_param_ty = sqlToDf(query_param_ty)
#PARAMETER TABLE THIS YEAR
query_param_ny = f"SELECT* FROM tbl_PPR_Parameters_Next_Year ORDER BY snapshot_date ASC"
df_param_ny = sqlToDf(query_param_ny)

app = Dash(__name__)
app.title = "PPR"

colors = {
    'background': 'white',
    'bar1': 'rgb(244, 197, 34)',
    'bar2': 'grey',
    'table_header': 'rgb(255, 251, 0)',
    'table_bottom': 'rgb(244, 197, 34)',
}

app.layout = html.Div([
    html.Div(children=[
        html.H1(children='PORTFOLIO POSITION REPORT'),

        html.Div(children='''
    Energy Management Directorate Portfolio Position Report.
    ''')
    ]),

    html.Br(),

    html.Div([

        html.Div([

            html.Label(children='''Report Date 1'''),

            dcc.Dropdown(
                id='report_date_selection',
                options=[{'label': i, 'value': i} for i in report_dates],
                value=report_dates_thur[len(report_dates_thur)-1],
                searchable=True,
                clearable = False
            )
        ],
        style={'width': '20%', 'display': 'inline-block'}),

        html.Label(children='''Report Date 2'''),

        html.Div([
            dcc.Dropdown(
                id='report_date2_selection',
                options=[{'label': i, 'value': i} for i in report_dates],
                value=report_dates_thur[len(report_dates_thur) - 2],
                searchable=True,
                clearable=False
            )
        ],
            style={'width': '20%', 'display': 'inline-block'}),

        ]),
        html.Div([
            dcc.RadioItems(
                years,
                years[0],
                id='year_selection',
                labelStyle={'display': 'inline-block', 'marginTop': '5px'}
            )
        ],
            style={'width': '49%', 'display': 'inline-block','float':'left'}),

        html.Div([

            dcc.RadioItems(
            ["TOTAL","B2B","MASS","FX"],
            "TOTAL",
            id='final_table_selection',
            labelStyle={'display': 'inline-block', 'marginTop': '5px'}
        )
    ],
        style={'width': '49%', 'display': 'inline-block', 'float': 'right'}),

    html.Div([
        dcc.Graph(
            id='pl_line_graph',clear_on_unhover = True
        )
    ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
        dcc.Graph(
            id='position_bar_chart'
        ),
    ], style={'display': 'inline-block','float':'right', 'width': '49%'}),

    html.Div([
        dash_table.DataTable(
            id='total_table',
            merge_duplicate_headers=True
        )
        ]),

    html.Br(),

    html.Div([
        dash_table.DataTable(
            id='param_table_bid',
            merge_duplicate_headers=True
        )
    ], style={'display': 'inline-block','float':'left', 'width': '49%'}),

    html.Div([
        dash_table.DataTable(
            id='param_table_ep',
            merge_duplicate_headers=True
        )
    ], style={'display': 'inline-block', 'float': 'right', 'width': '49%'}),

    html.Br(),
    html.Br(),

    html.Div([
        dash_table.DataTable(
            id='param_table_offer',
            merge_duplicate_headers=True
        )
    ], style={'display': 'inline-block', 'float': 'left', 'width': '49%'}),

    html.Div([
        dash_table.DataTable(
            id='param_table_yf',
            merge_duplicate_headers=True
        )
    ], style={'display': 'inline-block', 'float': 'right', 'width': '49%'}),

])

@app.callback(
    Output('pl_line_graph','figure'),
    Input('year_selection','value'),
    Input('final_table_selection','value'))
def update_pl_line_graph(year,final_table):
    """
                 hover_data={'species':False, # remove species from hover data
                             'sepal_length':':.2f', # customize hover for column of y attribute
                             'petal_width':True, # add other column, default formatting
                             'sepal_width':':.2f', # add other column, customized formatting
                             # data not in dataframe, default formatting
                             'suppl_1': np.random.random(len(df)),
                             # data not in dataframe, customized formatting
                             'suppl_2': (':.3f', np.random.random(len(df)))
                            })
    """
    # TOTAL P/L LINE GRAPH
    if final_table == "TOTAL":
        if year == date.today().year:
            df_pl_result_filtered = df_pl_result[df_pl_result["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="totalpl_thisyear", markers=True,text="totalpl_thisyear", title=f'P/L {year}')
        elif year == date.today().year+1:
            df_pl_result_filtered = df_pl_result_ny[df_pl_result_ny["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="totalpl_nextyear", markers=True,text="totalpl_nextyear", title=f'P/L {year}')
        fig_pl_line.update_xaxes(title_text="Report Date",showspikes=True)
        fig_pl_line.update_yaxes(title_text=f"Total P/L {year}",showspikes=True)
        fig_pl_line.update_traces(textposition='top center',hovertemplate='Report Date: %{x} <br>Total P/L: %{y}')
        fig_pl_line.update_layout(plot_bgcolor="white",hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family="Tahoma"
        )
                                  )
    elif final_table == "B2B":
        if year == date.today().year:
            df_pl_result_filtered = df_pl_result_b2b_ty[df_pl_result_b2b_ty["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="b2bpl_thisyear", markers=True,text="b2bpl_thisyear", title=f'P/L {year}')
        elif year == date.today().year+1:
            df_pl_result_filtered = df_pl_result_b2b_ny[df_pl_result_b2b_ny["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="b2bpl_nextyear", markers=True,text="b2bpl_nextyear", title=f'P/L {year}')
        fig_pl_line.update_xaxes(title_text="Report Date",showspikes=True)
        fig_pl_line.update_yaxes(title_text=f"B2B P/L {year}",showspikes=True)
        fig_pl_line.update_traces(textposition='top center',hovertemplate='Report Date: %{x} <br>B2B P/L: %{y}')
        fig_pl_line.update_layout(plot_bgcolor="white",hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family="Tahoma"
        )
                                  )

    elif final_table == "MASS":
        if year == date.today().year:
            df_pl_result_filtered = df_pl_result_mass_ty[df_pl_result_mass_ty["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="masspl_thisyear", markers=True,text="masspl_thisyear", title=f'P/L {year}')
        elif year == date.today().year+1:
            df_pl_result_filtered = df_pl_result_mass_ny[df_pl_result_mass_ny["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="masspl_nextyear", markers=True,text="masspl_nextyear", title=f'P/L {year}')
        fig_pl_line.update_xaxes(title_text="Report Date",showspikes=True)
        fig_pl_line.update_yaxes(title_text=f"MASS P/L {year}",showspikes=True)
        fig_pl_line.update_traces(textposition='top center',hovertemplate='Report Date: %{x} <br>MASS P/L: %{y}')
        fig_pl_line.update_layout(plot_bgcolor="white",hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family="Tahoma"
        )
                                  )

    elif final_table == "FX":
        if year == date.today().year:
            df_pl_result_filtered = df_pl_result_fx_ty[df_pl_result_fx_ty["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="fxpl_thisyear", markers=True,text="fxpl_thisyear", title=f'P/L {year}')
        elif year == date.today().year+1:
            df_pl_result_filtered = df_pl_result_fx_ny[df_pl_result_fx_ny["report_date"].dt.strftime('%Y-%m-%d').isin(report_dates_thur)]
            fig_pl_line = px.line(round(df_pl_result_filtered,0), x="report_date", y="fxpl_nextyear", markers=True,text="fxpl_nextyear", title=f'P/L {year}')
        fig_pl_line.update_xaxes(title_text="Report Date",showspikes=True)
        fig_pl_line.update_yaxes(title_text=f"FX P/L {year}",showspikes=True)
        fig_pl_line.update_traces(textposition='top center',hovertemplate='Report Date: %{x} <br>FX P/L: %{y}')
        fig_pl_line.update_layout(plot_bgcolor="white",hoverlabel=dict(
            bgcolor="white",
            font_size=16,
            font_family="Tahoma"
        )
                                  )
    return fig_pl_line
        
#INTRACTIVE POSITION BAR CHART FUNCTION INPUT=DROPDOWN
@app.callback(
    Output('position_bar_chart', 'figure'),
    Input('report_date_selection', 'value'),
    Input('year_selection','value'),
    Input('final_table_selection','value'),
    Input('pl_line_graph','hoverData'))
def update_graph(selected_date,year,table_type,hover_data):
    print(hover_data)
    if hover_data is not None:
        if table_type == "TOTAL":
            print(hover_data['points'][0]['x'])
            if year == date.today().year:
                df_total_pp = df_total_position_peak[df_total_position_peak.report_date == hover_data['points'][0]['x']].drop(
                    ["report_date"], axis=1)[:-1].reset_index(drop=True)
            elif year == date.today().year + 1:
                df_total_pp = df_total_position_peak_ny[df_total_position_peak_ny.report_date == hover_data['points'][0]['x']].drop(
                    ["report_date"], axis=1)[:-1].reset_index(drop=True)
            df_total_pp.columns = ["PEAK", "0FFPEAK"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df_total_pp.insert(loc=0, column="Months", value=pp_months)
            df_total_pp.index = df_total_pp["Months"]
            df_total_pp = df_total_pp.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df_total_pp, 1), barmode='group', text_auto=True,
                                           title=f"NET POSITION {year} REPORT DATE: {hover_data['points'][0]['x']}",
                                           color_discrete_map={"PEAK": f"{colors['bar1']}", '0FFPEAK': f"{colors['bar2']}"})
            # color_discrete_map={'PEAK':'yellow', '0FFPEAK':'grey'}
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")

        elif table_type == "B2B":
            if year == date.today().year:
                df = df_b2b_op[df_b2b_op.report_date == hover_data['points'][0]['x']].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            elif year == date.today().year + 1:
                df = df_b2b_op_ny[df_b2b_op_ny.report_date == hover_data['points'][0]['x']].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            df.columns = ["PEAK", "0FFPEAK"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df.insert(loc=0, column="Months", value=pp_months)
            df.index = df["Months"]
            df = df.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df, 1), barmode='group', text_auto=True,
                                           title=f"B2B OPEN POSITION {year} REPORT DATE: {hover_data['points'][0]['x']}",
                                           color_discrete_map={"PEAK": f"{colors['bar1']}",
                                                               '0FFPEAK': f"{colors['bar2']}"})
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")

        elif table_type == "MASS":
            if year == date.today().year:
                df = df_mass_op[df_mass_op.report_date == hover_data['points'][0]['x']].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            elif year == date.today().year + 1:
                df = df_mass_op_ny[df_mass_op_ny.report_date == hover_data['points'][0]['x']].drop(["report_date"],
                                                                                    axis=1).reset_index(drop=True)
            df.columns = ["PEAK", "0FFPEAK"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df.insert(loc=0, column="Months", value=pp_months)
            df.index = df["Months"]
            df = df.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df, 1), barmode='group', text_auto=True,
                                           title=f"MASS OPEN POSITION {year} REPORT DATE: {hover_data['points'][0]['x']}",
                                           color_discrete_map={"PEAK": f"{colors['bar1']}",
                                                               '0FFPEAK': f"{colors['bar2']}"})
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")

        elif table_type == "FX":
            if year == date.today().year:
                df = df_fx_op[df_fx_op.report_date == hover_data['points'][0]['x']].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            elif year == date.today().year + 1:
                df = df_fx_op_ny[df_fx_op_ny.report_date == hover_data['points'][0]['x']].drop(["report_date"],
                                                                                        axis=1).reset_index(drop=True)
            df.columns = ["B2B","MASS"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df.insert(loc=0, column="Months", value=pp_months)
            df.index = df["Months"]
            df = df.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df, 1), barmode='group', text_auto=True,
                                           title=f"FX OPEN POSITION {year} REPORT DATE: {hover_data['points'][0]['x']}",
                                           color_discrete_map={'B2B': f"{colors['bar2']}",'MASS': f"{colors['bar1']}"})
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")
        return fig_position_barchart
    else:
        if table_type == "TOTAL":
            if year == date.today().year:
                df_total_pp = df_total_position_peak[df_total_position_peak.report_date == selected_date].drop(["report_date"],axis=1)[:-1].reset_index(drop=True)
            elif year == date.today().year+1:
                df_total_pp = df_total_position_peak_ny[df_total_position_peak_ny.report_date == selected_date].drop(["report_date"],axis=1)[:-1].reset_index(drop=True)
            df_total_pp.columns = ["PEAK", "0FFPEAK"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df_total_pp.insert(loc=0, column="Months", value=pp_months)
            df_total_pp.index = df_total_pp["Months"]
            df_total_pp = df_total_pp.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df_total_pp, 1), barmode='group', text_auto=True,
                                           title=f'NET POSITION {year} REPORT DATE: {selected_date}',
                                           color_discrete_map={"PEAK": f"{colors['bar1']}", '0FFPEAK': f"{colors['bar2']}"})
            #color_discrete_map={'PEAK':'yellow', '0FFPEAK':'grey'}
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")

        elif table_type == "B2B":
            if year == date.today().year:
                df = df_b2b_op[df_b2b_op.report_date == selected_date].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            elif year == date.today().year + 1:
                df = df_b2b_op_ny[df_b2b_op_ny.report_date == selected_date].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            df.columns = ["PEAK", "0FFPEAK"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df.insert(loc=0, column="Months", value=pp_months)
            df.index = df["Months"]
            df = df.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df, 1), barmode='group', text_auto=True,
                                           title=f'B2B OPEN POSITION {year} REPORT DATE: {selected_date}',
                                           color_discrete_map={"PEAK": f"{colors['bar1']}",
                                                               '0FFPEAK': f"{colors['bar2']}"})
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")
        elif table_type == "MASS":
            if year == date.today().year:
                df = df_mass_op[df_mass_op.report_date == selected_date].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            elif year == date.today().year + 1:
                df = df_mass_op_ny[df_mass_op_ny.report_date == selected_date].drop(["report_date"],
                                                                                    axis=1).reset_index(drop=True)
            df.columns = ["PEAK", "0FFPEAK"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df.insert(loc=0, column="Months", value=pp_months)
            df.index = df["Months"]
            df = df.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df, 1), barmode='group', text_auto=True,
                                           title=f'MASS OPEN POSITION {year} REPORT DATE: {selected_date}',
                                           color_discrete_map={"PEAK": f"{colors['bar1']}",
                                                               '0FFPEAK': f"{colors['bar2']}"})
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")

        elif table_type == "FX":
            if year == date.today().year:
                df = df_fx_op[df_fx_op.report_date == selected_date].drop(["report_date"], axis=1).reset_index(
                    drop=True)
            elif year == date.today().year + 1:
                df = df_fx_op_ny[df_fx_op_ny.report_date == selected_date].drop(["report_date"],
                                                                                        axis=1).reset_index(drop=True)
            df.columns = ["B2B","MASS"]
            pp_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            df.insert(loc=0, column="Months", value=pp_months)
            df.index = df["Months"]
            df = df.drop(["Months"], axis=1)

            fig_position_barchart = px.bar(round(df, 1), barmode='group', text_auto=True,
                                           title=f'FX OPEN POSITION {year} REPORT DATE: {selected_date}',
                                           color_discrete_map={'B2B': f"{colors['bar2']}",'MASS': f"{colors['bar1']}"})
            fig_position_barchart.update_yaxes(title_text=f"Open Position MW")
            fig_position_barchart.update_traces(textposition='outside')
            fig_position_barchart.update_layout(plot_bgcolor="white")

        return fig_position_barchart
#INTRACTIVE TOTAL DATA TABLE FUNCTION INPUT=DROPDOWN
@app.callback(
    Output('total_table', 'data'),
    Output('total_table', 'columns'),
    Output('total_table', 'style_data_conditional'),
    Output('total_table', 'style_cell'),
    Output('total_table', 'style_header'),
    Output('total_table', 'style_data'),
    Input('report_date_selection', 'value'),
    Input('report_date2_selection', 'value'),
    Input('year_selection', 'value'),
    Input('final_table_selection', 'value'))
def upgrade_total_table(selected_date,selected_date2,year,table_type):
    print(table_type)
    if table_type =="TOTAL":
        if year == date.today().year:
            df = df_total[df_total.report_date == selected_date].drop(["report_date"],axis=1)
        elif year == date.today().year+1:
            df = df_total_ny[df_total_ny.report_date == selected_date].drop(["report_date"], axis=1)
        total_columns = ['PL-MTL TOTAL', 'SALES-MW TOTAL', 'HEDGE-MW TOTAL', 'OPEN POSITION-MW BASELOAD']
        df.columns = total_columns
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "TOTAL"]
        df.insert(loc=0, column="MONTH", value=months)
        datas = round(df, 1).to_dict('rows')
        columns = [{"name": [f"TOTAL {year}", i], "id": i} for i in (df.columns)]

        style_cell = {'textAlign': 'center'}

        style_header = {
            'backgroundColor': f"{colors['table_header']}",
            'color': 'black',
            'fontWeight': 'bold'
        }

        style = [
            {
                'if': {
                    'row_index': len(df) - 1,
                },
                'backgroundColor': f"{colors['table_bottom']}",
                'color': 'black',
                'fontWeight': 'bold'
            }
        ]
        style_data = {'border': '1px solid grey'}

    elif table_type == "B2B":
        if year == date.today().year:
            df = df_b2b[df_b2b.report_date == selected_date].drop(["report_date"], axis=1)
            df2 = df_b2b[df_b2b.report_date == selected_date2].drop(["report_date"], axis=1)
        elif year == date.today().year + 1:
            df = df_b2b_ny[df_b2b_ny.report_date == selected_date].drop(["report_date"], axis=1)
            df2 = df_b2b_ny[df_b2b_ny.report_date == selected_date2].drop(["report_date"], axis=1)
        energy_diff = df["energy_pl_mtl"].values - df2["energy_pl_mtl"].values
        sales_diff = df["sales_mw_total"].values - df2["sales_mw_total"].values
        hedge_diff = df["hedge_mw_total"].values - df2["hedge_mw_total"].values
        df.insert(loc=1, column="ENERGY_\u0394", value=energy_diff)
        df.insert(loc=7, column="SALES_\u0394", value=sales_diff)
        df.insert(loc=9, column="HEDGE_\u0394", value=hedge_diff)

        df.columns = [column.upper() for column in df.columns]
        df.columns = [column.replace("_", " ") for column in df.columns]
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "TOTAL"]
        df.insert(loc=0, column="MONTH", value=months)
        datas = round(df, 1).to_dict('rows')
        columns = [{"name": [f"B2B {year}", i], "id": i} for i in (df.columns)]

        style_cell = {'textAlign': 'center'}

        style_header = {
            'backgroundColor': f"{colors['table_header']}",
            'color': 'black',
            'fontWeight': 'bold'
        }

        style = [
            {
                'if': {
                    'row_index': len(df) - 1,
                },
                'backgroundColor': f"{colors['table_bottom']}",
                'color': 'black',
                'fontWeight': 'bold'
            }
        ]
        style_data = {'border': '1px solid grey'}

    elif table_type == "MASS":
        if year == date.today().year:
            df = df_mass[df_mass.report_date == selected_date].drop(["report_date"], axis=1)
            df2 = df_mass[df_mass.report_date == selected_date2].drop(["report_date"], axis=1)
        elif year == date.today().year + 1:
            df = df_mass_ny[df_mass_ny.report_date == selected_date].drop(["report_date"], axis=1)
            df2 = df_mass_ny[df_mass_ny.report_date == selected_date2].drop(["report_date"], axis=1)

        energy_diff = df["energy_pl_mtl_energy"].values - df2["energy_pl_mtl_energy"].values
        sales_diff = df["sales_mw_total"].values - df2["sales_mw_total"].values
        hedge_diff = df["hedge_mw_total"].values - df2["hedge_mw_total"].values
        df.insert(loc=1, column="ENERGY_\u0394", value=energy_diff)
        df.insert(loc=7, column="SALES_\u0394", value=sales_diff)
        df.insert(loc=9, column="HEDGE_\u0394", value=hedge_diff)

        df.columns = [column.upper() for column in df.columns]
        df.columns = [column.replace("_", " ") for column in df.columns]
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "TOTAL"]
        df.insert(loc=0, column="MONTH", value=months)
        datas = round(df, 1).to_dict('rows')
        columns = [{"name": [f"MASS {year}", i], "id": i} for i in (df.columns)]

        style_cell = {'textAlign': 'center'}

        style_header = {
            'backgroundColor': f"{colors['table_header']}",
            'color': 'black',
            'fontWeight': 'bold'
        }

        style = [
            {
                'if': {
                    'row_index': len(df) - 1,
                },
                'backgroundColor': f"{colors['table_bottom']}",
                'color': 'black',
                'fontWeight': 'bold'
            }
        ]
        style_data = {'border': '1px solid grey'}

    elif table_type == "FX":
        if year == date.today().year:
            df = df_fx[df_fx.report_date == selected_date].drop(["report_date"], axis=1)
        elif year == date.today().year + 1:
            df = df_fx_ny[df_fx_ny.report_date == selected_date].drop(["report_date"], axis=1)
        df.columns = [column.upper() for column in df.columns]
        df.columns = [column.replace("_", " ") for column in df.columns]
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "TOTAL"]
        df.insert(loc=0, column="MONTH", value=months)
        datas = round(df, 1).to_dict('rows')
        columns = [{"name": [f"FX {year}", i], "id": i} for i in (df.columns)]

        style_cell = {'textAlign': 'center'}

        style_header = {
            'backgroundColor': f"{colors['table_header']}",
            'color': 'black',
            'fontWeight': 'bold'
        }

        style = [
            {
                'if': {
                    'row_index': len(df) - 1,
                },
                'backgroundColor': f"{colors['table_bottom']}",
                'color': 'black',
                'fontWeight': 'bold'
            }
        ]
        style_data = {'border': '1px solid grey'}

    return datas, columns, style, style_cell, style_header, style_data

#INTRACTIVE PARAMETER BID DATA TABLE FUNCTION INPUT=DROPDOWN
@app.callback(
    Output('param_table_bid', 'data'),
    Output('param_table_bid', 'columns'),
    Output('param_table_bid', 'style_data_conditional'),
    Output('param_table_bid', 'style_cell'),
    Output('param_table_bid', 'style_header'),
    Output('param_table_bid', 'style_data'),
    Input('report_date_selection', 'value'),
    Input('report_date2_selection', 'value'),
    Input('year_selection', 'value'))
def upgrade_param_bid_table(selected_date,selected_date2,year):
    if year == date.today().year:
        df = df_param_ty[df_param_ty.report_date == selected_date].drop(["report_date"],axis=1)
        df2 = df_param_ty[df_param_ty.report_date == selected_date2].drop(["report_date"],axis=1)
    elif year == date.today().year+1:
        df = df_param_ny[df_param_ny.report_date == selected_date].drop(["report_date"], axis=1)
        df2 = df_param_ny[df_param_ny.report_date == selected_date2].drop(["report_date"], axis=1)
    df = df[["bid_baseload","bid_peak","bid_offpeak"]]

    bid_diff = df["bid_baseload"].values - df2["bid_baseload"].values
    df.insert(loc=3, column="\u0394 BID", value=bid_diff)

    param_columns = ["BASELOAD","PEAK","OFFPEAK","\u0394 BID"]
    df.columns = param_columns
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec","TOTAL"]
    df.insert(loc=0, column="MONTH", value=months)
    datas = round(df, 1).to_dict('rows')
    columns = [{"name": [f"ELECTRICITY FWC BID {year}", i], "id": i} for i in (df.columns)]

    style_cell = {'textAlign': 'center'}

    style_header = {
        'backgroundColor': f"{colors['table_header']}",
        'color': 'black',
        'fontWeight': 'bold'
    }

    style = [
        {
            'if': {
                'row_index': len(df) - 1,
            },
            'backgroundColor': f"{colors['table_bottom']}",
            'color': 'black',
            'fontWeight': 'bold'
        }
    ]
    style_data = {'border': '1px solid grey'}

    return datas, columns, style, style_cell, style_header, style_data

#INTRACTIVE PARAMETER EP DATA TABLE FUNCTION INPUT=DROPDOWN
@app.callback(
    Output('param_table_ep', 'data'),
    Output('param_table_ep', 'columns'),
    Output('param_table_ep', 'style_data_conditional'),
    Output('param_table_ep', 'style_cell'),
    Output('param_table_ep', 'style_header'),
    Output('param_table_ep', 'style_data'),
    Input('report_date_selection', 'value'),
    Input('report_date2_selection', 'value'),
    Input('year_selection', 'value'))
def upgrade_param_ep_table(selected_date,selected_date2,year):
    if year == date.today().year:
        df = df_param_ty[df_param_ty.report_date == selected_date].drop(["report_date"],axis=1)
        df2 = df_param_ty[df_param_ty.report_date == selected_date2].drop(["report_date"],axis=1)
    elif year == date.today().year+1:
        df = df_param_ny[df_param_ny.report_date == selected_date].drop(["report_date"], axis=1)
        df2 = df_param_ny[df_param_ny.report_date == selected_date2].drop(["report_date"], axis=1)
    df = df[["spread","peak_over_base"]]
    pb_diff = df["peak_over_base"].values - df2["peak_over_base"].values
    df.insert(loc=2, column="PEAK/BASE_\u0394", value=pb_diff)

    param_columns = ["SPREAD","PEAK/BASE","PEAK/BASE_\u0394"]
    df.columns = param_columns
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec","TOTAL"]
    df.insert(loc=0, column="MONTH", value=months)
    datas = round(df, 1).to_dict('rows')
    columns = [{"name": [f"ELECTRICITY PARAMETERS {year}", i], "id": i} for i in (df.columns)]

    style_cell = {'textAlign': 'center'}

    style_header = {
        'backgroundColor': f"{colors['table_header']}",
        'color': 'black',
        'fontWeight': 'bold'
    }

    style = [
        {
            'if': {
                'row_index': len(df) - 1,
            },
            'backgroundColor': f"{colors['table_bottom']}",
            'color': 'black',
            'fontWeight': 'bold'
        }
    ]
    style_data = {'border': '1px solid grey'}

    return datas, columns, style, style_cell, style_header, style_data

#INTRACTIVE PARAMETER OFFER DATA TABLE FUNCTION INPUT=DROPDOWN
@app.callback(
    Output('param_table_offer', 'data'),
    Output('param_table_offer', 'columns'),
    Output('param_table_offer', 'style_data_conditional'),
    Output('param_table_offer', 'style_cell'),
    Output('param_table_offer', 'style_header'),
    Output('param_table_offer', 'style_data'),
    Input('report_date_selection', 'value'),
    Input('report_date2_selection', 'value'),
    Input('year_selection', 'value'))
def upgrade_param_offer_table(selected_date,selected_date2,year):
    if year == date.today().year:
        df = df_param_ty[df_param_ty.report_date == selected_date].drop(["report_date"],axis=1)
        df2 = df_param_ty[df_param_ty.report_date == selected_date2].drop(["report_date"],axis=1)
    elif year == date.today().year+1:
        df = df_param_ny[df_param_ny.report_date == selected_date].drop(["report_date"], axis=1)
        df2 = df_param_ny[df_param_ny.report_date == selected_date2].drop(["report_date"], axis=1)
    df = df[["offer_baseload","offer_peak","offer_offpeak"]]

    offer_diff = df["offer_baseload"].values - df2["offer_baseload"].values
    df.insert(loc=3, column="\u0394 OFFER", value=offer_diff)

    param_columns = ["BASELOAD","PEAK","OFFPEAK","\u0394 OFFER"]
    df.columns = param_columns
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec","TOTAL"]
    df.insert(loc=0, column="MONTH", value=months)
    datas = round(df, 1).to_dict('rows')
    columns = [{"name": [f"ELECTRICITY FWC OFFER {year}", i], "id": i} for i in (df.columns)]

    style_cell = {'textAlign': 'center'}

    style_header = {
        'backgroundColor': f"{colors['table_header']}",
        'color': 'black',
        'fontWeight': 'bold'
    }

    style = [
        {
            'if': {
                'row_index': len(df) - 1,
            },
            'backgroundColor': f"{colors['table_bottom']}",
            'color': 'black',
            'fontWeight': 'bold'
        }
    ]
    style_data = {'border': '1px solid grey'}

    return datas, columns, style, style_cell, style_header, style_data

#INTRACTIVE PARAMETER OFFER DATA TABLE FUNCTION INPUT=DROPDOWN
@app.callback(
    Output('param_table_yf', 'data'),
    Output('param_table_yf', 'columns'),
    Output('param_table_yf', 'style_data_conditional'),
    Output('param_table_yf', 'style_cell'),
    Output('param_table_yf', 'style_header'),
    Output('param_table_yf', 'style_data'),
    Input('report_date_selection', 'value'),
    Input('report_date2_selection', 'value'),
    Input('year_selection', 'value'))
def upgrade_param_yf_table(selected_date,selected_date2,year):
    if year == date.today().year:
        df = df_param_ty[df_param_ty.report_date == selected_date].drop(["report_date"],axis=1)
        df2 = df_param_ty[df_param_ty.report_date == selected_date2].drop(["report_date"],axis=1)
    elif year == date.today().year+1:
        df = df_param_ny[df_param_ny.report_date == selected_date].drop(["report_date"], axis=1)
        df2 = df_param_ny[df_param_ny.report_date == selected_date2].drop(["report_date"], axis=1)
    df = df[["unit_cost","fx_fwc"]]

    uc_diff = df["unit_cost"].values - df2["unit_cost"].values
    df.insert(loc=1, column="\u0394 UNIT COST", value=uc_diff)

    fx_diff = df["fx_fwc"].values - df2["fx_fwc"].values
    df.insert(loc=1, column="\u0394 FX", value=fx_diff)

    param_columns = ["UNIT COST","FX","\u0394 UNIT COST","\u0394 FX"]
    df.columns = param_columns
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec","TOTAL"]
    df.insert(loc=0, column="MONTH", value=months)
    datas = round(df, 1).to_dict('rows')
    columns = [{"name": [f"YEKDEM FWC {year}", i], "id": i} for i in (df.columns)]

    style_cell = {'textAlign': 'center'}

    style_header = {
        'backgroundColor': f"{colors['table_header']}",
        'color': 'black',
        'fontWeight': 'bold'
    }

    style = [
        {
            'if': {
                'row_index': len(df) - 1,
            },
            'backgroundColor': f"{colors['table_bottom']}",
            'color': 'black',
            'fontWeight': 'bold'
        }
    ]
    style_data = {'border': '1px solid grey'}

    return datas, columns, style, style_cell, style_header, style_data
#OPENS BROWSER AT port. Automatic start
port = 8080
def open_browser():
	webbrowser.open_new("http://localhost:{}".format(port))
if __name__ == '__main__':
    app.run_server(debug=True,port=port)
    #Timer(1, open_browser).start();