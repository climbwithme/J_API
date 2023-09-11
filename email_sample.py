import sqlite3
import pandas as pd
import numpy as np
import mimetypes
import pdfkit
import os
import math

from email.message import EmailMessage
from email.utils import make_msgid
from email.mime.text import MIMEText
from email import generator
from config import Config
from datetime import date, datetime, time, timedelta
import plotly.graph_objs as go

dsr_date = date.today() - timedelta(days=Config.getDSRAdjustDate())
dsr_date_time = datetime.now() - timedelta(days=Config.getDSRAdjustDate())

ENG_MAP = {//Dictionary}
PDF_FILE_NAME = {//Dictionary}

options = {
    'margin-top': '0.75in',
    'margin-right': '0.75in',
    'margin-bottom': '0.75in',
    'margin-left': '0.75in',
    'encoding': "UTF-8",
    'custom-header': [
        ('Accept-Encoding', 'gzip')
    ],
    'enable-local-file-access': '',
    'no-outline': None
}


con = sqlite3.connect(Config.SQL_DB)
df = pd.read_sql_query(
    f"select * from dsr_summary where Date=\"{dsr_date}\"", con, parse_dates=['Start_Date, End_Date'])
trend_df = pd.read_sql_query(f"select * from dsr_exec_snapshot", con, parse_dates=['Date'])
defect_df = pd.read_sql_query(f"select * from defect_table", con)

execution_df = pd.read_sql_query(
    "select ProjectName as EngName, NodeName as Test_cycle, TestID, Path1 as Application, Path3 as Project_Name, Path4 as Test_Phase, Status, reason, TestPath4 as Lifecycle, Timestamp from status_table",
    con=con, parse_dates= ['Timestamp']
)
exec_df = pd.read_sql_query(
    "select ProjectName as EngName, NodeName as Test_cycle, TestID, EWorkstream as Application, ETestingArea as Project_Name, EOthers as Test_Phase, Lifecycle from exec_table",
    con=con
)

def workdays(d, end, excluded=(6, 7)):
    days = []
    while d.date() <= end.date():
        if d.isoweekday() not in excluded:
            days.append(d)
        d += timedelta(days=1)
    return days


def write_email(html, image_list, filename, msg, subject):
    with open(f'{filename}.eml', 'w') as out:
        part = MIMEText(html, 'html')
        msg.attach(part)
        msg['Subject'] = subject
        gen = generator.Generator(out)
        gen.flatten(msg)

# eng_list = ['246083_GCS']
eng_list = df.EngName.unique()
for eng_name in eng_list:
    image_list = []

    # msg = MIMEMultipart('alternative')
    msg = EmailMessage()
    html = """\
        <html>
                <head>
                    <style>
                        th
                        {
                            background-color: #5a287d;
                            color: beige;
                        }
                        .text-primary
                        {
                            color: #5a287d;
                        }
                    </style>
                    <meta name="pdfkit-page-size" content="A3"/>
                    <meta name="pdfkit-orientation" content="Landscape"/>
                </head>
            <body>
                <p> Good Morning ! </p>
                <p> You are identified as being a key stakeholder in the project engagement_name and as such the daily execution statistics (along with defects) are provided below. <br>
                    There is/are currently test_cycle_count test cycle(s) in active execution. A high level synopsis is provided here and there is more detail lower down the email. </p>
                
                <table>
                <tr>
                    <td><b>Overall RAG Status:</b><td>
                    <td RAG_STYLE>RAG_STATUS</td>
                    RAG_COMMENTS
                <tr>
                </table>

                <div class="container"> testing_comments </div>

                <div class="container"> test_overview_table </div>
                <br>
                <p> The cumulative defect figures for each entire test phase are listed below the details of all currently active test cycles within that "Test Phase". </p>

                <div> cycle_details </div>
            </body>
            <p><i>If you have any thoughts or improvements, please email <a href='mailto:emailhere@co.in'>here.</a></i></p>
        </html>
        """
    pdf_html_content = html
    print(eng_name)
    comments_df = pd.read_excel(
        f"excel_path", sheet_name=eng_name, parse_dates=['Date'])
    # print(f"Prepping for Engagement - {eng_name}")
    eng_df = df[df['EngName'] == eng_name]
    # print(eng_df.columns)
    # print(eng_df)
    summary_df = eng_df.copy()
    summary_df.drop_duplicates(inplace=True)
    summary_df = summary_df[['Application', 'Project_Name',  'Test_Phase', 'Test_cycle', 'Start_Date', 'End_Date',
                             'TotalTests', 'PassedTests', 'FailedTests', 'BlockedTests', 'NATests', 'DeferredTests', 'DescopedTests', 'NETests']]
    summary_df.columns = ['Application', 'Project Name',  'Test Phase', 'Test Cycle', 'Start Date',
                          'End Date', 'Total Tests', 'Passed', 'Failed', 'Blocked', 'N/A', 'Deferred', 'Descoped', 'Not Executed']
    summary_df = summary_df[summary_df['Total Tests'] > 0]
    summary_df['Start Date'] = summary_df['Start Date'].str.split(' ').str[0]
    summary_df['End Date'] = summary_df['End Date'].str.split(' ').str[0]
    summary_df.sort_values(
        ['Project Name', 'Test Phase', 'Test Cycle', 'End Date'], inplace=True)
    if summary_df.shape[0] == 0:
        break
    full_first_table = summary_df.to_html(index=False)
    print(summary_df)
    comments_df = comments_df.iloc[:,:6]
    comments_df.columns = ['DATE', 'Application', 'Project', 'Test Phase', 'Test Cycle Name', 'TM Comments']
    rag_status = comments_df['Test Phase'].iloc[0]
    rag_comments = comments_df['Test Cycle Name'].iloc[0]
    print(rag_status, rag_comments)
    testing_comments = comments_df['TM Comments'].iloc[0]
    overall_comm = ""
    prev_comm = ""
    # print(testing_comments, math.isnan(testing_comments))
    # try:
    if type(testing_comments) == str and len(testing_comments) > 0:
        overall_comm = overall_comm + "<h4 class='text-primary'> <u> Overall Testing Comments: </u> </h4>"
        for comm in testing_comments.split('\n'):
            if prev_comm == "":
                overall_comm = overall_comm + '<b>' + comm + '</b><br>'
            else:
                overall_comm = overall_comm + ' &nbsp&nbsp&nbsp&nbsp ' + comm + '<br>'
            prev_comm = comm
    # except:
        # pass
    overall_comm = overall_comm + '<br>'

    html = html.replace("test_overview_table", full_first_table)
    html = html.replace("RAG_STATUS", rag_status)
    pdf_html_content = pdf_html_content.replace("RAG_STATUS", rag_status)
    try:
        if len(rag_comments) > 0:
            html = html.replace("RAG_COMMENTS", f"<td colspan=2>{rag_comments}</td>")
            pdf_html_content = pdf_html_content.replace("RAG_COMMENTS", f"<td colspan=2>{rag_comments}</td>")
    except:
        html = html.replace("RAG_COMMENTS", "")
        pdf_html_content = pdf_html_content.replace("RAG_COMMENTS", "")
    html = html.replace("testing_comments", overall_comm)

    if rag_status == 'GREEN':
        html = html.replace('RAG_STYLE', 'style="background-color: lightgreen;"')
        pdf_html_content = pdf_html_content.replace('RAG_STYLE', 'style="background-color: lightgreen;"')
    elif rag_status == 'AMBER':
        html = html.replace('RAG_STYLE', 'style="background-color: #FFBF00;"')
        pdf_html_content = pdf_html_content.replace('RAG_STYLE', 'style="background-color: #FFBF00;"')
    elif rag_status == 'RED':
        html = html.replace('RAG_STYLE', 'style="background-color: red;color: white;"')
        pdf_html_content = pdf_html_content.replace('RAG_STYLE', 'style="background-color: red;color: white;"')

    pdf_html_content = pdf_html_content.replace("test_overview_table", full_first_table)
    pdf_html_content = pdf_html_content.replace("testing_comments", overall_comm)

    cycle_list = eng_df['Test_cycle'].unique()
    application_list = eng_df.Application.unique()
    trend_eng_df = trend_df[trend_df['EngName'] == eng_name]
    
    cycle_inner_html = ""
    pdf_cycle_inner_html = ""

    application_list = summary_df['Application'].unique()
    total_cycles = 0
    for application in application_list:
        project_list = summary_df[summary_df['Application'] == application]['Project Name'].unique()
        for proj in project_list:
            test_phase_list = summary_df[(summary_df['Project Name'] == proj) & (summary_df['Application'] == application)]['Test Phase'].unique()
            for test_phase in test_phase_list:
                test_cycle_list = summary_df[(summary_df['Project Name'] == proj) & (summary_df['Test Phase'] == test_phase) & (summary_df['Application'] == application)]['Test Cycle'].unique()
                for cycle in test_cycle_list:
                    print(f'{application} -> {proj} -> {test_phase} -> {cycle}')
                    fig = go.Figure()
                    cyc_ft_df = df[(df['Test_cycle'] == cycle) & (df['Test_Phase'] == test_phase) & (df['Project_Name'] == proj) & (df['Application'] == application)]

                    total = cyc_ft_df['TotalTests'].iloc[0]
                    passed = cyc_ft_df['PassedTests'].iloc[0]
                    failed = cyc_ft_df['FailedTests'].iloc[0]
                    blocked = cyc_ft_df['BlockedTests'].iloc[0]
                    na = cyc_ft_df['NATests'].iloc[0]
                    defer = cyc_ft_df['DeferredTests'].iloc[0]
                    not_executed = cyc_ft_df['NETests'].iloc[0]
                    descope = cyc_ft_df['DescopedTests'].iloc[0]
                    start_dt = cyc_ft_df['Start_Date'].iloc[0]
                    end_dt = cyc_ft_df['End_Date'].iloc[0]


                    cycle_df = trend_df[(trend_df['Test_cycle'] == cycle) & (trend_df['Test_Phase'] == test_phase) & (trend_df['Project_Name'] == proj) & (trend_df['Application'] == application) & (trend_df['Date'] >= (datetime.strptime(start_dt, '%Y-%m-%d %H:%M:%S') - timedelta(days=1)))]
                    fig.add_trace(go.Scatter(
                        x=cycle_df.Date, y=cycle_df.TestsCompleted, mode='lines', name='Tests completed to date'
                    ))

                    d1 = dsr_date_time + timedelta(days=1)
                    d2 = datetime.strptime(end_dt, '%Y-%m-%d %H:%M:%S')
                    d3 = datetime.strptime(start_dt, '%Y-%m-%d %H:%M:%S')
                    list_work_day = []
                    workdays_dt = workdays(d1, d2)
                    workdays_done = workdays(d3, d1)
                    for day in workdays_dt:
                        list_work_day.append(day.date())
                    pending_tests = not_executed + failed + blocked
                    print(list_work_day)
                    if pending_tests == 0:
                        list_of_count = []
                        list_work_day = []
                    else:
                        if len(list_work_day) == 0:
                            velocity = pending_tests
                        else: 
                            velocity = pending_tests / len(list_work_day)
                        print(velocity)
                        a = list(np.arange((total-pending_tests) +
                                 velocity, total+velocity, velocity))
                        list_of_count = [round(a) for a in a]
                        list_of_count.insert(
                            0, passed + na + defer + descope)
                        list_work_day.insert(0, dsr_date)
                    print(f"{start_dt} -> {list_work_day} -> {list_of_count}")
                    fig.add_trace(go.Scatter(x=list_work_day, y=list_of_count,
                                  mode='lines', name='Velocity required to complete',
                                  line=dict(dash='dash', color='goldenrod')))

                    fig.update_layout(yaxis_range=[0, total+4])

                    fig.add_hline(y=total, line_width=1, line_color="#5a287d")
                    fig.add_annotation(x=start_dt, y=total,
                                       text=f'Total no. of tests: {total}')
                    fig.add_vline(x=start_dt, line_width=1,
                                  line_dash="dash", line_color="white")
                    fig.add_vline(x=end_dt, line_width=1,
                                  line_dash="dash", line_color="white")
                    fig.update_layout(
                        margin=dict(l=20, r=20, t=60, b=20),
                        title_text= cycle,
                        title_x = 0.5,
                        paper_bgcolor='#5a287d',
                        plot_bgcolor='black',
                        width=700, height=300,
                        font=dict(color='White'),
                        xaxis=dict(showgrid=False),
                        yaxis=dict(showgrid=False)
                    )
                    corrected_cyclename = cycle.replace('/', '_')
                    image_name = f"images\\{eng_name}_{application}_{proj}_{test_phase}_{corrected_cyclename}.png"
                    pdf_image_name = f"{os.getcwd()}\\images\\{eng_name}_{application}_{proj}_{test_phase}_{corrected_cyclename}.png"
                    print(corrected_cyclename)
                    fig.write_image(image_name)

                    fig.data = []
                    fig.layout = {}
                    img_cid = make_msgid(domain='tam.com')
                    image_list.append((img_cid, image_name))

                    cycle_inner_html = cycle_inner_html + \
                        f"<div><h4 class='text-primary'><b><u>Cycle: {cycle}</b></u></h4>"
                    cycle_inner_html = cycle_inner_html + \
                        f"<div>{cycle} has completed {total-pending_tests} of {total} tests</div>"
                    pdf_cycle_inner_html = pdf_cycle_inner_html +  \
                        f"<div><h4 class='text-primary'><b><u>Cycle: {cycle}</b></u></h4>"
                    pdf_cycle_inner_html = pdf_cycle_inner_html + \
                        f"<div>{cycle} has completed {total-pending_tests} of {total} tests</div>"

                    if {total-pending_tests} == {total}:
                        sub_cyc__trend_df = cycle_df.copy()
                        sub_cyc__trend_df = sub_cyc__trend_df[(sub_cyc__trend_df['Test_cycle'] == cycle) & (sub_cyc__trend_df['Test_Phase'] == test_phase)]
                        sub_cyc__trend_df.reset_index(inplace=True)
                        # print(sub_cyc__trend_df['TestsCompleted'].ne(total).idxmax(), sub_cyc__trend_df.first_valid_index())
                        matched_index = sub_cyc__trend_df['TestsCompleted'].eq(total).idxmax()
                        # print(f'\t{matched_index} \t {total}')
                        # if sub_cyc__trend_df.shape[0] > matched_index + 1:
                            # break
                        if matched_index == 0:                            
                            cycle_status_text = f'''<p>
                            As of {sub_cyc__trend_df.iloc[matched_index]['Time'].split(' ')[0]}, all assigned tests were completed. After {matched_index+1} working day(s), we have passed {passed} test(s) with {descope + defer + na} test(s) identified as Not Applicable/Descoped/Deferred.
                            The velocity of tests to date is {round((total-pending_tests)/(matched_index+1),2)} test(s) per day and to complete on time we need to undertake 0 test(s) per day.
                            <br>At present there are {failed} failure(s) and {blocked} blocked script(s).
                            </p>
                            '''
                        else:                            
                            cycle_status_text = f'''<p>
                            As of {sub_cyc__trend_df.iloc[matched_index]['Time'].split(' ')[0]}, all assigned tests were completed. After {matched_index+1} working day(s), we have passed {passed} test(s) with {descope + defer + na} test(s) identified as Not Applicable/Descoped/Deferred.
                            The velocity of tests to date is {round((total-pending_tests)/(matched_index+1),2)} test(s) per day and to complete on time we need to undertake 0 test(s) per day.
                            <br>At present there are {failed} failure(s) and {blocked} blocked script(s).
                            </p>
                            '''

                    else:
                        cycle_status_text = f'''<p>
                        Testing is currently due to complete on {end_dt.split(' ')[0]}. After {len(workdays_done)} working day(s), we have passed {passed} test(s) with {descope + defer + na} test(s) identified as Not Applicable/Descoped/Deferred.
                        <br>The velocity of tests to date is {round((total-pending_tests)/len(workdays_done),2)} test(s) per day and to complete on time we need to undertake {round(velocity,2)} test(s) per day.
                        <br>At present there are {failed} failure(s) and {blocked} blocked script(s).
                        </p>
                        '''

                    # comments_df['DATE'] = pd.to_datetime(comments_df['DATE'])
                    print('------------------')
                    # print(comments_df['DATE'].dt.date)
                    print(comments_df[(comments_df['DATE'] == dsr_date)])
                    cycle_comm_df = comments_df[(comments_df['Application'] == application) & (comments_df['Project'] == proj) &
                                                (comments_df['Test Phase'] == test_phase) & (comments_df['Test Cycle Name'] == cycle)
                                                & (comments_df['DATE'].dt.strftime('%Y-%m-%d') == dsr_date.strftime('%Y-%m-%d'))]
                    try:
                        print(comments_df['DATE'].dt.strftime('%Y-%m-%d'), dsr_date.strftime('%Y-%m-%d'))
                        print(comments_df[comments_df['DATE'].dt.strftime('%Y-%m-%d') == dsr_date.strftime('%Y-%m-%d')])
                        print(cycle_comm_df)
                    except:
                        pass
                    try:
                        cyc_comment = cycle_comm_df['TM Comments'].iloc[0]
                    except:
                        cyc_comment = None
                    ####
                    cycle_inner_html = cycle_inner_html + cycle_status_text
                    pdf_cycle_inner_html = pdf_cycle_inner_html + cycle_status_text
                    prev_com = ""
                    overall_comm = ""
                    if type(cyc_comment) == str:
                        for comm in cyc_comment.split('\n'):
                            if prev_comm == "":
                                overall_comm = overall_comm + '<b>' + comm + '</b><br>'
                            else:
                                overall_comm = overall_comm + ' &nbsp&nbsp&nbsp&nbsp ' + comm + '<br>'
                            prev_comm = comm
                    try:
                        if len(overall_comm) > 0:
                            cycle_inner_html = cycle_inner_html + f"<h4>Cycle Comments:</h4>"
                            cycle_inner_html = cycle_inner_html + f'{overall_comm}' + '<br>'

                            pdf_cycle_inner_html = pdf_cycle_inner_html + f"<h4>Cycle Comments:</h4>"
                            pdf_cycle_inner_html = pdf_cycle_inner_html + f'{overall_comm}' + '<br>'
                    except:
                        pass


                    cycle_inner_html = cycle_inner_html + \
                        f"<img src='cid:{img_cid[1:-1]}'><br>"
                    # print(pdf_image_name)
                    pdf_cycle_inner_html = pdf_cycle_inner_html + f"<img src='{pdf_image_name}'><br>"
                    
                    execution_df.drop_duplicates(inplace=True)
                    exec_df.drop_duplicates(inplace=True)
                    # execution_df = exec_df.copy()
                    filtered_df = execution_df[
                        (execution_df.EngName == eng_name) & (execution_df.Application == application) &
                        (execution_df.Project_Name == proj) & (execution_df.Test_Phase == test_phase) &
                        (execution_df.Test_cycle == cycle)
                    ]
                    opp_filtered_df = execution_df[
                        (execution_df.EngName == eng_name) & (execution_df.Application == application) &
                        (execution_df.Project_Name == proj) & (execution_df.Test_Phase == test_phase) &
                        ~(execution_df.Test_cycle == cycle)
                    ]
                    filtered_exec_df = exec_df[
                        (exec_df.EngName == eng_name) & (exec_df.Application == application) &
                        (exec_df.Project_Name == proj) & (exec_df.Test_Phase == test_phase) &
                        (exec_df.Test_cycle == cycle)
                    ]
                    filtered_df.sort_values(by=['TestID', 'Timestamp'], inplace=True, ascending=False)
                    filtered_df.drop_duplicates(subset='TestID', inplace=True, keep='first')

                    filtered_df.loc[(filtered_df['Status']=='NotExecuted') & (filtered_df['reason'].str.contains('block', case=False)), 'Status'] = 'Blocked'
                    filtered_df.loc[(filtered_df['Status']=='NotExecuted') & (filtered_df['reason'].str.contains('defer', case=False)), 'Status'] = 'Deferred'
                    filtered_df.loc[(filtered_df['Status']=='NotExecuted') & (filtered_df['reason'].str.contains('descope', case=False)), 'Status'] = 'Descoped'
                    filtered_df.loc[(filtered_df['Status']=='NotExecuted') & (filtered_df['reason'].str.contains('N/A', case=False)), 'Status'] = 'N/A'

                    # print(f"{total} - {passed} - {failed} - {na} - {not_executed} - {blocked} - {defer} - {descope}")
                    # print(opp_filtered_df.columns)
                    # print(filtered_df.columns)
                    lc_list = ['Automation', 'Platform Regression Tests', 'Current Project Tests']
                    # for lc in lc_list:
                        # lc_filt_df = filtered_df[filtered_df['Lifecycle'] == lc]
                        # lc_opp_filt_df = opp_filtered_df[opp_filtered_df['Lifecycle'] == lc]
                    merged_df = pd.merge(filtered_df, opp_filtered_df, on=['TestID'], how="left")
                        # print('\t', lc)
                    basic = filtered_df.groupby(['Status', 'Lifecycle']).size().reset_index(name='Count')
                    new = merged_df[merged_df['Lifecycle_y'].isnull()].groupby(['Status_x', 'Lifecycle_x']).size().reset_index(name='Count')
                    new['Lifecycle_x'] = new['Lifecycle_x'].str.replace('Platform Regression Tests', 'Newly included Tests - Platform Regression Tests')
                    new['Lifecycle_x'] = new['Lifecycle_x'].str.replace('Automation', 'Newly included Tests - Automation')
                    new['Lifecycle_x'] = new['Lifecycle_x'].str.replace('Current Project Tests', 'Newly included Tests - Current Project Tests')
                    # new.drop_duplicates(inplace=True)
                    new.columns = ['Status', 'Lifecycle', 'Count']
                    final_value_df = pd.concat([basic,new], ignore_index=True, axis=0)
                    # print(final_value_df.pivot_table(index='Status', columns='Lifecycle', values='Count'))
                    pivot_values_table = final_value_df.pivot_table(index='Status', columns='Lifecycle', values='Count')
                    pivot_values_table.fillna(0, inplace=True)
                    pivot_values_table = pivot_values_table.astype(int, errors='ignore')
                    # print('\t', merged_df[merged_df['Lifecycle_y'].isnull()].shape[0])
                    # print('\t', merged_df[~merged_df['Lifecycle_y'].isnull()].shape[0])
                    if pivot_values_table.shape[0] > 0:
                        cycle_inner_html = cycle_inner_html + pivot_values_table.to_html()
                    total_cycles = total_cycles + 1
                inner_def_value = ""
                filtered_defects = defect_df[(defect_df['ProjectName'] == eng_name) & (defect_df['TAM Application'] == application) & (
                    defect_df['TAM Project/Release Name'].str.lower() == proj.lower()) & (defect_df['TAM Test Phase'] == test_phase)]
                closed_defects = filtered_defects[filtered_defects['State'] == 'Closed']
                print('CLOSED DEFECTS')
                closed_defects.drop_duplicates(subset=['Issue Number', 'ProjectName', 'TAM Application', 'TAM Project/Release Name', 'TAM Test Phase'], keep='first', inplace=True)
                print(closed_defects.head(2))
                print(closed_defects[['Issue Number', 'State', 'Root Cause']])
                open_defects = filtered_defects[filtered_defects['State'] != 'Closed']
                total_closed_def = closed_defects.shape[0]
                total_open_def = open_defects.shape[0]

                defect_html_inner = ""
                if total_closed_def == 0:
                    closed_str = "<div>" + \
                        "There are currently 0 closed defect(s)." + "</div>"
                else:
                    closed_str = "<div>" + \
                        f"There are currently {total_closed_def} closed defect(s). Their root cause analysis is listed below." + "</div>"
                    rca_df = closed_defects.groupby(
                        ['Root Cause']).size().reset_index()
                    rca_df.columns = ['Root Cause', 'Count']
                    rca_df.loc['Total'] = rca_df.sum(numeric_only=True)
                    rca_df.fillna('Total', inplace=True)
                    rca_df = rca_df.astype(int, errors='ignore')
                    # rca_df.loc[:,'Total'] = rca_df.sum(numeric_only=True, axis=1)
                    rca_df = rca_df.T
                    # rca_df.columns = [''] * len(rca_df.columns)
                    closed_str = closed_str + '<br>' + \
                        rca_df.to_html(header=False)

                if total_open_def == 0:
                    open_def_str = "There are currently 0 open defect(s)."
                else:
                    open_def_str = "<div>" + \
                        f"There are currently {total_open_def} open defect(s). Their breakdown is listed below:" + "</div>"
                    chk = open_defects.groupby(
                        ['State', 'Severity']).size().reset_index()
                    # chk.index = chk.index.droplevel(1)
                    chk.columns = ['State', 'Severity', 'Count']
                    open_df = chk.pivot(index=['Severity'], columns=[
                                        'State'], values=['Count']).reset_index()
                    open_df.fillna(0, inplace=True)
                    open_df.index.name = None
                    # open_df.columns.name = None
                    # open_df.index = open_df.index.droplevel(-1)
                    open_df.loc['Total'] = open_df.sum(numeric_only=True)
                    open_df.loc[:,'Total'] = open_df.sum(numeric_only=True, axis=1)
                    open_df.fillna('Total', inplace=True)
                    open_df = open_df.astype(int, errors='ignore')
                    # # print(open_df)
                    # # print(type(open_df))
                    open_def_str = open_def_str + '<br>' + open_df.to_html(index=False)
                inner_def_value = inner_def_value + f'''
                    <h4 class='text-primary'><b><u>Overall defect figures for {application} {proj} - {test_phase}:</u></b></h4>
                    {open_def_str}
                    <br>
                    {closed_str}
                    <br>
                    '''
                cycle_inner_html = cycle_inner_html + inner_def_value + '<br><hr>'
                pdf_cycle_inner_html = pdf_cycle_inner_html + inner_def_value + '<br><hr>'
            cycle_inner_html = cycle_inner_html

    html = html.replace("cycle_details", cycle_inner_html)
    html = html.replace("test_cycle_count", str(summary_df.shape[0]))
    html = html.replace("engagement_name", ENG_MAP.get(eng_name))
    pdf_html_content = pdf_html_content.replace("cycle_details", pdf_cycle_inner_html)
    pdf_html_content = pdf_html_content.replace("test_cycle_count", str(summary_df.shape[0]))
    pdf_html_content = pdf_html_content.replace("engagement_name", ENG_MAP.get(eng_name))
    msg.add_alternative(html, subtype='html')
    for image_cid, image_name in image_list:
        with open(image_name, 'rb') as img:
            # know the Content-Type of the image
            maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')

            # attach it
            msg.get_payload()[0].add_related(img.read(),
                                             maintype=maintype,
                                             subtype=subtype,
                                             cid=image_cid)
    # html = html + ""

    date_value = f'0{dsr_date.day}' if len(str(dsr_date.day)) == 1 else f'{dsr_date.day}'
    path_till_pdf = f'.\\emails\\{dsr_date.year}\\{dsr_date.strftime("%b")}\\{date_value}\\'
    try:
        os.makedirs(path_till_pdf)
    except:
        pass
    pdfkit.from_string(pdf_html_content, f'{path_till_pdf}\\{PDF_FILE_NAME.get(eng_name)}.pdf', options=options)
    write_email(html, image_list, filename=f"{path_till_pdf}\\{eng_name}", msg=msg, subject = f"{ENG_MAP.get(eng_name)} Execution Status Report {dsr_date.strftime('%d/%m/%Y')}")
