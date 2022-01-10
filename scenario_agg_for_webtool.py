import pandas as pd
import sqlite3
import numpy as np 

sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")
up_mid_down = pd.read_sql('select * from scenario_up_mid_down_results',connection)

# Aggregation for webtool 
agg_list = pd.read_csv('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/aggregation_list_CAfields.csv')
scenario = pd.DataFrame()

scenario['Field Name']=up_mid_down['Field_name']
scenario['Country'] = up_mid_down['Field location (Country)']
scenario['Scenario']=up_mid_down['Scenario']
scenario['toggle_value']=up_mid_down['toggle_value']

def upstream_gmj_kgboe_convert(x):
    return(up_mid_down[x]*up_mid_down['ES_MJperd']/up_mid_down['Total BOE Produced']/1000)

upstream_emission_category = {
    'Upstream: Exploration (kgCO2eq/boe)':'e-Total GHG emissions',
    'Upstream: Drilling & Development (kgCO2eq/boe)':'d-Total GHG emissions',
    'Upstream: Crude Production & Extraction (kgCO2eq/boe)':'p-Total GHG emissions',
    'Upstream: Surface Processing (kgCO2eq/boe)':'s-Total GHG emissions',
    'Upstream: Maintenance (kgCO2eq/boe)':'m-Total GHG emissions',
    'Upstream: Waste Disposal (kgCO2eq/boe)':'w-Total GHG emissions',
    'Upstream: Crude Transport (kgCO2eq/boe)':'t-Total GHG emissions',
    'Upstream: Other Small Sources (kgCO2eq/boe)':'Other small sources',
    'Upstream: Offsite emissions credit/debit (kgCO2eq/boe)':'Offsite emissions credit/debit',
    'Upstream: Carbon Dioxide Sequestration (kgCO2eq/boe)':'CSS-Total CO2 sequestered'
}

for i in upstream_emission_category:
    scenario[i] = upstream_gmj_kgboe_convert(upstream_emission_category[i])

scenario['Upstream Carbon Intensity (kgCO2eq/boe)']=sum([scenario[i] for i in upstream_emission_category])
# Adjust the combustion ratio for Electrifying Scenario 
scenario['Upstream Carbon Intensity (kgCO2eq/boe)'] = np.where((scenario['Scenario']=='Electrify')&(scenario['toggle_value']=='On'), scenario['Upstream Carbon Intensity (kgCO2eq/boe)']*(1-up_mid_down['combustion_ratio']),scenario['Upstream Carbon Intensity (kgCO2eq/boe)'])
def midstream_scaler(x):
    return(up_mid_down[x]*up_mid_down['Oil production volume']/up_mid_down['Total BOE Produced'])

midstream_emission_category_CO2 ={
    'Midstream: Electricity (kgCO2eq/boe)':'Electricity',
    'Midstream: Heat (kgCO2eq/boe)':'Heat',
    'Midstream: Steam (kgCO2eq/boe)':'Steam',
    'Midstream: Hydrogen via SMR (kgCO2eq/boe)':'Hydrogen via SMR',
    'Midstream: Hydrogen via CNR (kgCO2eq/boe)':'Hydrogen via CNR',
    'Midstream: Other Emissions (kgCO2eq/boe)':'Other Emissions'
}

for i in midstream_emission_category_CO2:
    scenario[i] = midstream_scaler(midstream_emission_category_CO2[i])

scenario['Midstream Carbon Intensity (kgCO2eq/boe)']=sum([scenario[i] for i in midstream_emission_category_CO2])

# Downstream Transport to Consumer include refinery product transport and NGL transport
scenario['Downstream: Transport to Consumers (kgCO2eq/boe)'] =up_mid_down['Transport Emissions Intensity (kg CO2eq. /BOE)']+\
up_mid_down['Transport Emissions Intensity (kg CO2eq. /BOE).1'] 

scenario['Downstream: Gasoline for Cars (kgCO2eq/boe)']=\
    up_mid_down['Gasoline Combustion Emissions Intensity (kg CO2eq. / BOE)']
scenario['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']=\
    up_mid_down['Jet Fuel Combustion Emissions Intensity (kg CO2eq. / BOE)']
scenario['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)'] = \
    up_mid_down['Diesel Combustion Emissions Intensity (kg CO2eq. / BOE)']
scenario['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] = \
    up_mid_down['Fuel Oil Combustion Emissions Intensity (kg CO2eq. / BOE)']
scenario['Downstream: Petroleum Coke for Power (kgCO2eq/boe)'] = \
    up_mid_down['Coke Combustion Emissions Intensity (kg CO2eq. / BOE)'] + up_mid_down['Coke Combustion Emissions Intensity (kg CO2eq. / BOE).1']
scenario['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)'] = \
    up_mid_down['Residual fuels Combustion Emissions Intensity (kg CO2eq. / BOE)']
scenario['Downstream: Natural Gas Liquids (kgCO2eq/boe)'] =\
    up_mid_down['Total NGL Combustion Emissions Intensity (kg CO2eq. / BOE)']
scenario['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] = \
    up_mid_down['Liquefied Petroleum Gases (LPG) Combustion Emissions Intensity (kg CO2eq. / BOE)']
scenario['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']=\
    up_mid_down['Total Process Emissions Intensity (kg CO2eq./boe total)']

# Adding OPGEE LNG and Natural Gas distribution into OPEM's Natural Gas Combustion 
scenario['Dwonstream: Natural Gas (kgCO2eq/boe)'] = \
    up_mid_down['Natural Gas Combustion Emissions Intensity (kg CO2eq. / BOE)']\
    +upstream_gmj_kgboe_convert('l-Total GHG emissions')+upstream_gmj_kgboe_convert('g-Total GHG emissions')

scenario['Downstream Carbon Intensity (kgCO2eq/boe)'] = scenario['Downstream: Transport to Consumers (kgCO2eq/boe)']\
    + scenario['Downstream: Gasoline for Cars (kgCO2eq/boe)'] \
    + scenario['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']\
    + scenario['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)']\
    + scenario['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] \
    + scenario['Downstream: Petroleum Coke for Power (kgCO2eq/boe)']\
    + scenario['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)']\
    + scenario['Downstream: Natural Gas Liquids (kgCO2eq/boe)']\
    + scenario['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] \
    + scenario['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']\
    + scenario['Dwonstream: Natural Gas (kgCO2eq/boe)'] 

scenario=scenario[['Field Name', 'Country', 'Scenario','toggle_value',
'Upstream Carbon Intensity (kgCO2eq/boe)', 'Midstream Carbon Intensity (kgCO2eq/boe)','Downstream Carbon Intensity (kgCO2eq/boe)']]
scenario['2020 Total Oil and Gas Production Volume (boe)']=up_mid_down['Total BOE Produced']*365
scenario_agg = scenario.merge(agg_list,left_on = 'Field Name', right_on='Field name',how = 'left')

scenario_agg.drop(columns =['Field Name','Field name'],inplace = True)
# Remove solar steam 68 cases 
scenario_agg = scenario_agg[~((scenario_agg['Scenario']=='Solarsteam')&(scenario_agg['toggle_value']=='68'))]

def w_avg(x,column_to_be_averaged,weight):
    return(np.average(x[column_to_be_averaged], 
                      weights = x[weight]))


columns_to_be_averaged = ['Upstream Carbon Intensity (kgCO2eq/boe)', 'Midstream Carbon Intensity (kgCO2eq/boe)','Downstream Carbon Intensity (kgCO2eq/boe)']

scenario_aggregated = pd.concat([
scenario_agg.groupby(['Country','Aggregation','Scenario','toggle_value']).apply(
    lambda x:w_avg(x,col,'2020 Total Oil and Gas Production Volume (boe)')).rename(col) 
    for col in columns_to_be_averaged],axis=1)

#scenario_aggregated.to_excel(sp_dir + '/Deep Dive page/Analytics/scenario_agg_1.xlsx')


uptoggles = scenario_aggregated.reset_index()

uptoggles.drop(columns = 'Country',inplace = True)

uptoggles = uptoggles.melt(id_vars=['Aggregation','Scenario','toggle_value'])

uptoggles['stage']=uptoggles['variable'].apply(lambda x: x.split()[0].lower())

uptoggles.drop(columns = 'variable',inplace = True)

uptoggles = uptoggles.pivot(index = ['Scenario','toggle_value','stage'],columns = 'Aggregation',values = 'value').reset_index()

uptoggles.to_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Deep Dive page/Analytics/scenario_agg_pivot.xlsx')

uptoggles.rename(columns = {'Scenario':'slider','toggle_value':'value'},inplace = True)

toggle_map = pd.read_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Deep Dive page/sensitivity_analysis.xlsx','upstream_toggle_map')

toggle_map =toggle_map[['slider','new_slider','Default?','toggle_value','value']]

toggle_map['value']=toggle_map['value'].apply(lambda x:str(x))

uptoggles_m = uptoggles.merge(toggle_map,how = 'left',indicator = True)

uptoggles_m.drop(columns =['slider','value','_merge'],inplace = True)

uptoggles_m.rename(columns ={'new_slider':'slider','toggle_value':'value'},inplace = True)

# Add 2020 TS from the default results

up_2020 = uptoggles_m[(uptoggles_m['slider']=='Electrify with Renewables')&(uptoggles_m['value']=='Off')]

up_2020['slider']='Time Series'
up_2020['value']='2020'
up_2020['Default?']='Y'

uptoggles_m=pd.concat([uptoggles_m,up_2020],axis=0)

# Calculate GWP scenarios based on up_mid_down default values

import sqlite3
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")
up_mid_down = pd.read_sql('select * from up_mid_downstream_results',connection)
# select only 2020 results for webtool
up_mid_down = up_mid_down[up_mid_down['year']=='2020'].reset_index()

OCI_infobase=pd.DataFrame()

OCI_infobase['Field Name']=up_mid_down['Field_name']
OCI_infobase['Country']=up_mid_down['Field location (Country)']

OCI_infobase['2020 Total Oil and Gas Production Volume (boe)'] = up_mid_down['Total BOE Produced']*365


OCI_infobase['2020 Crude Production Volume (bbl)']= up_mid_down['Oil production volume']*365

OCI_infobase['2020 Total Oil and Gas Production Volume (boe)']= up_mid_down['Total BOE Produced']*365

# Upstream methane accounts for all of flaring, part of fugitives/venting (Production, Gathering and Boosting, Secondary production)
OCI_infobase['Upstream Methane Intensity (kgCH4/boe)']=(up_mid_down['venting_ch4_uponly(t/d)']+up_mid_down['fugitive_ch4_uponly(t/d)']+up_mid_down['flaring_ch4(t/d)'])*365\
/(OCI_infobase['2020 Total Oil and Gas Production Volume (boe)'])*1000

# Midstream Methane Intensity calculation, use 100yr total emission from midstream runs and get fraction of CO2eq and convert back to methane 
OCI_infobase['Midstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total refinery processes']\
    *OCI_infobase['2020 Crude Production Volume (bbl)']/OCI_infobase['2020 Crude Production Volume (bbl)']\
    *up_mid_down['emission_frac_CH4']/30

# Downstream mehtane accounts for all OPEM methane output + upstream natural gas distribution /LNG
OCI_infobase['Downstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total Transport CH4 Emissions Intensity (kg CH4. / BOE)']+\
up_mid_down['Total Transport CH4 Emissions Intensity (kg CH4. / BOE).1']+up_mid_down['Total Combustion CH4 Emissions Intensity (kg CH4. / BOE)']+\
up_mid_down['Total Combustion CH4 Emissions Intensity (kg CH4. / BOE)']+\
up_mid_down['Total ProcessCH4  Emissions Intensity (kg CH4/boe total)']+ up_mid_down['tCH4/year']/\
(OCI_infobase['2020 Total Oil and Gas Production Volume (boe)'])*1000-OCI_infobase['Upstream Methane Intensity (kgCH4/boe)']


def upstream_gmj_kgboe_convert(x):
    return(up_mid_down[x]*up_mid_down['ES_MJperd']/up_mid_down['Total BOE Produced']/1000)

upstream_emission_category = {
    'Upstream: Exploration (kgCO2eq/boe)':'e-Total GHG emissions',
    'Upstream: Drilling & Development (kgCO2eq/boe)':'d-Total GHG emissions',
    'Upstream: Crude Production & Extraction (kgCO2eq/boe)':'p-Total GHG emissions',
    'Upstream: Surface Processing (kgCO2eq/boe)':'s-Total GHG emissions',
    'Upstream: Maintenance (kgCO2eq/boe)':'m-Total GHG emissions',
    'Upstream: Waste Disposal (kgCO2eq/boe)':'w-Total GHG emissions',
    'Upstream: Crude Transport (kgCO2eq/boe)':'t-Total GHG emissions',
    'Upstream: Other Small Sources (kgCO2eq/boe)':'Other small sources',
    'Upstream: Offsite emissions credit/debit (kgCO2eq/boe)':'Offsite emissions credit/debit',
    'Upstream: Carbon Dioxide Sequestration (kgCO2eq/boe)':'CSS-Total CO2 sequestered'
}

OCI_info100=pd.DataFrame()
OCI_info100['Field Name']=up_mid_down['Field_name']
OCI_info100['Country'] = up_mid_down['Field location (Country)']

for i in upstream_emission_category:
    OCI_info100[i] = upstream_gmj_kgboe_convert(upstream_emission_category[i])

OCI_info100['Upstream Carbon Intensity (kgCO2eq/boe)']=sum([OCI_info100[i] for i in upstream_emission_category])

def midstream_scaler(x):
    '''scale midstream emission from kgCO2eq/bbl to kgCO2eq/boe'''
    return(up_mid_down[x]*up_mid_down['Oil production volume']/up_mid_down['Total BOE Produced'])

midstream_emission_category_CO2 ={
    'Midstream: Electricity (kgCO2eq/boe)':'Electricity',
    'Midstream: Heat (kgCO2eq/boe)':'Heat',
    'Midstream: Steam (kgCO2eq/boe)':'Steam',
    'Midstream: Hydrogen via SMR (kgCO2eq/boe)':'Hydrogen via SMR',
    'Midstream: Hydrogen via CNR (kgCO2eq/boe)':'Hydrogen via CNR',
    'Midstream: Other Emissions (kgCO2eq/boe)':'Other Emissions'
}



for i in midstream_emission_category_CO2:
    OCI_info100[i] = midstream_scaler(midstream_emission_category_CO2[i])


OCI_info100['Midstream Carbon Intensity (kgCO2eq/boe)']=sum([OCI_info100[i] for i in midstream_emission_category_CO2])

# Downstream Transport to Consumer include refinery product transport and NGL transport
OCI_info100['Downstream: Transport to Consumers (kgCO2eq/boe)'] =up_mid_down['Transport Emissions Intensity (kg CO2eq. /BOE)']+\
up_mid_down['Transport Emissions Intensity (kg CO2eq. /BOE).1'] 

OCI_info100['Downstream: Gasoline for Cars (kgCO2eq/boe)']=\
    up_mid_down['Gasoline Combustion Emissions Intensity (kg CO2eq. / BOE)']
OCI_info100['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']=\
    up_mid_down['Jet Fuel Combustion Emissions Intensity (kg CO2eq. / BOE)']
OCI_info100['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)'] = \
    up_mid_down['Diesel Combustion Emissions Intensity (kg CO2eq. / BOE)']
OCI_info100['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] = \
    up_mid_down['Fuel Oil Combustion Emissions Intensity (kg CO2eq. / BOE)']
OCI_info100['Downstream: Petroleum Coke for Power (kgCO2eq/boe)'] = \
    up_mid_down['Coke Combustion Emissions Intensity (kg CO2eq. / BOE)'] + up_mid_down['Coke Combustion Emissions Intensity (kg CO2eq. / BOE).1']
OCI_info100['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)'] = \
    up_mid_down['Residual fuels Combustion Emissions Intensity (kg CO2eq. / BOE)']
OCI_info100['Downstream: Natural Gas Liquids (kgCO2eq/boe)'] =\
    up_mid_down['Total NGL Combustion Emissions Intensity (kg CO2eq. / BOE)']
OCI_info100['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] = \
    up_mid_down['Liquefied Petroleum Gases (LPG) Combustion Emissions Intensity (kg CO2eq. / BOE)']
OCI_info100['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']=\
    up_mid_down['Total Process Emissions Intensity (kg CO2eq./boe total)']

# Adding OPGEE LNG and Natural Gas distribution into OPEM's Natural Gas Combustion 
OCI_info100['Dwonstream: Natural Gas (kgCO2eq/boe)'] = \
    up_mid_down['Natural Gas Combustion Emissions Intensity (kg CO2eq. / BOE)']\
    +upstream_gmj_kgboe_convert('l-Total GHG emissions')+upstream_gmj_kgboe_convert('g-Total GHG emissions')

OCI_info100['Downstream Carbon Intensity (kgCO2eq/boe)'] = OCI_info100['Downstream: Transport to Consumers (kgCO2eq/boe)']\
    + OCI_info100['Downstream: Gasoline for Cars (kgCO2eq/boe)'] \
    + OCI_info100['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']\
    + OCI_info100['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)']\
    + OCI_info100['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] \
    + OCI_info100['Downstream: Petroleum Coke for Power (kgCO2eq/boe)']\
    + OCI_info100['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)']\
    + OCI_info100['Downstream: Natural Gas Liquids (kgCO2eq/boe)']\
    + OCI_info100['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] \
    + OCI_info100['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']\
    + OCI_info100['Dwonstream: Natural Gas (kgCO2eq/boe)'] 

OCI_info100['2020 Total Oil and Gas Production Volume (boe)']= up_mid_down['Total BOE Produced']*365

# Start Aggregation 

## Aggregation to desired field/basin level
agg_list = pd.read_csv('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/aggregation_list_CAfields.csv')

OCI_infobase_agg = pd.merge(OCI_infobase, agg_list,left_on = 'Field Name', right_on='Field name',how = 'left')

OCI_infobase_aggregated = pd.concat([OCI_infobase_agg.groupby(['Country','Aggregation']).agg({
                                             '2020 Total Oil and Gas Production Volume (boe)':'sum'}),
OCI_infobase_agg.groupby(['Country','Aggregation']).apply(lambda gp:np.average(gp['Upstream Methane Intensity (kgCH4/boe)'], weights = gp['2020 Total Oil and Gas Production Volume (boe)'])).rename('Upstream Methane Intensity (kgCH4/boe)'),
OCI_infobase_agg.groupby(['Country','Aggregation']).apply(lambda gp:np.average(gp['Midstream Methane Intensity (kgCH4/boe)'], weights = gp['2020 Total Oil and Gas Production Volume (boe)'])).rename('Midstream Methane Intensity (kgCH4/boe)'),
OCI_infobase_agg.groupby(['Country','Aggregation']).apply(lambda gp:np.average(gp['Downstream Methane Intensity (kgCH4/boe)'], weights = gp['2020 Total Oil and Gas Production Volume (boe)'])).rename('Downstream Methane Intensity (kgCH4/boe)')],axis=1)


OCI_infobase_aggregated.reset_index(inplace = True)

OCI_info100_agg = pd.merge(OCI_info100, agg_list,left_on = 'Field Name', right_on='Field name',how = 'left')


def w_avg(x,column_to_be_averaged,weight):
    return(np.average(x[column_to_be_averaged], 
                      weights = x[weight]))

columns_to_be_averaged = ['Upstream Carbon Intensity (kgCO2eq/boe)',
 'Midstream Carbon Intensity (kgCO2eq/boe)',
 'Downstream Carbon Intensity (kgCO2eq/boe)']
OCI_info100_aggregated = pd.concat([
OCI_info100_agg.groupby(['Country','Aggregation']).apply(
    lambda x:w_avg(x,col,'2020 Total Oil and Gas Production Volume (boe)')).rename(col) 
    for col in columns_to_be_averaged],axis=1)

OCI_info100_aggregated.reset_index(inplace = True)

Scenario_fields = scenario_aggregated.reset_index()['Aggregation'].unique()

methane = OCI_infobase_aggregated[OCI_infobase_aggregated['Aggregation'].isin(Scenario_fields)].drop(columns =['Country','2020 Total Oil and Gas Production Volume (boe)'])

GWP_scenario = dict()

GWP_scenario['100 yr']=OCI_info100_aggregated[OCI_info100_aggregated['Aggregation'].isin(Scenario_fields)].drop(columns = 'Country')

GWP_scenario['20 yr'] = (GWP_scenario['100 yr'].set_index('Aggregation')+methane.set_index('Aggregation').values*55).reset_index()

GWP_scenario['5 yr'] = (GWP_scenario['100 yr'].set_index('Aggregation')+methane.set_index('Aggregation').values*90).reset_index()

GWP_scenario['10 yr'] = (GWP_scenario['100 yr'].set_index('Aggregation')+methane.set_index('Aggregation').values*70).reset_index()

GWPtoggles = pd.concat(GWP_scenario).reset_index()

GWPtoggles.rename(columns ={'level_0':'toggle_value'},inplace = True)

GWPtoggles.drop(columns = 'level_1',inplace = True)

GWPtoggles['slider']='Global Warming Potential'

GWPtoggles['Default?']=np.where(GWPtoggles['toggle_value']=='20 yr','Y','N')

GWPtoggles = GWPtoggles.melt(id_vars = ['slider','toggle_value','Default?','Aggregation'])

GWPtoggles['stage'] = GWPtoggles['variable'].apply(lambda x: x.split()[0].lower())

GWPtoggles = GWPtoggles.pivot(index = ['slider','toggle_value','Default?','stage'],columns = 'Aggregation',values = 'value').reset_index()

GWPtoggles.rename(columns = {'toggle_value':'value'},inplace = True)

# Scaling midstream scenarios calculated by Michael

import pandas as pd

midstream = pd.read_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Deep Dive page/sensitivity_analysis.xlsx','Midstream_result')

midstream=midstream.loc[1:]

midstream.dropna(how='all',axis=1, inplace=True)

midstream = midstream.melt(id_vars=['slider','Key','Default?','toggle_value','stage'])

midstream.rename(columns = {'variable':'Field_name','value':'Midstream Carbon Intensity (kgCO2eq/bbl)'},inplace = True)

# Get Default Scenario Volume from up_mid_down
sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'
import sqlite3
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")
up_mid_down = pd.read_sql('''select "Field_name", "Oil production volume", "Total BOE Produced"
from scenario_up_mid_down_results
where Scenario =="Electrify" and toggle_value == "Off"''',connection)

midstream_volume = midstream.merge(up_mid_down,how = 'left',indicator = True)

# Aggregation based on aggregation list
agg_list = pd.read_csv('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/aggregation_list_CAfields.csv')

midstream_agg = midstream_volume.merge(agg_list,left_on='Field_name',right_on='Field name',how = 'left')

midstream_agg.drop(columns =['Field_name','Field name'],inplace = True)

midstream_agg['Midstream Carbon Emission (kgCO2eq)']=midstream_agg['Midstream Carbon Intensity (kgCO2eq/bbl)']*midstream_agg['Oil production volume']

midstream_agg.drop(columns=['Midstream Carbon Intensity (kgCO2eq/bbl)','Oil production volume','_merge'],inplace = True)

mid_stream_aggregated = midstream_agg.groupby(['Aggregation','slider','Key','Default?','toggle_value','stage'],as_index=False).agg({'Midstream Carbon Emission (kgCO2eq)':'sum','Total BOE Produced':'sum'})

mid_stream_aggregated['Midstream Carbon Intensity (kgCO2eq/boe)']=mid_stream_aggregated['Midstream Carbon Emission (kgCO2eq)']/mid_stream_aggregated['Total BOE Produced']

mid_stream_aggregated.drop(columns = ['Midstream Carbon Emission (kgCO2eq)','Total BOE Produced'],inplace = True)

midtoggles = mid_stream_aggregated.pivot(index = ['slider','Key','Default?','toggle_value','stage'],columns = 'Aggregation',values = 'Midstream Carbon Intensity (kgCO2eq/boe)').reset_index()

midtoggles.drop(columns = 'Key',inplace = True)

midtoggles.rename(columns ={'toggle_value':'value'},inplace = True)

downtoggles = pd.read_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Deep Dive page/sensitivity_analysis.xlsx','Downstream_result')

downtoggles.drop(columns = 'Key',inplace = True)

downtoggles=downtoggles[downtoggles['Williston'].notnull()]

all_scenarios = pd.concat([uptoggles_m,midtoggles,downtoggles,GWPtoggles],axis = 0)

all_scenarios = all_scenarios[all_scenarios.columns[-3:].to_list()+all_scenarios.columns[:-3].to_list()]

all_scenarios.to_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Webtool updates/basedata/slider.xlsx',index = False)

