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
scenario_agg.drop(columns =['Field Name','Field name'])

def w_avg(x,column_to_be_averaged,weight):
    return(np.average(x[column_to_be_averaged], 
                      weights = x[weight]))

columns_to_be_averaged = ['Upstream Carbon Intensity (kgCO2eq/boe)', 'Midstream Carbon Intensity (kgCO2eq/boe)','Downstream Carbon Intensity (kgCO2eq/boe)']

scenario_aggregated = pd.concat([
scenario_agg.groupby(['Country','Aggregation','Scenario','toggle_value']).apply(
    lambda x:w_avg(x,col,'2020 Total Oil and Gas Production Volume (boe)')).rename(col) 
    for col in columns_to_be_averaged],axis=1)
scenario_aggregated.to_excel(sp_dir + '/Deep Dive page/Analytics/scenario_agg.xlsx')


