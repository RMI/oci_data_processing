import pandas as pd
import sqlite3
import numpy as np 

sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")
up_mid_down = pd.read_sql('select * from scenario_up_mid_down',connection)

# Aggregation for webtool 
agg_list = pd.read_csv('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/aggregation_list_CAfields.csv')
scenario = pd.DataFrame()

scenario['Field Name']=up_mid_down['Field_name']
scenario['Country'] = up_mid_down['Field location (Country)']
scenario['Scenario']=up_mid_down['Scenario']
scenario['toggle_value']=up_mid_down['toggle_value'].apply(lambda x: x.replace(':','').replace(' & ','_').lower())
scenario['toggle_stage']=up_mid_down['toggle_stage']
scenario['flaring_ghg(t/d)'] = up_mid_down['flaring_ghg(t/d)']
scenario['Total BOED']=up_mid_down['Total BOE Produced']

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



# Calculate Flare volume scenario by editing the flaring related GHG. 0.5 and 1.5 multiplier from default scenario
scenario_fv_def = scenario[(scenario['Scenario']=='Electrify')&(scenario['toggle_value']=='Off')].copy(deep=True)


scenario_fv_def['Scenario']='Flare Volume'

scenario_fv_def['toggle_value']='1'

scenario_fv_50 = scenario_fv_def.copy(deep=True)

scenario_fv_50['toggle_value']='0.5'

scenario_fv_50['Upstream Carbon Intensity (kgCO2eq/boe)']=scenario_fv_50['Upstream Carbon Intensity (kgCO2eq/boe)']-0.5*scenario_fv_50['flaring_ghg(t/d)']*1000/scenario_fv_50['Total BOED']

scenario_fv_150 = scenario_fv_def.copy(deep=True)

scenario_fv_150['toggle_value']='1.5'

scenario_fv_150['Upstream Carbon Intensity (kgCO2eq/boe)']=scenario_fv_150['Upstream Carbon Intensity (kgCO2eq/boe)']+0.5*scenario_fv_150['flaring_ghg(t/d)']*1000/scenario_fv_150['Total BOED']

scenario=pd.concat([scenario,scenario_fv_def,scenario_fv_50,scenario_fv_150])

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
scenario['Downstream: Transport to Consumers (kgCO2eq/boe)'] =(up_mid_down['Transport Emissions Intensity (kg CO2eq. /BOE)']
    + up_mid_down['Transport Emissions Intensity (kg CO2eq. /BOE).1'] 
    + upstream_gmj_kgboe_convert('l-Total GHG emissions')
    + upstream_gmj_kgboe_convert('g-Total GHG emissions'))

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

scenario['Downstream: Natural Gas (kgCO2eq/boe)'] = \
    up_mid_down['Natural Gas Combustion Emissions Intensity (kg CO2eq. / BOE)']
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
    + scenario['Downstream: Natural Gas (kgCO2eq/boe)'] 

scenario=scenario[['Field Name', 'Country', 'Scenario','toggle_value','toggle_stage',
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
scenario_agg.groupby(['Country','Aggregation','Scenario','toggle_value','toggle_stage']).apply(
    lambda x:w_avg(x,col,'2020 Total Oil and Gas Production Volume (boe)')).rename(col) 
    for col in columns_to_be_averaged],axis=1)

#scenario_aggregated.to_excel(sp_dir + '/Deep Dive page/Analytics/scenario_agg_1.xlsx')



scenario_aggregated.reset_index(inplace = True)

scenario_aggregated.drop(columns = 'Country',inplace = True)

toggles = scenario_aggregated.melt(id_vars=['Aggregation','Scenario','toggle_value','toggle_stage'])


toggles['stage']=toggles['variable'].apply(lambda x: x.split()[0].lower())

toggles.drop(columns = 'variable',inplace = True)



toggles = toggles.pivot(index = ['Scenario','toggle_value','stage','toggle_stage'],columns = 'Aggregation',values = 'value').reset_index()

#toggles.to_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Deep Dive page/Analytics/scenario_agg_pivot.xlsx')

toggles.rename(columns = {'Scenario':'slider','toggle_value':'value'},inplace = True)



toggle_map = pd.read_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Deep Dive page/sensitivity_analysis.xlsx','toggle_map')

toggle_map =toggle_map[['slider','new_slider','Default?','toggle_value','value']]

toggle_map['value']=toggle_map['value'].apply(lambda x:str(x))


toggles = toggles.merge(toggle_map,how = 'left',indicator = True)


toggles[toggles['_merge']!='both']

toggles.drop(columns =['slider','value','_merge'],inplace = True)

toggles.rename(columns ={'new_slider':'slider','toggle_value':'value'},inplace = True)


# Add 2020 TS from the default results

up_2020 = toggles[(toggles['slider']=='Renewable Electricity')&(toggles['value']=='Off')].copy(deep=True)
up_2020['slider']='Time Series'
up_2020['value']='2020'
up_2020['Default?']='Y'

toggles=pd.concat([toggles,up_2020],axis=0)


# Scaling midstream scenarios calculated by Michael

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
from scenario_up_mid_down
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
midtoggles['toggle_stage']='Midstream'


#downtoggles = pd.read_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Deep Dive page/sensitivity_analysis.xlsx','Downstream_result')

#downtoggles.drop(columns = 'Key',inplace = True)

#downtoggles=downtoggles[(downtoggles['Williston'].notnull())&(downtoggles['slider'].notnull())]
#downtoggles['toggle_stage']='Downstream'


# ## Fix GWP 20 scaling inconsistency with default by setting it as Scenario default
# GWP_20 = toggles[(toggles['slider']=='Renewable Electricity')&(toggles['value']=='Off')].copy(deep=True)
# GWP_20['slider']='Global Warming Potential'
# GWP_20['value'] ='20 yr'

# Continue to fix GWP 100,5,10 based on the methane concentration

all_scenarios = pd.concat([toggles,midtoggles])
all_scenarios = all_scenarios[all_scenarios.columns[-3:].to_list()+all_scenarios.columns[:-3].to_list()]
all_scenarios.to_excel('/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Webtool updates/basedata/slider_6.xlsx',index = False)