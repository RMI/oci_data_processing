import pandas as pd
import numpy as np
sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'
up_mid_down = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Postprocessed_outputs_2/downstream_postprocessed_fix.csv')
#up_mid_down = df #df[df['gwp']==20]
OCI_info = pd.DataFrame()
# select relevant columns from up_mid_down to OCI_info
OCI_info['Field Name']=up_mid_down['Field_name']
OCI_info['Country']=up_mid_down['Field location (Country)']
OCI_info['Assay Name']=up_mid_down['Assay Name']
OCI_info['API Gravity']=up_mid_down['API gravity']
OCI_info['Sulfur Content (wt%)']=up_mid_down['sulfur']
OCI_info['Gas composition H2S']=up_mid_down['Gas composition H2S']
OCI_info['Gas composition CO2'] = up_mid_down['Gas composition CO2']
OCI_info['Gas composition C1']=up_mid_down['Gas composition C1']
OCI_info['2020 Total Oil and Gas Production Volume (boe)'] = up_mid_down['Total_BOE_Produced']*365
OCI_info['Location']=np.where(up_mid_down['Offshore?']==0,'Onshore', 'Offshore')
OCI_info['Max Depth (ft)']=up_mid_down['Field depth']
OCI_info['Water-to-oil Ratio (bbl water/bbl oil)']=up_mid_down['Water-to-oil ratio (WOR)']
OCI_info['Gas shipped as LNG']=np.where(up_mid_down['lng?']==True,1,0)
# If any of the following operation is 1 in OPGEE input, it's considered as Enhanced Oil Recovery
OCI_info['Enhanced recovery']=up_mid_down[['Natural gas reinjection','Water flooding','Gas lifting',
                                            'Gas flooding','Steam flooding']].any(axis='columns')
OCI_info['Fracked'] = up_mid_down['frack?']
OCI_info['gwp']=up_mid_down['gwp']
OCI_info['Default Refinery Configuration']=up_mid_down['Default Refinery']
OCI_info['Heating Value Processed Oil and Gas (MJ/d)']=up_mid_down['ES_MJperd']
OCI_info['Years in Production']=up_mid_down['Field age']
OCI_info['Number of Producing Wells']=up_mid_down['Number of producing wells']
OCI_info['Gas-to-Oil Ratio (scf/bbl)']=up_mid_down['Gas-to-oil ratio (GOR)']
OCI_info['Flaring-to-Oil Ratio (scf flared/bbl)'] = up_mid_down['Flaring-to-oil ratio']
OCI_info['Steam-to-Oil Ratio (bbl steam/bbl oil)'] = up_mid_down['Steam-to-oil ratio (SOR)']
OCI_info['2020 Crude Production Volume (bbl)']= up_mid_down['Oil production volume']*365
OCI_info['2020 Total Oil and Gas Production Volume (boe)']= up_mid_down['Total_BOE_Produced']*365
# Upstream methane accounts for all of flaring, part of fugitives/venting (Production, Gathering and Boosting, Secondary production)
OCI_info['Upstream Methane Intensity (kgCH4/boe)']=(up_mid_down['venting_ch4_uponly(t/d)']+up_mid_down['fugitive_ch4_uponly(t/d)']+up_mid_down['flaring_ch4(t/d)'])*365\
/(OCI_info['2020 Total Oil and Gas Production Volume (boe)'])*1000
# Midstream Methane Intensity calculation, use 100yr total emission from midstream runs and get fraction of CO2eq and convert back to methane 
OCI_info['Midstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total refinery processes']\
    *OCI_info['2020 Crude Production Volume (bbl)']/OCI_info['2020 Crude Production Volume (bbl)']\
    *up_mid_down['emission_frac_CH4']/30
# Downstream mehtane accounts for all OPEM methane output + upstream natural gas distribution /LNG
OCI_info['Downstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total_CH4_Emissions_kgCH4/BOE']+\
    up_mid_down['tCH4/year']/OCI_info['2020 Total Oil and Gas Production Volume (boe)']*1000-OCI_info['Upstream Methane Intensity (kgCH4/boe)']
OCI_info['Total Methane Intensity (kgCH4/boe)'] = OCI_info['Upstream Methane Intensity (kgCH4/boe)']+\
OCI_info['Midstream Methane Intensity (kgCH4/boe)'] +OCI_info['Downstream Methane Intensity (kgCH4/boe)']
OCI_info['Upstream Methane Emission Rate (NGSI Standard %)'] = up_mid_down['tCH4/year-miQ']*up_mid_down['ES_Gas_output(MJ/d)']/up_mid_down['ES_MJperd']\
        /(up_mid_down['FS_Gas_at_Wellhead(t/d)']*365*up_mid_down['Gas composition C1']/100)
#If gas output is negative in OPGEE, zero it out to avoid a negative methane emission rate
OCI_info['Upstream Methane Emission Rate (NGSI Standard %)'] = np.where(OCI_info['Upstream Methane Emission Rate (NGSI Standard %)']<0,0,OCI_info['Upstream Methane Emission Rate (NGSI Standard %)']*100)
OCI_info['Upstream Methane Emission Rate (gCH4/Total MJ Produced)'] = up_mid_down['tCH4/year']*1e6/(up_mid_down['ES_MJperd']*365)

def refinery_config(x):
    try:
        '''Simplying Refinery Configuration'''
        if x.startswith('Deep'):
            return 'Deep Conversion'
        elif x.startswith('Medium'):
            return 'Medium Conversion'
        elif x.startswith('Hydro'):
            return 'Hydroskimming'
    except:
        return None
OCI_info['Default Refinery Configuration']=OCI_info['Default Refinery Configuration'].apply(lambda x: refinery_config(x))
OCI_info['Gas_at_Wellhead(t/d)']=up_mid_down['FS_Gas_at_Wellhead(t/d)']
OCI_info['Total Energy Produced (MJ/d)']=up_mid_down['ES_MJperd']


def upstream_gmj_kgboe_convert(x):
    return(up_mid_down[x]*up_mid_down['ES_MJperd']/up_mid_down['Total_BOE_Produced']/1000)

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
    OCI_info[i] = upstream_gmj_kgboe_convert(upstream_emission_category[i])

OCI_info['Upstream Carbon Intensity (kgCO2eq/boe)']=sum([OCI_info[i] for i in upstream_emission_category])

def midstream_scaler(x):
    '''scale midstream emission from kgCO2eq/bbl to kgCO2eq/boe'''
    return(up_mid_down[x]*up_mid_down['Oil production volume']/up_mid_down['Total_BOE_Produced'])

midstream_emission_category_CO2 ={
    'Midstream: Electricity (kgCO2eq/boe)':'Electricity',
    'Midstream: Heat (kgCO2eq/boe)':'Heat',
    'Midstream: Steam (kgCO2eq/boe)':'Steam',
    'Midstream: Hydrogen via SMR (kgCO2eq/boe)':'Hydrogen via SMR',
    'Midstream: Hydrogen via CNR (kgCO2eq/boe)':'Hydrogen via CNR',
    'Midstream: Other Emissions (kgCO2eq/boe)':'Other Emissions'
}



for i in midstream_emission_category_CO2:
    OCI_info[i] = midstream_scaler(midstream_emission_category_CO2[i])



OCI_info['Midstream Carbon Intensity (kgCO2eq/boe)']=sum([OCI_info[i] for i in midstream_emission_category_CO2])

# Downstream Transport to Consumer include refinery product transport and NGL transport
OCI_info['Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)'] =up_mid_down['Total_TransportEmissions_kgCO2e/BOE'] 

OCI_info['Downstream: Transport of LNG to Consumers (kgCO2eq/boe)'] = upstream_gmj_kgboe_convert('l-Total GHG emissions')
OCI_info['Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)'] = upstream_gmj_kgboe_convert('g-Total GHG emissions')
#OCI_info['Downstream: Transport to Consumers (kgCO2eq/boe)'] = OCI_info['Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)'] +\
#    OCI_info['Downstream: Transport of LNG to Consumers (kgCO2eq/boe)'] + OCI_info['Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)']

OCI_info['Downstream: Gasoline for Cars (kgCO2eq/boe)']=\
    up_mid_down['Gasoline_CombustionEmissions_kgCO2e/BOE']
OCI_info['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']=\
    up_mid_down['Jet_CombustionEmissions_kgCO2e/BOE']
OCI_info['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)'] = \
    up_mid_down['Diesel_CombustionEmissions_kgCO2e/BOE']
OCI_info['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] = \
    up_mid_down['FuelOil_CombustionEmissions_kgCO2e/BOE']
OCI_info['Downstream: Petroleum Coke for Power (kgCO2eq/boe)'] = \
    up_mid_down['RefineryCoke_CombustionEmissions_kgCO2e/BOE'] + up_mid_down['UpgraderCoke_CombustionEmissions_kgCO2e/BOE']
OCI_info['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)'] = \
    up_mid_down['ResidFuel_CombustionEmissions_kgCO2e/BOE']
OCI_info['Downstream: Natural Gas Liquids (kgCO2eq/boe)'] =\
    up_mid_down['UpstreamNGLProd_CombustionEmissions_kgCO2e/BOE'] 
OCI_info['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] = \
    up_mid_down['RefineryLPG_CombustionEmissions_kgCO2e/BOE']
OCI_info['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']=\
    up_mid_down['Ethane_ConversionEmissions_kgCO2e/BOE']

OCI_info['Downstream: Natural Gas (kgCO2eq/boe)'] = \
   up_mid_down['NatGas_CombustionEmissions_kgCO2e/BOE'].clip(0,None)
    
OCI_info['Downstream Carbon Intensity (kgCO2eq/boe)'] = (OCI_info['Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)']
    + OCI_info['Downstream: Transport of LNG to Consumers (kgCO2eq/boe)']
    + OCI_info['Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)']
    + OCI_info['Downstream: Gasoline for Cars (kgCO2eq/boe)'] 
    + OCI_info['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']
    + OCI_info['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)']
    + OCI_info['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] 
    + OCI_info['Downstream: Petroleum Coke for Power (kgCO2eq/boe)']
    + OCI_info['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)']
    + OCI_info['Downstream: Natural Gas Liquids (kgCO2eq/boe)']
    + OCI_info['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] 
    + OCI_info['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']
    + OCI_info['Downstream: Natural Gas (kgCO2eq/boe)']) 

OCI_info['Total Emission Carbon Intensity (kgCO2eq/boe)']=OCI_info['Upstream Carbon Intensity (kgCO2eq/boe)']+\
OCI_info['Midstream Carbon Intensity (kgCO2eq/boe)']+OCI_info['Downstream Carbon Intensity (kgCO2eq/boe)']

OCI_info['Industry GHG Responsibility (kgCO2eq/boe)']=(OCI_info['Upstream Carbon Intensity (kgCO2eq/boe)']
    + OCI_info['Midstream Carbon Intensity (kgCO2eq/boe)']
    + OCI_info['Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)']
    + OCI_info['Downstream: Transport of LNG to Consumers (kgCO2eq/boe)']
    + OCI_info['Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)']) 

OCI_info['Consumer GHG Responsibility (kgCO2eq/boe)'] = OCI_info['Total Emission Carbon Intensity (kgCO2eq/boe)'] \
    - OCI_info['Industry GHG Responsibility (kgCO2eq/boe)']

# Post-aggregation field property value assignment
## Flare rate is defined in the following categorical way
def flare_rate_category(x):
    flare_rate= x['Flaring-to-Oil Ratio (scf flared/bbl)']/(1+x['Gas-to-Oil Ratio (scf/bbl)']/5800)
    if flare_rate<=1:
        return 'Very Low Flare'
    elif flare_rate>1 and flare_rate<=5:
        return 'Low Flare'
    elif flare_rate>5 and flare_rate<=50:
        return 'Medium Flare'
    elif flare_rate>50 and flare_rate<=500:
        return 'High Flare'
    elif flare_rate>500 and flare_rate<=1000:
        return 'Very High Flare'
    elif flare_rate>1000:
        return 'Ultra High Flare'
    else:
        return 'No Flare Info'


OCI_info['Flare Rate']=OCI_info.apply(lambda x: flare_rate_category(x),axis =1)

OPEC_list = ['Equatorial Guinea','Gabon','Republic of the Congo','Iran','Iraq','Kuwait','Saudi Arabia',
            'United Arab Emirates','Algeria','Libya','Venezuela','Angola','Nigeria','Russian Federation',
            'Azerbaijan','Bahrain','Kazakhstan','Malaysia','Mexico','Oman']

OCI_info['OPEC'] = OCI_info['Country'].apply(lambda x:'Y' if x in OPEC_list else 'N')

#OCI_info['Oil or Gas']=OCI_info['Gas-to-Oil Ratio (scf/bbl)'].apply(lambda x: 'Gas' if x>1000 else 'Oil')

# def resource_type(x):
#     if x['API Gravity']<15:
#         return 'Extra-Heavy Oil'
#     elif x['API Gravity']>=15 and x['API Gravity']<=24:
#         return 'Heavy Oil' 
#     elif x['API Gravity']>24 and x['API Gravity']<=34:
#         return 'Medium Oil'
#     elif x['API Gravity']>34 and x['API Gravity']<=45:
#         return 'Light Oil'
#     elif x['API Gravity']>45 and x['API Gravity']<50:
#         return 'Ultra-Light Oil'
#     elif x['API Gravity']>=50 and x['Gas-to-Oil Ratio (scf/bbl)']>=1000 and x['Gas-to-Oil Ratio (scf/bbl)']<5000:
#         return 'Condensate'
#     elif x['API Gravity']>=60 or (x['Gas-to-Oil Ratio (scf/bbl)']>=5000 and x['Gas-to-Oil Ratio (scf/bbl)']<=25000):
#         return 'Wet Gas'
#     elif x['Gas-to-Oil Ratio (scf/bbl)']>25000:
#         return 'Dry Gas'
#     elif x['Filed Name'] in ['Sulige', 'Powder River', 'Fairview', 'Roma', 'Condabri', 'Talinga', 'Orana', 'Uinta']:
#         return 'Coal-bed Gas'
#     # elif x['API Gravity']>45 and x['Gas composition CO2']>=10:
#     #     return 'Acid Gas' 
#     else: 
#         return 'Uncategorized'
#OCI_info['Resource Type']=OCI_info.apply(lambda x: resource_type(x),axis =1)
opgee_inputs =pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Inputs/public_run.csv')
OCI_info.drop(columns = ['API Gravity', 'Sulfur Content (wt%)', 'Gas composition H2S', 'Gas composition CO2'],inplace = True)
opgee_inputs = opgee_inputs[['Field location (Country)', 'Field name','API gravity', 'Sulfur wt percent', 'Gas composition H2S', 'Gas composition CO2','Resource type']]
opgee_inputs['Field name'] = opgee_inputs['Field name'].apply(lambda x: x.strip())
OCI_info = OCI_info.merge(opgee_inputs, left_on = ['Country','Field Name'],right_on = ['Field location (Country)', 'Field name'],how = 'left',indicator = True)
if OCI_info[OCI_info['_merge']!='both'].shape[0]==0:
    OCI_info.drop(columns = '_merge',inplace = True)
else:
    print(OCI_info[OCI_info['_merge']!='both'])
OCI_info['Oil or Gas'] = OCI_info['Resource type'].apply(lambda x: 'Oil' if x.lower() in ['Extra-Heavy Oil'.lower(),'Heavy Oil'.lower(),'Medium Oil'.lower(),'Light Oil'.lower(),'Ultra-Light Oil'.lower(), 'Condensate.lower()'] else 'Gas')
OCI_info['Gas composition H2S'] = pd.to_numeric(OCI_info['Gas composition H2S'],errors = 'coerce')
OCI_info['Gas composition CO2'] = pd.to_numeric(OCI_info['Gas composition CO2'],errors = 'coerce')
def sweet_sour(x):
    if x['Oil or Gas']=='Oil' and x['Sulfur wt percent']>=0.5:
        return 'Sour Oil'
    elif x['Oil or Gas']=='Oil' and x['Sulfur wt percent']<0.5:
        return 'Sweet Oil'
    elif x['Oil or Gas']=='Gas' and (x['Gas composition H2S']>=2 or x['Gas composition CO2']>=2 or x['Sulfur wt percent']>=0.5):
        return 'Sour Gas'
    else:
        return 'Sweet Gas'    
OCI_info['Sour or Sweet'] = OCI_info.apply(lambda x: sweet_sour(x),axis =1)
#coordinates = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Inputs/coordinates.csv')
# ## Fuzzy match to coordinates table field names
# from fuzzywuzzy import process

# def field_match(row,list_fields):
#     m = process.extractOne(row['Field Name'],list_fields)
#     row['matched_field']=m[0]
#     row['match_score']=m[1]
#     return row

# match_table = OCI_info[['Field Name','Country']].drop_duplicates().apply(lambda x: field_match(x, coordinates['Field Name'].to_list()),axis =1 )
# match_table.to_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Inputs/field_match.csv',index = False)
coordinates = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Inputs/coordinates.csv')
match_table = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Inputs/field_match.csv')
select_coordinates = match_table.merge(coordinates,left_on = ['matched_field'],right_on = ['Field Name'], how = 'left',indicator = True)
select_coordinates[select_coordinates['_merge']!='both']
select_coordinates = select_coordinates[['Field Name_x','Country_y','Region','Latitude','Longitude']]
select_coordinates.rename(columns = {'Field Name_x':'Field Name'}, inplace = True)
OCI_info = OCI_info.merge(select_coordinates, how = 'left',indicator = True)
def region_mod(x):
    if x in(['Africa','South America','North America','Oceania','Asia']):
        return x+'n'
    elif x in(['Middle East','Caribbean','Former Soviet Union']):
        return x
    else:
        return x

OCI_info['Region_m']=OCI_info['Region'].apply(region_mod)

OCI_info['descriptor']= (
        'The '
        + OCI_info['Field Name']
        +' field is located in '
        + OCI_info['Country']
        + '. This '
        + OCI_info['Region_m']
        + ' region asset is classified as '
        + OCI_info['Resource type'].apply(lambda x: x.lower())
        + '. All produced liquids are processed in a '
        + OCI_info['Default Refinery Configuration'].apply(lambda x: x.lower())
        + ' refinery assuming the following oil assay: '
        + OCI_info['Assay Name']
        + '. Following are the detailed resource characteristics modeled in the OCI+.')

OCI_info.drop(columns = 'Region_m',inplace = True)

# Rounding columns
OCI_info = OCI_info.round({
    '2020 Total Oil and Gas Production Volume (boe)':0,
    'Max Depth(ft)':0,
    'Heating Value Processed Oil and Gas (MJ/d)':0,
    'Years in Production':0,
    'Number of Producing Wells':0,
    '2020 Crude Production Volume (bbl)':0,
    'API Gravity':0,
    'Sulfur Content Weight Percent':2,
    'Water-to-oil Ratio (bbl water/bbl oil)':1,
    'Gas-to-Oil Ratio (scf/bbl)':1,
    'Flaring-to-Oil Ratio (scf flared/bbl)':1,
    'Upstream Methane Intensity (kgCH4/boe)':3,
    'Midstream Methane Intensity (kgCH4/boe)':3,
    'Downstream Methane Intensity (kgCH4/boe)':3,
    'Total Methane Intensity (kgCH4/boe)':3,
    'Upstream Methane Emission Rate (NGSI Standard %)':3,
    'Upstream Methane Emission Rate (gCH4/Total MJ Produced)':3,
    'Gas composition H2S':1,
    'Gas composition CO2':1,
    'Gas composition C1':1,
    'Total Emission Carbon Intensity (kgCO2eq/boe)': 0,
    'Upstream Carbon Intensity (kgCO2eq/boe)':0,
    'Midstream Carbon Intensity (kgCO2eq/boe)':0,
    'Downstream Carbon Intensity (kgCO2eq/boe)':0})
OCI_info.to_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Postprocessed_Outputs_2/info_fix.csv',index = False)

info_base_cols = ['Country', 'Field Name', 'Assay Name', '2020 Total Oil and Gas Production Volume (boe)', 'Location', 'Max Depth(ft)', 'Gas shipped as LNG', 'Enhanced recovery', 'Fracked', 'Default Refinery Configuration',
'Heating Value Processed Oil and Gas (MJ/d)', 'Years in Production', 'Number of Producing Wells', '2020 Crude Production Volume (bbl)', 
'Region', 'Latitude', 'Longitude', 'API Gravity', 'Sulfur Content Weight Percent', 'Water-to-oil Ratio (bbl water/bbl oil)', 
'Gas-to-Oil Ratio (scf/bbl)', 'Flaring-to-Oil Ratio (scf flared/bbl)', 'Upstream Methane Intensity (kgCH4/boe)', 
'Midstream Methane Intensity (kgCH4/boe)', 'Downstream Methane Intensity (kgCH4/boe)', 'Total Methane Intensity (kgCH4/boe)',
'Upstream Methane Emission Rate (NGSI Standard %)', 'Upstream Methane Emission Rate (gCH4/Total MJ Produced)', 'Gas composition H2S',
'Gas composition CO2', 'Gas composition C1', 'Flare Rate', 'OPEC', 'Oil or Gas', 'Resource Type', 'Sour or Sweet', 'descriptor']
OCI_info.drop(columns = '_merge', inplace = True)
coordinates = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Inputs/coordinates.csv')
match_table = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Inputs/field_match.csv')
select_coordinates = match_table.merge(coordinates,left_on = ['matched_field'],right_on = ['Field Name'], how = 'left',indicator = True)
select_coordinates[select_coordinates['_merge']!='both']
select_coordinates = select_coordinates[['Field Name_x','Country_y','Region','Latitude','Longitude']]
select_coordinates.rename(columns = {'Field Name_x':'Field Name'}, inplace = True)
OCI_info = OCI_info.merge(select_coordinates, how = 'left',indicator = True)
def region_mod(x):
    if x in['Asia','Oceania']:
        return 'Asia-Pacific'
    elif x in(['Africa','South America','North America']):
        return x+'n'
    elif x in(['Middle East','Caribbean','Former Soviet Union']):
        return x
    else:
        return x

OCI_info['Region_m']=OCI_info['Region'].apply(region_mod)

OCI_info['descriptor']= (
        'The '
        + OCI_info['Field Name']
        +' field is located in '
        + OCI_info['Country']
        + '. This '
        + OCI_info['Region_m']
        + ' region asset is classified as '
        + OCI_info['Resource type'].apply(lambda x: x.lower())
        + '. All produced liquids are processed in a '
        + OCI_info['Default Refinery Configuration'].apply(lambda x: x.lower())
        + ' refinery assuming the following oil assay: '
        + OCI_info['Assay Name']
        + '. Following are the detailed resource characteristics modeled in the OCI+.')

OCI_info.drop(columns = 'Region_m',inplace = True)

OCI_info.rename(columns ={'Max Depth (ft)':'Max Depth(ft)','API gravity': 'API Gravity',
'Sulfur wt percent': 'Sulfur Content Weight Percent', 'Resource type': 'Resource Type'},inplace = True)
infobase = OCI_info[info_base_cols][OCI_info['gwp']==100]
info_100_cols=['Country', 'Field Name', 'Upstream: Exploration (kgCO2eq/boe)', 'Upstream: Drilling & Development (kgCO2eq/boe)', 
'Upstream: Crude Production & Extraction (kgCO2eq/boe)', 'Upstream: Surface Processing (kgCO2eq/boe)', 
'Upstream: Maintenance (kgCO2eq/boe)', 'Upstream: Waste Disposal (kgCO2eq/boe)', 'Upstream: Crude Transport (kgCO2eq/boe)', 
'Upstream: Other Small Sources (kgCO2eq/boe)', 'Upstream: Offsite emissions credit/debit (kgCO2eq/boe)', 
'Upstream: Carbon Dioxide Sequestration (kgCO2eq/boe)', 'Upstream Carbon Intensity (kgCO2eq/boe)', 
'Midstream: Electricity (kgCO2eq/boe)', 'Midstream: Heat (kgCO2eq/boe)', 'Midstream: Steam (kgCO2eq/boe)', 
'Midstream: Hydrogen via SMR (kgCO2eq/boe)', 'Midstream: Hydrogen via CNR (kgCO2eq/boe)', 
'Midstream: Other Emissions (kgCO2eq/boe)', 'Midstream Carbon Intensity (kgCO2eq/boe)', 
'Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)', 
'Downstream: Transport of LNG to Consumers (kgCO2eq/boe)', 
'Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)', 
'Downstream: Gasoline for Cars (kgCO2eq/boe)', 'Downstream: Jet Fuel for Planes (kgCO2eq/boe)', 
'Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)', 'Downstream: Fuel Oil for Boilers (kgCO2eq/boe)', 
'Downstream: Petroleum Coke for Power (kgCO2eq/boe)', 'Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)', 
'Downstream: Natural Gas Liquids (kgCO2eq/boe)', 'Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)', 
'Downstream: Petrochemical Feedstocks (kgCO2eq/boe)', 'Downstream: Natural Gas (kgCO2eq/boe)', 
'Downstream Carbon Intensity (kgCO2eq/boe)', 'Total Emission Carbon Intensity (kgCO2eq/boe)', 
'Industry GHG Responsibility (kgCO2eq/boe)', 'Consumer GHG Responsibility (kgCO2eq/boe)']
info100 = OCI_info[OCI_info['gwp']==100][info_100_cols]
info20 = OCI_info[OCI_info['gwp']==20][info_100_cols]
infobase['Fracked'] = infobase['Fracked'].apply(lambda x: bool(x))
infobase['Gas shipped as LNG'] = infobase['Gas shipped as LNG'].apply(lambda x: bool(x))
infobase.to_csv('/Users/rwang/Documents/oci/basedata/infobase.csv')
info20.to_csv('/Users/rwang/Documents/oci/basedata/info20.csv')
info100.to_csv('/Users/rwang/Documents/oci/basedata/info100.csv')