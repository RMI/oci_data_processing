import pandas as pd
import numpy as np
sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'

up_mid_down = pd.read_excel(sp_dir+'/Downstream/Analytics/up_mid_down_new_100yr.xlsx')


OCI_infobase=pd.DataFrame()

OCI_infobase['Field Name']=up_mid_down['Field_name']

OCI_infobase['Country']=up_mid_down['Field location (Country)']

OCI_infobase['Assay Name']=up_mid_down['matched_assay']

OCI_infobase['API Gravity']=up_mid_down['API gravity']

OCI_infobase['Sulfur Content (wt%)']=up_mid_down['sulfur']
OCI_infobase['Gas composition H2S']=up_mid_down['Gas composition H2S']
OCI_infobase['Gas composition CO2'] = up_mid_down['Gas composition CO2']
OCI_infobase['Gas composition C1']=up_mid_down['Gas composition C1']
OCI_infobase['2020 Total Oil and Gas Production Volume (boe)'] = up_mid_down['Total BOE Produced']*365

OCI_infobase['Location']=np.where(up_mid_down['Offshore?']==0,'Onshore', 'Offshore')

OCI_infobase['Depth (ft)']=up_mid_down['Field depth']

OCI_infobase['Water-to-oil Ratio (bbl water/bbl oil)']=up_mid_down['Water-to-oil ratio (WOR)']

OCI_infobase['Gas shipped as LNG']=np.where(up_mid_down['lng?']==True,1,0)

# If any of the following operation is 1 in OPGEE input, it's considered as Enhanced Oil Recovery
OCI_infobase['Enhanced recovery']=up_mid_down[['Natural gas reinjection','Water flooding','Gas lifting',
                                               'Gas flooding','Steam flooding']].any(axis='columns')

OCI_infobase['Fracked'] = up_mid_down['frack?']

OCI_infobase['Default Refinery Configuration']=up_mid_down['Default Refinery']
OCI_infobase['Heating Value Processed Oil and Gas (MJ/d)']=up_mid_down['ES_MJperd']

OCI_infobase['Years in Production']=up_mid_down['Field age']

OCI_infobase['Number of Producing Wells']=up_mid_down['Number of producing wells']

OCI_infobase['Gas-to-Oil Ratio (scf/bbl)']=up_mid_down['Gas-to-oil ratio (GOR)']

OCI_infobase['Flaring-to-Oil Ratio (scf flared/bbl)'] = up_mid_down['Flaring-to-oil ratio']

OCI_infobase['Steam-to-Oil Ratio (bbl steam/bbl oil)'] = up_mid_down['Steam-to-oil ratio (SOR)']

#OCI_infobase['Upstream Methane Fugitives + Venting (tonnes CH4/d)']=up_mid_down['venting_ch4_uponly(t/d)']+up_mid_down['fugitive_ch4_uponly(t/d)']
#OCI_infobase['Upstream CO2 Fugitives + Venting (tonnes CO2/d)']=up_mid_down['venting_co2(t/d)']+up_mid_down['fugitive_co2(t/d)']

OCI_infobase['2020 Crude Production Volume (bbl)']= up_mid_down['Oil production volume']*365

OCI_infobase['2020 Total Oil and Gas Production Volume (boe)']= up_mid_down['Total BOE Produced']*365

# Upstream methane accounts for all of flaring, part of fugitives/venting (Production, Gathering and Boosting, Secondary production)
OCI_infobase['Upstream Methane Intensity (kgCH4/boe)']=(up_mid_down['venting_ch4_uponly(t/d)']+up_mid_down['fugitive_ch4_uponly(t/d)']+up_mid_down['flaring_ch4(t/d)'])*365\
/(OCI_infobase['2020 Total Oil and Gas Production Volume (boe)'])*1000

# Midstream Methane Intensity calculation, use 20yr total emission from midstream runs and get fraction of CO2eq and convert back to methane 
OCI_infobase['Midstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total refinery processes']\
    *OCI_infobase['2020 Crude Production Volume (bbl)']/OCI_infobase['2020 Crude Production Volume (bbl)']\
    *up_mid_down['emission_frac_CH4']/30

# Downstream mehtane accounts for all OPEM methane output + upstream natural gas distribution /LNG
OCI_infobase['Downstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total Transport CH4 Emissions Intensity (kg CH4. / BOE)']+\
up_mid_down['Total Transport CH4 Emissions Intensity (kg CH4. / BOE).1']+up_mid_down['Total Combustion CH4 Emissions Intensity (kg CH4. / BOE)']+\
up_mid_down['Total Combustion CH4 Emissions Intensity (kg CH4. / BOE)']+\
up_mid_down['Total ProcessCH4  Emissions Intensity (kg CH4/boe total)']+ up_mid_down['tCH4/year']/\
(OCI_infobase['2020 Total Oil and Gas Production Volume (boe)'])*1000-OCI_infobase['Upstream Methane Intensity (kgCH4/boe)']


OCI_infobase['Total Methane Intensity (kgCH4/boe)'] = OCI_infobase['Upstream Methane Intensity (kgCH4/boe)']+\
OCI_infobase['Midstream Methane Intensity (kgCH4/boe)'] +OCI_infobase['Downstream Methane Intensity (kgCH4/boe)']

OCI_infobase['Upstream Methane Emission Rate (NGSI Standard %)'] = up_mid_down['tCH4/year-miQ']*up_mid_down['ES_Gas_output(MJ/d)']/up_mid_down['ES_MJperd']\
        /(up_mid_down['FS_Gas_at_Wellhead(t/d)']*365*up_mid_down['Gas composition C1']/100)

#If gas output is negative in OPGEE, zero it out to avoid a negative methane emission rate
OCI_infobase['Upstream Methane Emission Rate (NGSI Standard %)'] = np.where(OCI_infobase['Upstream Methane Emission Rate (NGSI Standard %)']<0,0,OCI_infobase['Upstream Methane Emission Rate (NGSI Standard %)']*100)

OCI_infobase['Upstream Methane Emission Rate (gCH4/Total MJ Produced)'] = up_mid_down['tCH4/year']*1e6/(up_mid_down['ES_MJperd']*365)

def refinery_config(x):
    '''Simplying Refinery Configuration'''
    if x.startswith('Deep'):
        return 'Deep Conversion'
    elif x.startswith('Medium'):
        return 'Medium Conversion'
    elif x.startswith('Hydro'):
        return 'Hydroskimming'

OCI_infobase['Default Refinery Configuration']=OCI_infobase['Default Refinery Configuration'].apply(lambda x: refinery_config(x))

OCI_infobase['Gas_at_Wellhead(t/d)']=up_mid_down['FS_Gas_at_Wellhead(t/d)']

OCI_infobase['Total Energy Produced (MJ/d)']=up_mid_down['ES_MJperd']

## Load the Excel file with manual coordinates inputs

coordinates = pd.read_excel(sp_dir+'/Upstream/oci_field_cordinates.xlsx')
coordinates.drop_duplicates(subset = ['Country', 'Field Name'], inplace = True)
OCI_infobase = pd.merge(OCI_infobase,coordinates,left_on=['Country', 'Field Name'],right_on=['Country', 'Field Name'],how = 'left')

OCI_info100=pd.DataFrame()
OCI_info100['Field Name']=up_mid_down['Field_name']
OCI_info100['Country'] = up_mid_down['Field location (Country)']

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
    OCI_info100[i] = upstream_gmj_kgboe_convert(upstream_emission_category[i])

OCI_info100['Upstream Carbon Intensity (kgCO2eq/boe)']=sum([OCI_info100[i] for i in upstream_emission_category])

def midstream_scaler(x):
    return(up_mid_down[x]*OCI_infobase['2020 Crude Production Volume (bbl)']/OCI_infobase['2020 Total Oil and Gas Production Volume (boe)'])

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

OCI_info100['Total Emission Carbon Intensity (kgCO2eq/boe)']=OCI_info100['Upstream Carbon Intensity (kgCO2eq/boe)']+\
OCI_info100['Midstream Carbon Intensity (kgCO2eq/boe)']+OCI_info100['Downstream Carbon Intensity (kgCO2eq/boe)']

OCI_info100['Industry GHG Responsibility (kgCO2eq/boe)']=OCI_info100['Upstream Carbon Intensity (kgCO2eq/boe)']+\
    OCI_info100['Midstream Carbon Intensity (kgCO2eq/boe)']+OCI_info100['Downstream: Transport to Consumers (kgCO2eq/boe)'] 

OCI_info100['Consumer GHG Responsibility (kgCO2eq/boe)'] = OCI_info100['Total Emission Carbon Intensity (kgCO2eq/boe)'] \
    - OCI_info100['Industry GHG Responsibility (kgCO2eq/boe)']

OCI_info20 = pd.DataFrame()
OCI_info20['Field Name']=up_mid_down['Field_name']
OCI_info20['Country'] = up_mid_down['Field location (Country)']

OCI_info20['Upstream Carbon Intensity (kgCO2eq/boe)'] = (OCI_info100['Upstream Carbon Intensity (kgCO2eq/boe)'] + OCI_infobase['Upstream Methane Intensity (kgCH4/boe)']*55)

OCI_info20['Midstream Carbon Intensity (kgCO2eq/boe)'] = (OCI_info100['Midstream Carbon Intensity (kgCO2eq/boe)'] + OCI_infobase['Midstream Methane Intensity (kgCH4/boe)']*55)

OCI_info20['Downstream Carbon Intensity (kgCO2eq/boe)'] = (OCI_info100['Downstream Carbon Intensity (kgCO2eq/boe)'] + OCI_infobase['Downstream Methane Intensity (kgCH4/boe)']*55)

# Start Aggregation 

