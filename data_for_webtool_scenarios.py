# %%
import pandas as pd
sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'
up_mid_down = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Postprocessed_outputs_2/downstream_postprocessed_scenarios_fix.csv')
up_mid_down = up_mid_down[up_mid_down['gwp']==20]
def prep_for_webtool(up_mid_down):
    scenario = pd.DataFrame()
    # select relevant columns from up_mid_down to scenario
    scenario['Field Name']=up_mid_down['Field_name']
    # scenario['Country']=up_mid_down['Field location (Country)']
    # scenario['Assay Name']=up_mid_down['Assay Name']
    # scenario['API Gravity']=up_mid_down['API gravity']
    # scenario['Sulfur Content (wt%)']=up_mid_down['sulfur']
    # scenario['Gas composition H2S']=up_mid_down['Gas composition H2S']
    # scenario['Gas composition CO2'] = up_mid_down['Gas composition CO2']
    # scenario['Gas composition C1']=up_mid_down['Gas composition C1']
    # scenario['2020 Total Oil and Gas Production Volume (boe)'] = up_mid_down['Total_BOE_Produced']*365
    # scenario['Location']=np.where(up_mid_down['Offshore?']==0,'Onshore', 'Offshore')
    # scenario['Max Depth (ft)']=up_mid_down['Field depth']
    # scenario['Water-to-oil Ratio (bbl water/bbl oil)']=up_mid_down['Water-to-oil ratio (WOR)']
    # scenario['Gas shipped as LNG']=np.where(up_mid_down['lng?']==True,1,0)
    # # If any of the following operation is 1 in OPGEE input, it's considered as Enhanced Oil Recovery
    # scenario['Enhanced recovery']=up_mid_down[['Natural gas reinjection','Water flooding','Gas lifting',
    #                                             'Gas flooding','Steam flooding']].any(axis='columns')
    # scenario['Fracked'] = up_mid_down['frack?']
    # scenario['gwp']=up_mid_down['gwp']
    # scenario['Default Refinery Configuration']=up_mid_down['Default Refinery']
    # scenario['Heating Value Processed Oil and Gas (MJ/d)']=up_mid_down['ES_MJperd']
    # scenario['Years in Production']=up_mid_down['Field age']
    # scenario['Number of Producing Wells']=up_mid_down['Number of producing wells']
    # scenario['Gas-to-Oil Ratio (scf/bbl)']=up_mid_down['Gas-to-oil ratio (GOR)']
    # scenario['Flaring-to-Oil Ratio (scf flared/bbl)'] = up_mid_down['Flaring-to-oil ratio']
    # scenario['Steam-to-Oil Ratio (bbl steam/bbl oil)'] = up_mid_down['Steam-to-oil ratio (SOR)']
    # scenario['2020 Crude Production Volume (bbl)']= up_mid_down['Oil production volume']*365
    # scenario['2020 Total Oil and Gas Production Volume (boe)']= up_mid_down['Total_BOE_Produced']*365
    # # Upstream methane accounts for all of flaring, part of fugitives/venting (Production, Gathering and Boosting, Secondary production)
    # scenario['Upstream Methane Intensity (kgCH4/boe)']=(up_mid_down['venting_ch4_uponly(t/d)']+up_mid_down['fugitive_ch4_uponly(t/d)']+up_mid_down['flaring_ch4(t/d)'])*365\
    # /(scenario['2020 Total Oil and Gas Production Volume (boe)'])*1000
    # # Midstream Methane Intensity calculation, use 100yr total emission from midstream runs and get fraction of CO2eq and convert back to methane 
    # scenario['Midstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total refinery processes']\
    #     *scenario['2020 Crude Production Volume (bbl)']/scenario['2020 Crude Production Volume (bbl)']\
    #     *up_mid_down['emission_frac_CH4']/30
    # # Downstream mehtane accounts for all OPEM methane output + upstream natural gas distribution /LNG
    # scenario['Downstream Methane Intensity (kgCH4/boe)']=up_mid_down['Total_CH4_Emissions_kgCH4/BOE']+\
    #     up_mid_down['tCH4/year']/scenario['2020 Total Oil and Gas Production Volume (boe)']*1000-scenario['Upstream Methane Intensity (kgCH4/boe)']
    # scenario['Total Methane Intensity (kgCH4/boe)'] = scenario['Upstream Methane Intensity (kgCH4/boe)']+\
    # scenario['Midstream Methane Intensity (kgCH4/boe)'] +scenario['Downstream Methane Intensity (kgCH4/boe)']
    # scenario['Upstream Methane Emission Rate (NGSI Standard %)'] = up_mid_down['tCH4/year-miQ']*up_mid_down['ES_Gas_output(MJ/d)']/up_mid_down['ES_MJperd']\
    #         /(up_mid_down['FS_Gas_at_Wellhead(t/d)']*365*up_mid_down['Gas composition C1']/100)
    # #If gas output is negative in OPGEE, zero it out to avoid a negative methane emission rate
    # scenario['Upstream Methane Emission Rate (NGSI Standard %)'] = np.where(scenario['Upstream Methane Emission Rate (NGSI Standard %)']<0,0,scenario['Upstream Methane Emission Rate (NGSI Standard %)']*100)
    # scenario['Upstream Methane Emission Rate (gCH4/Total MJ Produced)'] = up_mid_down['tCH4/year']*1e6/(up_mid_down['ES_MJperd']*365)

    # def refinery_config(x):
    #     try:
    #         '''Simplying Refinery Configuration'''
    #         if x.startswith('Deep'):
    #             return 'Deep Conversion'
    #         elif x.startswith('Medium'):
    #             return 'Medium Conversion'
    #         elif x.startswith('Hydro'):
    #             return 'Hydroskimming'
    #     except:
    #         return None
    # scenario['Default Refinery Configuration']=scenario['Default Refinery Configuration'].apply(lambda x: refinery_config(x))
    # scenario['Gas_at_Wellhead(t/d)']=up_mid_down['FS_Gas_at_Wellhead(t/d)']
    # scenario['Total Energy Produced (MJ/d)']=up_mid_down['ES_MJperd']


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
        scenario[i] = upstream_gmj_kgboe_convert(upstream_emission_category[i])

    scenario['Upstream Carbon Intensity (kgCO2eq/boe)']=sum([scenario[i] for i in upstream_emission_category])

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
        scenario[i] = midstream_scaler(midstream_emission_category_CO2[i])



    scenario['Midstream Carbon Intensity (kgCO2eq/boe)']=sum([scenario[i] for i in midstream_emission_category_CO2])

    # Downstream Transport to Consumer include refinery product transport and NGL transport
    scenario['Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)'] =up_mid_down['Total_TransportEmissions_kgCO2e/BOE'] 

    scenario['Downstream: Transport of LNG to Consumers (kgCO2eq/boe)'] = upstream_gmj_kgboe_convert('l-Total GHG emissions')
    scenario['Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)'] = upstream_gmj_kgboe_convert('g-Total GHG emissions')
    #scenario['Downstream: Transport to Consumers (kgCO2eq/boe)'] = scenario['Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)'] +\
    #    scenario['Downstream: Transport of LNG to Consumers (kgCO2eq/boe)'] + scenario['Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)']

    scenario['Downstream: Gasoline for Cars (kgCO2eq/boe)']=        up_mid_down['Gasoline_CombustionEmissions_kgCO2e/BOE']
    scenario['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']=        up_mid_down['Jet_CombustionEmissions_kgCO2e/BOE']
    scenario['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)'] =         up_mid_down['Diesel_CombustionEmissions_kgCO2e/BOE']
    scenario['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] =         up_mid_down['FuelOil_CombustionEmissions_kgCO2e/BOE']
    scenario['Downstream: Petroleum Coke for Power (kgCO2eq/boe)'] =         up_mid_down['RefineryCoke_CombustionEmissions_kgCO2e/BOE'] + up_mid_down['UpgraderCoke_CombustionEmissions_kgCO2e/BOE']
    scenario['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)'] =         up_mid_down['ResidFuel_CombustionEmissions_kgCO2e/BOE']
    scenario['Downstream: Natural Gas Liquids (kgCO2eq/boe)'] =        up_mid_down['UpstreamNGLProd_CombustionEmissions_kgCO2e/BOE'] 
    scenario['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] =         up_mid_down['RefineryLPG_CombustionEmissions_kgCO2e/BOE']
    scenario['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']=        up_mid_down['Ethane_ConversionEmissions_kgCO2e/BOE']

    scenario['Downstream: Natural Gas (kgCO2eq/boe)'] =         up_mid_down['NatGas_CombustionEmissions_kgCO2e/BOE']
        
    scenario['Downstream Carbon Intensity (kgCO2eq/boe)'] = (scenario['Downstream: Transport of Petroleum Products to Consumers (kgCO2eq/boe)']
        + scenario['Downstream: Transport of LNG to Consumers (kgCO2eq/boe)']
        + scenario['Downstream: Transport of Pipeline Gas to Consumers (kgCO2eq/boe)']
        + scenario['Downstream: Gasoline for Cars (kgCO2eq/boe)'] 
        + scenario['Downstream: Jet Fuel for Planes (kgCO2eq/boe)']
        + scenario['Downstream: Diesel for Trucks and Engines (kgCO2eq/boe)']
        + scenario['Downstream: Fuel Oil for Boilers (kgCO2eq/boe)'] 
        + scenario['Downstream: Petroleum Coke for Power (kgCO2eq/boe)']
        + scenario['Downstream: Liquid Heavy Ends for Ships (kgCO2eq/boe)']
        + scenario['Downstream: Natural Gas Liquids (kgCO2eq/boe)']
        + scenario['Downstream: Liquefied Petroleum Gases (kgCO2eq/boe)'] 
        + scenario['Downstream: Petrochemical Feedstocks (kgCO2eq/boe)']
        + scenario['Downstream: Natural Gas (kgCO2eq/boe)']) 
    scenario['Total BOED'] = up_mid_down['Total_BOE_Produced']
    try:
        scenario['flaring_ghg(t/d)']=up_mid_down['flaring_ghg(t/d)']
    except:
        print('no flaring_ghg column')   
    return scenario
scenario = prep_for_webtool(up_mid_down)
scenario['slider']=up_mid_down['Scenario']
scenario['value']=up_mid_down['Scenario_value']
scenario['Default?']=up_mid_down['Default?']

# Edit results for midstream scenarios
# Load baserun results
up_mid_down_base = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Postprocessed_outputs_2/downstream_postprocessed_fix.csv')
# Add renewable hydrogen scenario by subtracting midstream SMR related emission
scenario_rh_def = prep_for_webtool(up_mid_down_base[(up_mid_down_base['gwp']==20) 
    & up_mid_down_base['Field_name'].isin(['Kern River','Frade', 'Duri'])])
scenario_rh_def['slider']='Renewable Hydrogen'
scenario_rh_def['value']='Off'
scenario_rh_def['Default?']='Y'
scenario_rh_on =scenario_rh_def.copy(deep=True)
scenario_rh_on['Midstream Carbon Intensity (kgCO2eq/boe)']=scenario_rh_on['Midstream Carbon Intensity (kgCO2eq/boe)']-scenario_rh_on['Midstream: Hydrogen via SMR (kgCO2eq/boe)']
scenario_rh_on['value']='On'
scenario_rh_on['Default?']='N'

# Add eletricity source scenario by subtracting midstream electricity related emission
scenario_es_def = prep_for_webtool(up_mid_down_base[(up_mid_down_base['gwp']==20) 
    & up_mid_down_base['Field_name'].isin(['Greater Burgan', 'Sacha', 'Shengli', 'Cold Lake'])])
scenario_es_def['slider']='Electricity Source'
scenario_es_def['value']='Fossil fuel - fired power'
scenario_es_def['Default?']='Y'
scenario_es_on =scenario_es_def.copy(deep=True)
scenario_es_on['Midstream Carbon Intensity (kgCO2eq/boe)']=scenario_es_on['Midstream Carbon Intensity (kgCO2eq/boe)']-scenario_es_on['Midstream: Electricity (kgCO2eq/boe)']
scenario_es_on['value']='Low carbon power'
scenario_es_on['Default?']='N'
# Add Flare Volume Scenario

scenario_fv_def = prep_for_webtool(up_mid_down_base[(up_mid_down_base['gwp']==20) 
    & up_mid_down_base['Field_name'].isin(['South Caspian Basin','Sacha', 'Waha'])])
scenario_fv_def['slider']='Flare Volume'
scenario_fv_def['value']='1'
scenario_fv_def['Default?']='Y'
scenario_fv_50 = scenario_fv_def.copy(deep=True)
scenario_fv_50['value']='0.5'
scenario_fv_50['Upstream Carbon Intensity (kgCO2eq/boe)']=scenario_fv_50['Upstream Carbon Intensity (kgCO2eq/boe)']-0.5*scenario_fv_50['flaring_ghg(t/d)']*1000/scenario_fv_50['Total BOED']
scenario_fv_50['Default?']='N'
scenario_fv_150 = scenario_fv_def.copy(deep=True)
scenario_fv_150['value']='1.5'
scenario_fv_150['Upstream Carbon Intensity (kgCO2eq/boe)']=scenario_fv_150['Upstream Carbon Intensity (kgCO2eq/boe)']+0.5*scenario_fv_150['flaring_ghg(t/d)']*1000/scenario_fv_150['Total BOED']
scenario_fv_150['Default?']='N'


# Add Decommisioning Scenario
scenario_decom_def = prep_for_webtool(up_mid_down_base[(up_mid_down_base['gwp']==20) 
    & up_mid_down_base['Field_name'].isin(['Brent','Wilmington', 'Minas'])])
scenario_decom_def['slider']='Decommission Depleted Assets'
scenario_decom_def['value'] = 'no'
scenario_decom_def['Default?']='Y'
scenario_decom_on = scenario_decom_def.copy(deep = True)
# Set the upstream emission of the decommision scenario to be Ghawar's upstream emission value
scenario_decom_on['Upstream Carbon Intensity (kgCO2eq/boe)']= 68
scenario_decom_on['value']='yes'
scenario_decom_on['Default?']='N'


# Add GWP Scenario

scenario_gwp_def = prep_for_webtool(up_mid_down_base[(up_mid_down_base['gwp']==20) 
    & up_mid_down_base['Field_name'].isin(['Greater Burgan', 'Sacha', 'Shengli', 'Cold Lake'])])
scenario_gwp_def['slider']='Global Warming Potential'
scenario_gwp_def['value'] = '20'
scenario_gwp_def['Default?']='Y'

scenario_gwp_100 = prep_for_webtool(up_mid_down_base[(up_mid_down_base['gwp']==100) 
    & up_mid_down_base['Field_name'].isin(['Greater Burgan', 'Sacha', 'Shengli', 'Cold Lake'])])
scenario_gwp_100['slider']='Global Warming Potential'
scenario_gwp_100['value'] = '100'
scenario_gwp_100['Default?']='N'

scenario = pd.concat([scenario, scenario_rh_on, scenario_rh_def, scenario_es_def, 
    scenario_es_on,scenario_fv_50,scenario_fv_def,scenario_fv_150, scenario_decom_def, scenario_decom_on,
    scenario_gwp_def, scenario_gwp_100])
scenario = scenario[['Upstream Carbon Intensity (kgCO2eq/boe)','Midstream Carbon Intensity (kgCO2eq/boe)','Downstream Carbon Intensity (kgCO2eq/boe)','slider','value','Default?','Field Name']]
def toggle_stage(x):
    if x in(['Liquefied Natural Gas', 'Petroleum Coke Combustion']):
        return 'Downstream'
    elif x in(['Carbon Capture and Storage', 'Renewable Electricity','Renewable Electricity',
            'Flare Efficiency','Venting + Fugitive Leakage', 'Solar Steam', 'Energy to Pump Water', 'Flare Volume', 'Decommission Depleted Assets']):
        return 'Upstream'
    elif x in(['Electricity Source', 'Renewable Hydrogen']):
        return 'Midstream'
    elif x =='Global Warming Potential':
        return 'Global Warming Potential'
scenario['toggle_stage']=scenario['slider'].apply(lambda x: toggle_stage(x))
scenario.to_excel(sp_dir +'/Upstream/upstream_data_pipeline_sp/Postprocessed_outputs_2/scenario.xlsx')
scenario[['Upstream Carbon Intensity (kgCO2eq/boe)', 'Midstream Carbon Intensity (kgCO2eq/boe)', 
    'Downstream Carbon Intensity (kgCO2eq/boe)']] = scenario[['Upstream Carbon Intensity (kgCO2eq/boe)', 'Midstream Carbon Intensity (kgCO2eq/boe)', 
    'Downstream Carbon Intensity (kgCO2eq/boe)']].astype(int)
def reshape(scenario,stage):
    df = scenario[['slider',	'value',	'Default?',	'Field Name', 'toggle_stage', stage+' Carbon Intensity (kgCO2eq/boe)']]
    df['stage']=stage.lower()
    df.rename(columns = {stage+' Carbon Intensity (kgCO2eq/boe)':'emission_value'},inplace = True)
    return df
scenario = pd.concat([reshape(scenario,'Upstream'), reshape(scenario,'Midstream'), reshape(scenario,'Downstream')])
scenario = scenario.pivot(index = ['slider','Default?','value','toggle_stage','stage'],columns = 'Field Name',
    values = 'emission_value').reset_index()

slider_value = pd.read_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Postprocessed_outputs_2/sliders_index.csv')
# Renaming toggle values to be consistent with blob description of scenarios

scenario = scenario.merge(slider_value, how = 'left', indicator = True)
if scenario[scenario['_merge']!='both'].shape[0]!=0:
    print(scenario[scenario['_merge']!='both'])
else:
    scenario.drop(columns = ['value','_merge'], inplace = True)
scenario.rename(columns = {'New_value':'value'},inplace = True)
# Reindexing to switch scenario toggle values so that the lowest emission scenarios are shown on the left and highest on the right

scenario = scenario.reindex([0,1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 
23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 
52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 84, 
85, 86, 81, 82, 83, 87, 88, 89, 93, 94, 95, 96, 97, 98, 90, 91, 92])
scenario
scenario.to_csv(sp_dir + '/Upstream/upstream_data_pipeline_sp/Postprocessed_outputs_2/sliders_fix.csv',index = False)
scenario.to_csv('/Users/rwang/Documents/oci/basedata/sliders_fix.csv',index = False)

