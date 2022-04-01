import pandas as pd
import sqlite3
import os
import numpy as np

opem_dir = '/Users/rwang/Documents/OCI+/Downstream/opem'

connection = sqlite3.connect("../OCI_Database.db")


def opem_input_prep(upstream,midstream):
    ''' Preparing OPEM inputs/product slates '''

    # Calculate Crude to Refinery in bbl/d from Energy Summary tab of OPGEE model. Formula is based on cell G6 in OPEM input tab  
    #https://rockmtnins.sharepoint.com/:x:/r/sites/CAE/_layouts/15/Doc.aspx?sourcedoc=%7B5E0994C9-8E35-440B-8BB4-31DF5167F60C%7D&file=OCI%20site%20input%20table%20sources.xlsx&action=default&mobileredirect=true&cid=544ad233-565f-438c-9ce2-d7d5b607b1da

    # ('Energy Summary'!E89*1e3/'Energy Summary'!M133)/(158.9873*141.5/(131.5+Results!G34))
    upstream['crude_to_refinery(bbl/d)']= (upstream['ES_Crude_output(mmbut/d)']*1e3/upstream['ES_Energy_Density_crude(mmbtu/t)'])/(158.9873*141.5/(131.5+upstream['API gravity']))

    # Calculate NGL_C2 export from the field, formula based on cell G8 in OPEM input tab
    # (Flowsheet!W17+Flowsheet!CP17)*1000/(5.61458350903291*20.98*2.2)

    upstream['NGL_C2(boed)'] = (upstream['FS_LPG_export_C2(t/d)']+upstream['FS_Ethane_to_Petchem(t/d)'])*1000/(5.61458350903291*20.98*2.2)

    # Calculate NGL_C3 export from the field, formula based on cell G9 in OPEM input tab
    # Flowsheet!W18*1000000/(42*1920)+Flowsheet!W9*(1000000)*0.75/(1923*42) assuming 75% of LPG exported is C3
    upstream['NGL_C3(boed)'] = upstream['FS_LPG_export_C3(t/d)']*1000000/(42*1920)+upstream['FS_LPG_export_LPG(t/d)']*(1000000)*0.75/(1923*42)

    # Calculate NGL_C4 export from the dfield, formula based on cell G10 in OPEM input tab
    # Flowsheet!W19*1000000/(42*2213)+Flowsheet!W9*(1000000)*0.25/(1923*42) assuming 25% of LPG exported is C4
    upstream['NGL_C4(boed)'] = upstream['FS_LPG_export_C4(t/d)']*1000000/(42*2213)+upstream['FS_LPG_export_LPG(t/d)']*(1000000)*0.25/(1923*42)

    # Calculate NGL_C5+ export from the field, formula based on cell G11 in OPEM input tab
    # Assuming all NGL minus C2, C3 and C4 is C5+
    # ('Energy Summary'!E87-(((Flowsheet!W17+Flowsheet!CP17)*'Energy Summary'!M141) + 
    # (Flowsheet!W18*'Energy Summary'!M142) + (Flowsheet!W19*'Energy Summary'!M143) + 
    #(Flowsheet!W9*84950/1923)))/(42*.110)
    upstream['NGL_C5(boed)'] = (upstream['ES_NGL_output(mmbtu/d)']-(((upstream['FS_LPG_export_C2(t/d)']+ \
                            upstream['FS_Ethane_to_Petchem(t/d)'])*upstream['ES_Energy_Density_C2(mmbtu/t)']) + \
                            (upstream['FS_LPG_export_C3(t/d)']*upstream['ES_Energy_Density_C3(mmbtu/t)']) + \
                            (upstream['FS_LPG_export_C4(t/d)']*upstream['ES_Energy_Density_C4(mmbtu/t)']) + \
                            (upstream['FS_LPG_export_LPG(t/d)']*84950/1923)))/(42*.110)

    # Calculate petcoke from the field, formula based on cell G12 in OPEM input tab
    # 1000*(Flowsheet!HG7-'Energy Summary'!E77/'Energy Summary'!M135)
    upstream['petcoke(kg/d)'] = 1000*(upstream['FS_Petcoke_to_stock(t/d)']-upstream['ES_Petcoke_fuel(mmbtu/d)']/upstream['ES_Energy_Density_petcoke(mmbtu/t)'])

    upstream_midstream = upstream.merge(midstream,right_on=
            ['opgee_field','opgee_country'],left_on = 
            ['Field_name', 'Field location (Country)'], how = 'left')

    #upstream_midstream[upstream_midstream['sulfur'].isna()][['Field name','year']]

    # Filter out obsolete and misinput upstream fields such as Bakken , Generic 
    upstream_midstream = upstream_midstream[upstream_midstream['sulfur'].notna()]

    upstream_midstream_for_opem = upstream_midstream

    upstream_midstream_for_opem['OPEM_field_name']=(upstream_midstream_for_opem['opgee_field']+';'
                                                    +upstream_midstream_for_opem['original_file']+';'
                                                    +upstream_midstream_for_opem['Scenario']+';'
                                                    +upstream_midstream_for_opem['toggle_value'])
                                                    


    print('Getting data for OPEM Product Slates...')

    upstream_midstream_for_opem['volume_flow_bbl']=''
    upstream_midstream_for_opem['Product Slate (bbl product per day)'] = ''
    upstream_midstream_for_opem['energy_flow_MJ']=''
    upstream_midstream_for_opem['mass_flow_kg']=''

    # LPG: Mass [kg]/((2.04 kg/m3 gas)*(0.159 m3/bbl gas)*(270 bbl gas/bbl liquid))
    # where .159 is a constant to convert volume units, and mass is found on [Sheet1 row 39] of the prelim output
    # Petrochem feedstock: Mass [kg]/((1.264 kg/m3 gas)*(0.159 m3/bbl gas)*(270 bbl gas/bbl liquid))
    # where .159 is a constant to convert volume units, and mass is found on [Sheet1 row 40] of the prelim output
    upstream_midstream_for_opem['Liquefied Petroleum Gases (LPG)_bbl']= upstream_midstream_for_opem['Liquified Petroleum Gas (LPG).1']/(2.04*0.159*270)
    upstream_midstream_for_opem['Petrochemical Feedstocks_bbl']=upstream_midstream_for_opem['Petrochemical Feedstocks.1']/(1.264*0.159*270)
    upstream_midstream_for_opem['Asphalt_bbl']= 0

    upstream_midstream_for_opem['emission(kgCO2eq/bbl)']='kgCO2eq/bbl'
    upstream_midstream_for_opem['Per energy content of refinery product (gCO2eq/MJ)'] = 'Emission by Product (gCO2eq/MJ)'
    upstream_midstream_for_opem['Allocation to gasoline (g CO2eq/MJ)']='Process Emission (gCO2eq/MJ)'

    opem_product_slate =  upstream_midstream_for_opem[['OPEM_field_name',                                                   
                                                    'Product Slate (bbl product per day)',
                                                    'volume_flow_bbl','throughput','Gasoline',
    'Jet Fuel',
    'Diesel',
    'Fuel Oil',
    'Petroleum Coke',
    'Residual Fuels',
    'Refinery Fuel Gas (RFG)',
    'Liquefied Petroleum Gases (LPG)_bbl',
    'Petrochemical Feedstocks_bbl',
    'Asphalt_bbl',                                                   
    'energy_flow_MJ',
    'Blended Gasoline',
    'Jet-A/AVTUR',
    'ULSD',
    'Fuel Oil.1',
    'Coke',
    'Liquid Heavy Ends',
    'RFG',
    'Surplus NCR H2',
    'Liquified Petroleum Gas (LPG)',
    'Petrochemical Feedstocks',
    'Asphalt',
    'Gasoline S wt%',
    'Gasoline H2 wt%','mass_flow_kg',
    'Blended Gasoline.1',
    'Jet-A/AVTUR.1',
    'ULSD.1',
    'Fuel Oil.2',
    'Coke.1',
    'Liquid Heavy Ends.1',
    'Sulphur',
    'RFG.1',
    'Surplus NCR H2.1',
    'Liquified Petroleum Gas (LPG).1',
    'Petrochemical Feedstocks.1',
    'Asphalt.1']]

    opem_product_slate = opem_product_slate.T

    opem_product_slate.columns = opem_product_slate.iloc[0]
    opem_product_slate = opem_product_slate.iloc[1:,:]

    slate_index = pd.read_csv(opem_dir + '/src/opem/products/product_slates/all_product_slates.csv')

    opem_product_slate.index = slate_index.iloc[:,0]

    #opem_product_slate.to_excel('../Deep Dive page/Analytics/all_product_slates.xlsx')
    opem_product_slate.to_csv(opem_dir + '/src/opem/products/product_slates/all_product_slates.csv')

    print('Preparing data for opem_input...')

    opem_input = pd.DataFrame()
    opem_input['OPEM_field_name']=upstream_midstream_for_opem['OPEM_field_name']
    opem_input['Upstream Field Selection']=''
    opem_input['Gas Production Volume (MCFD)']=upstream_midstream_for_opem['ES_Gas_output(mmbtu/d)']*1000/983  
    opem_input['Oil Production Volume (BOED)']=upstream_midstream_for_opem['crude_to_refinery(bbl/d)']
    opem_input['NGL Volume source']=2
    opem_input['NGL C2 Volume (BOED)']=upstream_midstream_for_opem['NGL_C2(boed)']
    opem_input['NGL C3 Volume (BOED)']=upstream_midstream_for_opem['NGL_C3(boed)']
    opem_input['NGL C4 Volume (BOED)']=upstream_midstream_for_opem['NGL_C4(boed)']
    opem_input['NGL C5+ Volume (BOED)']=upstream_midstream_for_opem['NGL_C5(boed)']
    opem_input['Total field NGL volume (BOED)']=''
    opem_input['OPGEE Coke mass (kg/d)']=upstream_midstream_for_opem['petcoke(kg/d)']
    opem_input['% Field NGL C2 Volume allocated to Ethylene converstion']=1
    opem_input['GWP selection (yr period, 100 or 20)']=20

    opem_input_T = opem_input.set_index('OPEM_field_name').T

    #opem_input_T.to_excel(sp_dir + '/Deep Dive page/Analytics/opem_input.xlsx')

    opem_input_index = pd.read_csv(opem_dir + '/opem_input.csv',header=0)
    opem_input_T.reset_index(inplace = True)

    # Get the index from opem_input.csv and update it with opem input values
    df = pd.concat([opem_input_index.iloc[:,:5],opem_input_T.iloc[:,1:]],axis = 1)

    df.to_csv('./opem_input.csv',index=False)
    return upstream_midstream_for_opem


def opem_output_prep(upstream_midstream_for_opem):
    opem_output = pd.read_csv(opem_dir +'/opem_output.csv',header=1)

    #upstream_midstream_for_opem['estimate_boe/d'] = upstream_midstream_for_opem['Oil production volume']*(1+upstream_midstream_for_opem['Gas-to-oil ratio (GOR)']/5800)

    opem_output_T = opem_output.set_index('Selected Oil').T

    opem_output_T.reset_index(inplace = True)

    # Save and reload to get unique column headers
    opem_output_T.to_excel('opem_output.xlsx',index=False)
    opem_output_T = pd.read_excel('opem_output.xlsx')

    up_mid_down = upstream_midstream_for_opem.merge(opem_output_T,left_on='OPEM_field_name',right_on ='index',how='left')
    print('OPEM run completed and up_mid_down file updated.')
    return up_mid_down


#==========Start Upstream Toggle Sensitiviy Runs ===================
upstream = pd.read_sql('select * from scenario_upstream_results',connection)
midstream = pd.read_sql('select * from scenario_midstream_results_nondefault',connection)
default_refinery_table =pd.read_sql('SELECT opgee_field, opgee_country, "Default Refinery" FROM midstream_results',connection)

# Inner join to select only the default refinery type for midstream results
default_refinery_table.rename(columns = {'Default Refinery':'Refinery Type'},inplace = True)
midstream_default = midstream.merge(default_refinery_table,how ='inner')


upstream_midstream_for_opem_uptoggle = opem_input_prep(upstream,midstream_default)

print('Running opem...')
os.system('opem')
up_mid_down_uptoggle = opem_output_prep(upstream_midstream_for_opem_uptoggle)
# set stage of the sensitivity analysis 
up_mid_down_uptoggle['toggle_stage']=np.where(up_mid_down_uptoggle['Scenario']=='LNG','Downstream','Upstream')


# #==========Start Midstream Toggle Sensitiviy Runs ===================
upstream_default = upstream[(upstream['Scenario']=='Electrify' )&( upstream['toggle_value']=='Off')]
upstream = upstream_default
upstream['Scenario'] = 'Refinery Type'


# Calculate Crude to Refinery in bbl/d from Energy Summary tab of OPGEE model. Formula is based on cell G6 in OPEM input tab  
#https://rockmtnins.sharepoint.com/:x:/r/sites/CAE/_layouts/15/Doc.aspx?sourcedoc=%7B5E0994C9-8E35-440B-8BB4-31DF5167F60C%7D&file=OCI%20site%20input%20table%20sources.xlsx&action=default&mobileredirect=true&cid=544ad233-565f-438c-9ce2-d7d5b607b1da

# ('Energy Summary'!E89*1e3/'Energy Summary'!M133)/(158.9873*141.5/(131.5+Results!G34))
upstream['crude_to_refinery(bbl/d)']= (upstream['ES_Crude_output(mmbut/d)']*1e3/upstream['ES_Energy_Density_crude(mmbtu/t)'])/(158.9873*141.5/(131.5+upstream['API gravity']))

# Calculate NGL_C2 export from the field, formula based on cell G8 in OPEM input tab
# (Flowsheet!W17+Flowsheet!CP17)*1000/(5.61458350903291*20.98*2.2)

upstream['NGL_C2(boed)'] = (upstream['FS_LPG_export_C2(t/d)']+upstream['FS_Ethane_to_Petchem(t/d)'])*1000/(5.61458350903291*20.98*2.2)

# Calculate NGL_C3 export from the field, formula based on cell G9 in OPEM input tab
# Flowsheet!W18*1000000/(42*1920)+Flowsheet!W9*(1000000)*0.75/(1923*42) assuming 75% of LPG exported is C3
upstream['NGL_C3(boed)'] = upstream['FS_LPG_export_C3(t/d)']*1000000/(42*1920)+upstream['FS_LPG_export_LPG(t/d)']*(1000000)*0.75/(1923*42)

# Calculate NGL_C4 export from the dfield, formula based on cell G10 in OPEM input tab
# Flowsheet!W19*1000000/(42*2213)+Flowsheet!W9*(1000000)*0.25/(1923*42) assuming 25% of LPG exported is C4
upstream['NGL_C4(boed)'] = upstream['FS_LPG_export_C4(t/d)']*1000000/(42*2213)+upstream['FS_LPG_export_LPG(t/d)']*(1000000)*0.25/(1923*42)

# Calculate NGL_C5+ export from the field, formula based on cell G11 in OPEM input tab
# Assuming all NGL minus C2, C3 and C4 is C5+
# ('Energy Summary'!E87-(((Flowsheet!W17+Flowsheet!CP17)*'Energy Summary'!M141) + 
# (Flowsheet!W18*'Energy Summary'!M142) + (Flowsheet!W19*'Energy Summary'!M143) + 
#(Flowsheet!W9*84950/1923)))/(42*.110)
upstream['NGL_C5(boed)'] = (upstream['ES_NGL_output(mmbtu/d)']-(((upstream['FS_LPG_export_C2(t/d)']+ \
                        upstream['FS_Ethane_to_Petchem(t/d)'])*upstream['ES_Energy_Density_C2(mmbtu/t)']) + \
                        (upstream['FS_LPG_export_C3(t/d)']*upstream['ES_Energy_Density_C3(mmbtu/t)']) + \
                        (upstream['FS_LPG_export_C4(t/d)']*upstream['ES_Energy_Density_C4(mmbtu/t)']) + \
                        (upstream['FS_LPG_export_LPG(t/d)']*84950/1923)))/(42*.110)

# Calculate petcoke from the field, formula based on cell G12 in OPEM input tab
# 1000*(Flowsheet!HG7-'Energy Summary'!E77/'Energy Summary'!M135)
upstream['petcoke(kg/d)'] = 1000*(upstream['FS_Petcoke_to_stock(t/d)']-upstream['ES_Petcoke_fuel(mmbtu/d)']/upstream['ES_Energy_Density_petcoke(mmbtu/t)'])

upstream_midstream = upstream.merge(midstream,right_on=
        ['opgee_field','opgee_country'],left_on = 
        ['Field_name', 'Field location (Country)'], how = 'left')
upstream_midstream['toggle_value'] = upstream_midstream['Refinery Type']

#upstream_midstream[upstream_midstream['sulfur'].isna()][['Field name','year']]

# Filter out obsolete and misinput upstream fields such as Bakken , Generic 
upstream_midstream = upstream_midstream[upstream_midstream['sulfur'].notna()]

upstream_midstream_for_opem = upstream_midstream

upstream_midstream_for_opem['OPEM_field_name']=(upstream_midstream_for_opem['opgee_field']+';'
                                                +upstream_midstream_for_opem['original_file']+';'
                                                +upstream_midstream_for_opem['Scenario']+';'
                                                +upstream_midstream_for_opem['toggle_value'])
                                                


print('Getting data for OPEM Product Slates...')

upstream_midstream_for_opem['volume_flow_bbl']=''
upstream_midstream_for_opem['Product Slate (bbl product per day)'] = ''
upstream_midstream_for_opem['energy_flow_MJ']=''
upstream_midstream_for_opem['mass_flow_kg']=''

# LPG: Mass [kg]/((2.04 kg/m3 gas)*(0.159 m3/bbl gas)*(270 bbl gas/bbl liquid))
# where .159 is a constant to convert volume units, and mass is found on [Sheet1 row 39] of the prelim output
# Petrochem feedstock: Mass [kg]/((1.264 kg/m3 gas)*(0.159 m3/bbl gas)*(270 bbl gas/bbl liquid))
# where .159 is a constant to convert volume units, and mass is found on [Sheet1 row 40] of the prelim output
upstream_midstream_for_opem['Liquefied Petroleum Gases (LPG)_bbl']= upstream_midstream_for_opem['Liquified Petroleum Gas (LPG).1']/(2.04*0.159*270)
upstream_midstream_for_opem['Petrochemical Feedstocks_bbl']=upstream_midstream_for_opem['Petrochemical Feedstocks.1']/(1.264*0.159*270)
upstream_midstream_for_opem['Asphalt_bbl']= 0

upstream_midstream_for_opem['emission(kgCO2eq/bbl)']='kgCO2eq/bbl'
upstream_midstream_for_opem['Per energy content of refinery product (gCO2eq/MJ)'] = 'Emission by Product (gCO2eq/MJ)'
upstream_midstream_for_opem['Allocation to gasoline (g CO2eq/MJ)']='Process Emission (gCO2eq/MJ)'

opem_product_slate =  upstream_midstream_for_opem[['OPEM_field_name',                                                   
                                                'Product Slate (bbl product per day)',
                                                'volume_flow_bbl','throughput','Gasoline',
'Jet Fuel',
'Diesel',
'Fuel Oil',
'Petroleum Coke',
'Residual Fuels',
'Refinery Fuel Gas (RFG)',
'Liquefied Petroleum Gases (LPG)_bbl',
'Petrochemical Feedstocks_bbl',
'Asphalt_bbl',                                                   
'energy_flow_MJ',
'Blended Gasoline',
'Jet-A/AVTUR',
'ULSD',
'Fuel Oil.1',
'Coke',
'Liquid Heavy Ends',
'RFG',
'Surplus NCR H2',
'Liquified Petroleum Gas (LPG)',
'Petrochemical Feedstocks',
'Asphalt',
'Gasoline S wt%',
'Gasoline H2 wt%','mass_flow_kg',
'Blended Gasoline.1',
'Jet-A/AVTUR.1',
'ULSD.1',
'Fuel Oil.2',
'Coke.1',
'Liquid Heavy Ends.1',
'Sulphur',
'RFG.1',
'Surplus NCR H2.1',
'Liquified Petroleum Gas (LPG).1',
'Petrochemical Feedstocks.1',
'Asphalt.1']]

opem_product_slate = opem_product_slate.T

opem_product_slate.columns = opem_product_slate.iloc[0]
opem_product_slate = opem_product_slate.iloc[1:,:]

slate_index = pd.read_csv(opem_dir + '/src/opem/products/product_slates/all_product_slates.csv')

opem_product_slate.index = slate_index.iloc[:,0]

#opem_product_slate.to_excel('../Deep Dive page/Analytics/all_product_slates.xlsx')
opem_product_slate.to_csv(opem_dir + '/src/opem/products/product_slates/all_product_slates.csv')

print('Preparing data for opem_input...')

opem_input = pd.DataFrame()
opem_input['OPEM_field_name']=upstream_midstream_for_opem['OPEM_field_name']
opem_input['Upstream Field Selection']=''
opem_input['Gas Production Volume (MCFD)']=upstream_midstream_for_opem['ES_Gas_output(mmbtu/d)']*1000/983  
opem_input['Oil Production Volume (BOED)']=upstream_midstream_for_opem['crude_to_refinery(bbl/d)']
opem_input['NGL Volume source']=2
opem_input['NGL C2 Volume (BOED)']=upstream_midstream_for_opem['NGL_C2(boed)']
opem_input['NGL C3 Volume (BOED)']=upstream_midstream_for_opem['NGL_C3(boed)']
opem_input['NGL C4 Volume (BOED)']=upstream_midstream_for_opem['NGL_C4(boed)']
opem_input['NGL C5+ Volume (BOED)']=upstream_midstream_for_opem['NGL_C5(boed)']
opem_input['Total field NGL volume (BOED)']=''
opem_input['OPGEE Coke mass (kg/d)']=upstream_midstream_for_opem['petcoke(kg/d)']
opem_input['% Field NGL C2 Volume allocated to Ethylene converstion']=1
opem_input['GWP selection (yr period, 100 or 20)']=20

opem_input_T = opem_input.set_index('OPEM_field_name').T

#opem_input_T.to_excel('../Deep Dive page/Analytics/opem_input.xlsx')

opem_input_index = pd.read_csv(opem_dir + '/opem_input.csv',header=0)
opem_input_T.reset_index(inplace = True)

# Get the index from opem_input.csv and update it with opem input values
df = pd.concat([opem_input_index.iloc[:,:5],opem_input_T.iloc[:,1:]],axis = 1)

df.to_csv('./opem_input.csv',index=False)

#upstream_default['toggle_value'] = upstream_midstream_for_opem_midtoggle['Refinery Type']
#upstream_midstream_for_opem_midtoggle = opem_input_prep(upstream_default,midstream)

print('Running opem...')
os.system('opem')
up_mid_down_midtoggle = opem_output_prep(upstream_midstream_for_opem)
up_mid_down_midtoggle['toggle_stage']='Midstream'
#print(up_mid_down_midtoggle.shape)


#==========Start Downstream Toggle Sensitiviy Runs ===================
downtoggle = {'Petroleum Coke Combustion':['0','0.5'],'Transport Distance':['0.5','1.5']}
downresult = []
for s in downtoggle:
    for t in downtoggle[s]:
        upstream_default['Scenario']=s
        upstream_default['toggle_value']=t
        upstream_midstream_for_opem_uptoggle = opem_input_prep(upstream_default,midstream_default)
        df = pd.read_csv('./opem_input.csv')
        if s =='Petroleum Coke Combustion':                       
            df.iloc[[17,20],5:]=float(t)
        else:
            df.iloc[76:147,5:]= pd.concat([float(t)*df.iloc[76:147,4]]*(df.iloc[76:147,5:].shape[1]),axis=1,ignore_index=True)
        df.to_csv('./opem_input.csv',index = False)
        print('Running opem for scenario '+s+' toggle value ' +t)
        os.system('opem')
        downresult.append(opem_output_prep(upstream_midstream_for_opem_uptoggle))

up_mid_down_downtoggle = pd.concat(downresult,axis=0)
up_mid_down_downtoggle['toggle_stage']='Downstream'

up_mid_down_all = pd.concat([up_mid_down_uptoggle,up_mid_down_midtoggle, up_mid_down_downtoggle])
up_mid_down_all.to_sql('scenario_up_mid_down',connection, if_exists='replace', index=False)
