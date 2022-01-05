import pandas as pd
import numpy as np

# Get the directory of all csv files

import os

opem_dir = '/Users/rwang/Documents/OCI+/Downstream/opem'
sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'
d = sp_dir+ '/Upstream/csvs/'
directory = os.fsencode(d)
list_csv =[]
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith('.csv') and not filename.startswith('2'): 
        if filename.startswith('Vent-fug'): 
            os.rename(d+filename, d+'Vent_fug'+filename[8:])
            list_csv.append('Vent_fug'+filename[8:])
        if filename.startswith('TS'):
            if 'Eagle Ford - condensate' in filename: #fix Eagle Ford -condensate error 
                continue 
            nfilename = filename[:7]+'-'+filename[8:]
            os.rename(d+filename, d+nfilename)
            if '-OML 11' in nfilename:
                os.rename(d+nfilename, d+nfilename.replace('-OML 11 ','-Bonny OML 11'))
                nfilename = nfilename.replace('-OML 11 ','-Bonny OML 11')
            list_csv.append(nfilename)
        else:
            list_csv.append(filename)
        continue
    else:
        continue


# Renaming files that were not created consistently with other scenario runs
# for file in list_csv:
#     if file.startswith('Vent-fug'):
#          os.rename(d+file, d+'Vent_fug'+file[8:])

# Extract data needed from all results.csv files
list_results = []
for filename in list_csv:
    if filename.endswith('Results.csv'):
        list_results.append(filename)

column_names =  [    
'Downhole pump',
 'Water reinjection ',
 'Natural gas reinjection',
 'Water flooding',
 'Gas lifting',
 'Gas flooding',
 'Steam flooding',
 'Oil sands mine (integrated with upgrader)',
 'Oil sands mine (non-integrated with upgrader)',
 'Field location (Country)',
 'Field_name',
 'Field age',
 'Field depth',
 'Oil production volume',
 'Number of producing wells',
 'Number of water injecting wells',
 'Production tubing diameter',
 'Productivity index',
 'Reservoir pressure',
 'Reservoir temperature',
 'Offshore?',
 'API gravity',
 'Gas composition N2',
 'Gas composition CO2',
 'Gas composition C1',
 'Gas composition C2',
 'Gas composition C3',
 'Gas composition C4+',
 'Gas composition H2S',
 'Gas-to-oil ratio (GOR)',
 'Water-to-oil ratio (WOR)',
 'Water injection ratio',
 'Gas lifting injection ratio',
 'Gas flooding injection ratio',
 'Flood gas ',
 'Liquids unloading practice',
 'Fraction of CO2 breaking through to producers',
 'Source of makeup CO2',
 'Percentage of sequestration credit assigned to the oilfield',
 'Steam-to-oil ratio (SOR)',
 'Fraction of required electricity generated onsite',
 'Fraction of remaining natural gas reinjected',
 'Fraction of produced water reinjected',
 'Fraction of steam generation via cogeneration ',
 'Fraction of steam generation via solar thermal',
 'Heater/treater',
 'Stabilizer column',
 'Upgrader type',
 'Associated Gas Processing Path',
 'Flaring-to-oil ratio',
 'Venting-to-oil ratio (purposeful)',
 'Volume fraction of diluent',
 'Low carbon richness (semi-arid grasslands)',
 'Moderate carbon richness (mixed)',
 'High carbon richness (forested)',
 'Low intensity development and low oxidation',
 'Moderate intensity development and moderate oxidation',
 'High intensity development and high oxidation',
 'Ocean tanker',
 'Barge',
 'Pipeline',
 'Rail',
 'Truck',
 'Transport distance (one way) - Ocean tanker',
 'Transport distance (one way) - Barge',
 'Transport distance (one way) - Pipeline',
 'Transport distance (one way) - Rail',
 'Transport distance (one way) - Truck',
 'Ocean tanker size, if applicable',
 'Small sources emissions',
 'e-Total energy consumption','e-Total GHG emissions', 
    'e-Total GHG emissions-Combustion/land use','e-Total GHG emissions-VFF',
    'd-Total energy consumption','d-Total GHG emissions', 
    'd-Total GHG emissions-Combustion/land use','d-Total GHG emissions-VFF',
    'p-Total energy consumption','p-Total GHG emissions', 
    'p-Total GHG emissions-Combustion/land use','p-Total GHG emissions-VFF',
    's-Total energy consumption','s-Total GHG emissions', 
    's-Total GHG emissions-Combustion/land use','s-Total GHG emissions-VFF',
    'l-Total energy consumption','l-Total GHG emissions', 
    'l-Total GHG emissions-Combustion/land use','l-Total GHG emissions-VFF',
    'm-Total energy consumption','m-Total GHG emissions', 
    'm-Total GHG emissions-Combustion/land use','m-Total GHG emissions-VFF',
    'w-Total energy consumption','w-Total GHG emissions', 
    'w-Total GHG emissions-Combustion/land use','w-Total GHG emissions-VFF',
    't-Total energy consumption','t-Total GHG emissions', 
    't-Total GHG emissions-Combustion/land use','t-Total GHG emissions-VFF','t-Loss factor',
    'g-Total energy consumption','g-Total GHG emissions', 
    'g-Total GHG emissions-Combustion/land use','g-Total GHG emissions-VFF',
    'Other small sources','Offsite emissions credit/debit','Lifecycle energy consumption',
    'CSS-Total CO2 sequestered','Lifecycle GHG emissions','Field-by-field check']

# function to extract results from one csv an store in a dataframe
def clean_df(df,column_names):
    '''clean the df and transpose to map the column names'''
    df = df.iloc[: , 7:]
    df = df.iloc[[8,9,10,11,12,13,14,15,16,19,20,21,22,23,24,25,26,27,28,29,30,33,35,36,37,38,39,40,41,45,46,47,48,49,
         50,54,57,58,61,62,63,64,65,66,67,69,70,71,76,85,86,87,91,92,93,95,96,97,101,102,103,104,105,107,108,109,110,
        111,112,114,129,130,131,132,135,136,137,138,141,142,143,144,147,148,149,150,153,154,155,156,159,160,161,162,
                 165,166,167,168,171,172,173,174,175,178,179,180,181,183,185,187,190,192,194]]
    df_t = df.transpose()
    df_t.columns = column_names
    df_t = df_t.dropna(how = 'all')
    return df_t



list_df =[]
for file in list_results:
    #print(file)
    df = pd.read_csv(d+file,header = None)  
    result = clean_df(df,column_names)
    result['Scenario']=file.split('-')[0]
    result['toggle_value']=file.split('-')[1]
    result['original_file']=file.split('-')[2]
    #result['year']=file.split('-')[2][:-5].split('_')[0]
    result['field_type']=file.split('-')[2][:-5].split('_')[0].lower()
    result['frack?']= True if file.split('-')[2][:-5].split('_')[1].lower()=='frack' else False
    result['lng?']= True if file.split('-')[2][:-5].split('_')[2].lower()=='lng' else False
    list_df.append(result)

results_df = pd.concat(list_df)
#results_df = final_df[(final_df['year']=='2020')]

# Converting all numerical columns into float type

numerical_columns = [
 'Field age',
 'Field depth',
 'Oil production volume',
 'Number of producing wells',
 'Number of water injecting wells',
 'Production tubing diameter',
 'Productivity index',
 'Reservoir pressure',
 'Reservoir temperature',
 'Offshore?',
 'API gravity',
 'Gas composition N2',
 'Gas composition CO2',
 'Gas composition C1',
 'Gas composition C2',
 'Gas composition C3',
 'Gas composition C4+',
 'Gas composition H2S',
 'Gas-to-oil ratio (GOR)',
 'Water-to-oil ratio (WOR)',
 'Water injection ratio',
 'Gas lifting injection ratio',
 'Gas flooding injection ratio',
 'Flood gas ',
 'Liquids unloading practice',
 'Fraction of CO2 breaking through to producers',
 'Source of makeup CO2',
 'Percentage of sequestration credit assigned to the oilfield',
 'Steam-to-oil ratio (SOR)',
 'Fraction of required electricity generated onsite',
 'Fraction of remaining natural gas reinjected',
 'Fraction of produced water reinjected',
 'Fraction of steam generation via cogeneration ',
 'Fraction of steam generation via solar thermal',
 'Heater/treater',
 'Stabilizer column',
 'Upgrader type',
 'Associated Gas Processing Path',
 'Flaring-to-oil ratio',
 'Venting-to-oil ratio (purposeful)',
 'Volume fraction of diluent',
 'Low carbon richness (semi-arid grasslands)',
 'Moderate carbon richness (mixed)',
 'High carbon richness (forested)',
 'Low intensity development and low oxidation',
 'Moderate intensity development and moderate oxidation',
 'High intensity development and high oxidation',
 'Ocean tanker',
 'Barge',
 'Pipeline',
 'Rail',
 'Truck',
 'Transport distance (one way) - Ocean tanker',
 'Transport distance (one way) - Barge',
 'Transport distance (one way) - Pipeline',
 'Transport distance (one way) - Rail',
 'Transport distance (one way) - Truck',
 'Ocean tanker size, if applicable',
 'Small sources emissions',
 'e-Total energy consumption',
 'e-Total GHG emissions',
 'e-Total GHG emissions-Combustion/land use',
 'e-Total GHG emissions-VFF',
 'd-Total energy consumption',
 'd-Total GHG emissions',
 'd-Total GHG emissions-Combustion/land use',
 'd-Total GHG emissions-VFF',
 'p-Total energy consumption',
 'p-Total GHG emissions',
 'p-Total GHG emissions-Combustion/land use',
 'p-Total GHG emissions-VFF',
 's-Total energy consumption',
 's-Total GHG emissions',
 's-Total GHG emissions-Combustion/land use',
 's-Total GHG emissions-VFF',
 'l-Total energy consumption',
 'l-Total GHG emissions',
 'l-Total GHG emissions-Combustion/land use',
 'l-Total GHG emissions-VFF',
 'm-Total energy consumption',
 'm-Total GHG emissions',
 'm-Total GHG emissions-Combustion/land use',
 'm-Total GHG emissions-VFF',
 'w-Total energy consumption',
 'w-Total GHG emissions',
 'w-Total GHG emissions-Combustion/land use',
 'w-Total GHG emissions-VFF',
 't-Total energy consumption',
 't-Total GHG emissions',
 't-Total GHG emissions-Combustion/land use',
 't-Total GHG emissions-VFF',
 't-Loss factor',
 'g-Total energy consumption',
 'g-Total GHG emissions',
 'g-Total GHG emissions-Combustion/land use',
 'g-Total GHG emissions-VFF',
 'Other small sources',
 'Offsite emissions credit/debit',
 'Lifecycle energy consumption',
 'CSS-Total CO2 sequestered',
 'Lifecycle GHG emissions']

results_df.rename(columns = {'Field name':'Field_name'}, inplace = True)

results_df = results_df.replace(r'^\s+$', np.nan, regex=True)

results_df = results_df.replace(r'\\', np.nan, regex=True)

results_df.reset_index(inplace = True)

results_df.drop(columns = 'index',inplace = True)

results_df[numerical_columns]= results_df[numerical_columns].astype(float)

results_df['Field_name']=results_df['Field_name'].apply(lambda x: x.strip())

# Extract data needed from all energy summary.csv files

list_energy_summary = []
for filename in list_csv:
    if filename.endswith('Energy Summary.csv'):
        list_energy_summary.append(filename)

ES_MJperd =[]
ES_mmbtuperd = []
ES_Energy_Density_crude_oil = []
ES_Energy_Density_petcoke = []
ES_Energy_Density_C2 = []
ES_Energy_Density_C3 = []
ES_Energy_Density_C4 = []
ES_Crude_output = []
ES_Gas_output = []
ES_NGL_output = []
ES_Gas_output_MJ = []
ES_Petcoke_fuel =[]
Field_name = []
original_file = []
Scenario = []
toggle_value = []

for file in list_energy_summary:
    df = pd.read_csv(d+file,header=None)
    ES_MJperd.append(float(df.iloc[127,5]))
    ES_mmbtuperd.append(float(df.iloc[127,4]))
    ES_Energy_Density_crude_oil.append(float(df.iloc[132,12]))
    ES_Energy_Density_petcoke.append(float(df.iloc[134,12]))
    ES_Energy_Density_C2.append(float(df.iloc[140,12]))
    ES_Energy_Density_C3.append(float(df.iloc[141,12]))
    ES_Energy_Density_C4.append(float(df.iloc[142,12]))
   
    ES_Crude_output.append(float(df.iloc[88,4]))
    ES_Gas_output.append(float(df.iloc[84,4]))
    
    if df.iloc[120,3] == 'Gas':
        ES_Gas_output_MJ.append(float(df.iloc[120,5]))
    else:
        ES_Gas_output_MJ.append(float(df.iloc[123,5]))
        
    ES_NGL_output.append(float(df.iloc[86,4]))
    ES_Petcoke_fuel.append(float(df.iloc[76,4]))
    
    Field_name.append(df.iloc[0,7].strip())
    
    Scenario.append(file.split('-')[0])
    toggle_value.append(file.split('-')[1])
    original_file.append(file.split('-')[2])
    

energy_summary = pd.DataFrame({'Field_name':Field_name,'Scenario':Scenario,'toggle_value':toggle_value,'original_file':original_file,
                               'ES_MJperd':ES_MJperd,'ES_mmbtuperd':ES_mmbtuperd,
                               'ES_Energy_Density_crude(mmbtu/t)':ES_Energy_Density_crude_oil,'ES_Energy_Density_petcoke(mmbtu/t)':ES_Energy_Density_petcoke,
                              'ES_Energy_Density_C2(mmbtu/t)':ES_Energy_Density_C2,'ES_Energy_Density_C3(mmbtu/t)':ES_Energy_Density_C3,
                               'ES_Energy_Density_C4(mmbtu/t)':ES_Energy_Density_C4, 'ES_Crude_output(mmbut/d)':ES_Crude_output,
                              'ES_Gas_output(mmbtu/d)':ES_Gas_output, 'ES_NGL_output(mmbtu/d)':ES_NGL_output,
                              'ES_Gas_output(MJ/d)':ES_Gas_output_MJ,'ES_Petcoke_fuel(mmbtu/d)':ES_Petcoke_fuel})


merge_keys=['Field_name','original_file','Scenario','toggle_value']

results_ES = pd.merge(results_df,energy_summary,left_on=merge_keys,right_on=merge_keys,how='left')

results_ES['tCO2e/yr']=results_ES['Lifecycle GHG emissions']*\
    results_ES['ES_MJperd']/10**6*365

#results_ES['Field Carbon Intensity(kgCO2e/boe)']=results_ES['tCO2e/yr']*1000/results_ES['annual production(boe/yr)']

# Extract methane emission from flaring.csv files

list_flaring=[]
for filename in list_csv:
    if filename.endswith('Flaring.csv'):
        list_flaring.append(filename)

flaring_ch4 =[]
Field_name = []
original_file = []
Scenario = []
toggle_value = []

for file in list_flaring:
    df = pd.read_csv(d+file,header=None)
    flaring_ch4.append(float(df.iloc[80,12]))
    Field_name.append(df.iloc[0,7].strip())
    Scenario.append(file.split('-')[0])
    toggle_value.append(file.split('-')[1])
    original_file.append(file.split('-')[2])

flaring = pd.DataFrame({'flaring_ch4(t/d)':flaring_ch4,'Field_name':Field_name,'Scenario':Scenario,'toggle_value':toggle_value,'original_file':original_file})

# Extract co2 and methane emission from vff summary.csv files

list_vff=[]
for filename in list_csv:
    if filename.endswith('VFF Summary.csv'):
        list_vff.append(filename)

venting_ch4 =[]
venting_ch4_miq = []
venting_ch4_uponly = []
fugitive_ch4 =[]
fugitive_ch4_miq = []
fugitive_ch4_uponly = []
venting_co2 = []
fugitive_co2 = []
Field_name = []
original_file = []
Scenario = []
toggle_value = []

for file in list_vff:
    df = pd.read_csv(d+file,header=None)
    venting_ch4.append(sum(df.iloc[111:157,9].apply(lambda x:float(x))))
    fugitive_ch4.append(sum(df.iloc[111:157,10].apply(lambda x:float(x))))
    venting_co2.append(sum(df.iloc[111:157,7].apply(lambda x:float(x))))
    fugitive_co2.append(sum(df.iloc[111:157,8].apply(lambda x:float(x))))
    venting_ch4_miq.append(sum(df.iloc[111:131,9].apply(lambda x:float(x)))+sum(df.iloc[147:157,9].apply(lambda x:float(x))))
    fugitive_ch4_miq.append(sum(df.iloc[111:131,10].apply(lambda x:float(x)))+sum(df.iloc[147:157,10].apply(lambda x:float(x))))
    fugitive_ch4_uponly.append(sum(df.iloc[111:136,10].apply(lambda x:float(x)))+sum(df.iloc[147:157,10].apply(lambda x:float(x))))
    venting_ch4_uponly.append(sum(df.iloc[111:136,9].apply(lambda x:float(x)))+sum(df.iloc[147:157,9].apply(lambda x:float(x))))
    
    Field_name.append(df.iloc[0,7].strip())
    Scenario.append(file.split('-')[0])
    toggle_value.append(file.split('-')[1])
    original_file.append(file.split('-')[2])
vff = pd.DataFrame({'Field_name':Field_name,'Scenario':Scenario,'toggle_value':toggle_value,'original_file':original_file,
                   'venting_ch4(t/d)':venting_ch4,'fugitive_ch4(t/d)':fugitive_ch4,
                   'venting_co2(t/d)':venting_co2,'fugitive_co2(t/d)':fugitive_co2,
                   'venting_ch4_miq(t/d)':venting_ch4_miq,'fugitive_ch4_miq(t/d)':fugitive_ch4_miq,
                   'venting_ch4_uponly(t/d)':venting_ch4_uponly,'fugitive_ch4_uponly(t/d)':fugitive_ch4_uponly})

# merge flaring and vff to calculate methane emission 
ch4_co2 = pd.merge(vff,flaring,left_on = merge_keys,right_on= merge_keys)

ch4_co2['tCH4/year'] = (ch4_co2['flaring_ch4(t/d)']+ch4_co2['venting_ch4(t/d)']+ch4_co2['fugitive_ch4(t/d)'])*365
ch4_co2['tCH4/year-miQ']=(ch4_co2['flaring_ch4(t/d)']+ch4_co2['venting_ch4_miq(t/d)']+ch4_co2['fugitive_ch4_miq(t/d)'])*365


# merge results, energy summary, flaring and vff 
results_ES_ch4_co2 = pd.merge(results_ES,ch4_co2,left_on=merge_keys,right_on=merge_keys,how='left')

# Extract data from flow sheet.csv files
list_FS=[]
for filename in list_csv:
    if filename.endswith('Flow Sheet.csv'):
        list_FS.append(filename)

FS_LPG_export_LPG = [] #Flow Sheet!W9
FS_LPG_export_C2 = [] #W17
FS_LPG_export_C3 = [] #W18
FS_LPG_export_C4  = [] #W19 
FS_Ethane_to_Petchem = [] #CP17
FS_Petcoke_to_stock =[]
FS_Gas_at_Wellhead =[] #AF24
        
Field_name = []
original_file = []
Scenario = []
toggle_value = []

for file in list_FS:
    df = pd.read_csv(d+file,header=None)
    FS_LPG_export_LPG.append(float(df.iloc[8,22]))
    FS_LPG_export_C2.append(float(df.iloc[16,22]))
    FS_LPG_export_C3.append(float(df.iloc[17,22]))
    FS_LPG_export_C4.append(float(df.iloc[18,22]))
    FS_Ethane_to_Petchem.append(float(df.iloc[16,93]))
    FS_Petcoke_to_stock.append(float(df.iloc[6,214]))
    FS_Gas_at_Wellhead.append(float(df.iloc[23,31]))
    Field_name.append(('-'.join(file.split('-')[3:-1])).strip())
    Scenario.append(file.split('-')[0])
    toggle_value.append(file.split('-')[1])
    original_file.append(file.split('-')[2])
    
flowsheet = pd.DataFrame({'Field_name':Field_name,'Scenario':Scenario,'toggle_value':toggle_value,'original_file':original_file,
                   'FS_LPG_export_LPG(t/d)':FS_LPG_export_LPG,'FS_LPG_export_C2(t/d)':FS_LPG_export_C2,
                   'FS_LPG_export_C3(t/d)': FS_LPG_export_C3, 'FS_LPG_export_C4(t/d)':FS_LPG_export_C4,
                          'FS_Ethane_to_Petchem(t/d)':FS_Ethane_to_Petchem,
                         'FS_Petcoke_to_stock(t/d)':FS_Petcoke_to_stock,'FS_Gas_at_Wellhead(t/d)':FS_Gas_at_Wellhead})

results_ES_ch4_co2_fs =pd.merge(results_ES_ch4_co2,flowsheet, left_on=merge_keys,right_on=merge_keys,how='left')

# Extract data from GHG summary.csv files
list_GHGS=[]
for filename in list_csv:
    if filename.endswith('GHG Summary.csv'):
        list_GHGS.append(filename)

Field_name = []
original_file = []
Scenario = []
toggle_value = []
combustion_to_totalGHG = []

for file in list_GHGS:
    df = pd.read_csv(d+file,header=None)
    combustion_to_totalGHG.append(float(df.iloc[18,7])/float(df.iloc[18,12])) #H19/M19
    Field_name.append(('-'.join(file.split('-')[3:-1])).strip())
    Scenario.append(file.split('-')[0])
    toggle_value.append(file.split('-')[1])
    original_file.append(file.split('-')[2])
    
combustion = pd.DataFrame({'Field_name':Field_name,'Scenario':Scenario,'toggle_value':toggle_value,'original_file':original_file,
                   'combustion_ratio':combustion_to_totalGHG})

results_ES_ch4_co2_fs_ghg =pd.merge(results_ES_ch4_co2_fs,combustion, left_on=merge_keys,right_on=merge_keys,how='left')
results_ES_ch4_co2_fs_ghg.to_excel(sp_dir+'/Deep Dive page/Analytics/all_upstream_results.xlsx',index = False)

upstream = results_ES_ch4_co2_fs_ghg 
upstream=upstream[upstream['Scenario']!='Cogeneration'] #remove co-gen scenario
upstream = upstream[upstream['Field_name']!='Amenamkpono'] #remove the nigeria field

import sqlite3
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")
upstream.to_sql('scenario_upstream_results',connection, if_exists='replace',index = False)




