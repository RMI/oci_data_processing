import pandas as pd
import numpy as np
import os

# Load all csvs files into csvs folder
#os.system("az storage blob download-batch --destination '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/csvs' --source csvs --sas-token '?sv=2020-08-04&ss=bfqt&srt=sco&sp=rl&se=2022-10-14T03:32:20Z&st=2021-10-13T19:32:20Z&spr=https&sig=WL8KGvOgEve5iluhVafKP0MMMkkBOPmluV3%2B8LGAFb8%3D' --account-name ocirmistorage")

# Extracting Data from csvs folder 
# Get the directory of all csv files
sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'
d = sp_dir + '/Upstream/csvs/'
directory = os.fsencode(sp_dir + '/Upstream/csvs')
list_csv =[]
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith('.csv') and filename.startswith('2'): 
        #print(filename)
        list_csv.append(filename)
        continue
    else:
        continue


print('Extract data needed from all results.csv files...')

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
    df = pd.read_csv(d+file,header = None)  
    result = clean_df(df,column_names)
    result['original_file']=file.split('-')[0]
    result['year']=file.split('-')[0][:-5].split('_')[0]
    result['field_type']=file.split('-')[0][:-5].split('_')[1].lower()
    result['frack?']= True if file.split('-')[0][:-5].split('_')[2].lower()=='frack' else False
    result['lng?']= True if file.split('-')[0][:-5].split('_')[3].lower()=='lng' else False
    list_df.append(result)

results_df = pd.concat(list_df)


#### skip, skip, skip, Error check code; we are moving forward to use the field that says error because the emission differences are small

# df = pd.concat(list_df)
# # convert all numerical values to floats; keep categorical values as strings
# df[['Oil production volume','Gas-to-oil ratio (GOR)','Lifecycle GHG emissions'
#  ]] = df[['Oil production volume','Gas-to-oil ratio (GOR)','Lifecycle GHG emissions'
#  ]].applymap(lambda x: float(x))
# df['annual production(boe/yr)']=df['Oil production volume']*(1+df['Gas-to-oil ratio (GOR)']/5800)*365

# #df[df['Field-by-field check']=='ERROR'][['Field location (Country)','Field name','annual production(boe/yr)']].sort_values(by='annual production(boe/yr)',ascending = False)

# df_error = df.groupby(['Field-by-field check','Field location (Country)'])['annual production(boe/yr)'].sum().unstack(level=0)

# df_error.head()

# df_error['error_ok_ratio']=df_error['ERROR']/df_error['OK']

# df_error['error_ok_ratio'].sort_values(ascending = False)

# df.groupby('Field location (Country)').sum()

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

results_df = results_df.replace(r'^\s+$', np.nan, regex=True)

results_df = results_df.replace(r'\\', np.nan, regex=True)

results_df.reset_index(inplace = True)

results_df.drop(columns = 'index',inplace = True)

results_df[numerical_columns]= results_df[numerical_columns].astype(float)

results_df['Field_name']=results_df['Field_name'].apply(lambda x: x.strip())

print('Extract data needed from all energy summary.csv files...')

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
    original_file.append(file.split('-')[0])

energy_summary = pd.DataFrame({'Field_name':Field_name,'original_file':original_file,
                               'ES_MJperd':ES_MJperd,'ES_mmbtuperd':ES_mmbtuperd,
                               'ES_Energy_Density_crude(mmbtu/t)':ES_Energy_Density_crude_oil,'ES_Energy_Density_petcoke(mmbtu/t)':ES_Energy_Density_petcoke,
                              'ES_Energy_Density_C2(mmbtu/t)':ES_Energy_Density_C2,'ES_Energy_Density_C3(mmbtu/t)':ES_Energy_Density_C3,
                               'ES_Energy_Density_C4(mmbtu/t)':ES_Energy_Density_C4, 'ES_Crude_output(mmbut/d)':ES_Crude_output,
                              'ES_Gas_output(mmbtu/d)':ES_Gas_output, 'ES_NGL_output(mmbtu/d)':ES_NGL_output,
                              'ES_Gas_output(MJ/d)':ES_Gas_output_MJ,'ES_Petcoke_fuel(mmbtu/d)':ES_Petcoke_fuel})


results_ES = results_df.merge(energy_summary,how='outer',indicator = True)

if results_ES[results_ES['_merge']!='both'].shape[0]>0:
    print('Unmatched Field: Results // Energy Summary. Check the merge')
else:
    results_ES.drop(columns = '_merge',inplace = True)

results_ES['tCO2e/yr']=results_ES['Lifecycle GHG emissions']*\
    results_ES['ES_MJperd']/10**6*365

print('Extract methane emission from flaring.csv files...')

list_flaring=[]
for filename in list_csv:
    if filename.endswith('Flaring.csv'):
        list_flaring.append(filename)

flaring_ch4 =[]
Field_name = []
original_file = []

for file in list_flaring:
    df = pd.read_csv(d+file,header=None)
    flaring_ch4.append(float(df.iloc[80,12]))
    Field_name.append(df.iloc[0,7].strip())
    original_file.append((file.split('-')[0]))

flaring = pd.DataFrame({'flaring_ch4(t/d)':flaring_ch4,'Field_name':Field_name,'original_file':original_file})

print('Extract co2 and methane emission from vff summary.csv files...')

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
    original_file.append((file.split('-')[0]))

vff = pd.DataFrame({'Field_name':Field_name,'original_file':original_file,
                   'venting_ch4(t/d)':venting_ch4,'fugitive_ch4(t/d)':fugitive_ch4,
                   'venting_co2(t/d)':venting_co2,'fugitive_co2(t/d)':fugitive_co2,
                   'venting_ch4_miq(t/d)':venting_ch4_miq,'fugitive_ch4_miq(t/d)':fugitive_ch4_miq,
                   'venting_ch4_uponly(t/d)':venting_ch4_uponly,'fugitive_ch4_uponly(t/d)':fugitive_ch4_uponly})

# merge flaring and vff to calculate methane emission 
ch4_co2 = vff.merge(flaring,how ='outer',indicator = True)
if ch4_co2[ch4_co2['_merge']!='both'].shape[0]>0:
    print('Unmatched Field: vff // flaring. Check the merge')
else:
    ch4_co2.drop(columns = '_merge',inplace = True)

ch4_co2['tCH4/year'] = (ch4_co2['flaring_ch4(t/d)']+ch4_co2['venting_ch4(t/d)']+ch4_co2['fugitive_ch4(t/d)'])*365
ch4_co2['tCH4/year-miQ']=(ch4_co2['flaring_ch4(t/d)']+ch4_co2['venting_ch4_miq(t/d)']+ch4_co2['fugitive_ch4_miq(t/d)'])*365


# merge results, energy summary, flaring and vff 
results_ES_ch4_co2 = results_ES.merge(ch4_co2,how='outer',indicator = True)

if results_ES_ch4_co2[results_ES_ch4_co2['_merge']!='both'].shape[0]>0:
    print('Unmatched Field: results_ES // ch4_co2. Check the merge')
else:
    results_ES_ch4_co2.drop(columns = '_merge',inplace = True)

# results_ES_ch4_co2['Field Methane Intensity(kgCH4/boe)']=results_ES_ch4_co2['tCH4/year']*1000/results_ES_ch4_co2['annual production(boe/yr)']

#results_ES_ch4_co2['fugitive intensity(kgCH4/boe)']= results_ES_ch4_co2['fugitive_ch4(t/d)']*1000*365/results_ES_ch4_co2['annual production(boe/yr)']
#results_ES_ch4_co2['venting intensity(kgCH4/boe)']= results_ES_ch4_co2['venting_ch4(t/d)']*1000*365/results_ES_ch4_co2['annual production(boe/yr)']
#results_ES_ch4_co2['flaring intensity(kgCH4/boe)']= results_ES_ch4_co2['flaring_ch4(t/d)']*1000*365/results_ES_ch4_co2['annual production(boe/yr)']

# Extract data from allocation.csv files
# Commenting out as we are not using allocation sheet anymore.

# list_allocation=[]
# for filename in list_csv:
#     if filename.endswith('Allocation.csv'):
#         list_allocation.append(filename)

# allocation_crude = [] #Allocation!H24
# allocation_NGL = [] #H14
# allocation_petcoke = [] #H30   
# allocation_gas = [] #H33
        
# Field_name = []
# original_file = []
# for file in list_allocation:
#     df = pd.read_csv(d+file,header=None)
#     allocation_crude.append(float(df.iloc[23,7]))
#     allocation_NGL.append(float(df.iloc[13,7]))
#     allocation_petcoke.append(float(df.iloc[16,7]))
#     allocation_gas.append(float(df.iloc[14,7]))
#     Field_name.append(('-'.join(file.split('-')[1:-1])).strip())
#     original_file.append((file.split('-')[0]))

# allocation = pd.DataFrame({'Field_name':Field_name,'original_file':original_file,
#                    'allocation_crude(mmbtu/d)':allocation_crude,'allocation_NGL(mmbtu/d)':allocation_NGL,
#                    'allocation_petcoke(mmbtu/d)':allocation_petcoke, 'allocation_gas(mmbtu/d)':allocation_gas})

# merge results, energy summary, flaring, vff, allocation tabs
#results_ES_ch4_co2_allo = pd.merge(results_ES_ch4_co2,allocation,left_on=['original_file','Field name'],right_on=['original_file','Field_name'],how='left')

print('Extract data from flow sheet.csv files...')
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
for file in list_FS:
    df = pd.read_csv(d+file,header=None)
    FS_LPG_export_LPG.append(float(df.iloc[8,22]))
    FS_LPG_export_C2.append(float(df.iloc[16,22]))
    FS_LPG_export_C3.append(float(df.iloc[17,22]))
    FS_LPG_export_C4.append(float(df.iloc[18,22]))
    FS_Ethane_to_Petchem.append(float(df.iloc[16,93]))
    FS_Petcoke_to_stock.append(float(df.iloc[6,214]))
    FS_Gas_at_Wellhead.append(float(df.iloc[23,31]))
    Field_name.append(('-'.join(file.split('-')[1:-1])).strip())
    original_file.append((file.split('-')[0]))

flowsheet = pd.DataFrame({'Field_name':Field_name,'original_file':original_file,
                   'FS_LPG_export_LPG(t/d)':FS_LPG_export_LPG,'FS_LPG_export_C2(t/d)':FS_LPG_export_C2,
                   'FS_LPG_export_C3(t/d)': FS_LPG_export_C3, 'FS_LPG_export_C4(t/d)':FS_LPG_export_C4,
                          'FS_Ethane_to_Petchem(t/d)':FS_Ethane_to_Petchem,
                         'FS_Petcoke_to_stock(t/d)':FS_Petcoke_to_stock,'FS_Gas_at_Wellhead(t/d)':FS_Gas_at_Wellhead})

results_ES_ch4_co2_fs =results_ES_ch4_co2.merge(flowsheet,how='outer',indicator = True)
if results_ES_ch4_co2_fs[results_ES_ch4_co2_fs['_merge']!='both'].shape[0]>0:
    print('Unmatched Field: results_ES_ch4_co2 // flowsheet. Check the merge')
else:
    results_ES_ch4_co2_fs.drop(columns = '_merge',inplace = True)

results_ES_ch4_co2_fs['GWP']='100yr'
results_ES_ch4_co2_fs.to_excel(sp_dir + '/Upstream/Analytics/all_upstream_results.xlsx',index = False)

print('Update upstream results in OCI database...')
import sqlite3
connection = sqlite3.connect(sp_dir + "/OCI_Database.db")
results_ES_ch4_co2_fs.to_sql('upstream_results', connection, if_exists='replace', index=False)
print('Upstream data updates completed.')