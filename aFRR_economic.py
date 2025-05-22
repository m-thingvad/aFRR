#%% libs
import pandas as pd
import requests
# Capacity data 
def capacity_data(grid_zone):
    url=f'https://api.energidataservice.dk/dataset/AfrrReservesNordic?start={time_start}&end={time_end}&columns=HourDK,aFRR_UpCapPriceEUR&filter={{"PriceArea":["{grid_zone}"]}}'
    response = requests.get(url).json()['records'] # Call endpoint
    df = pd.DataFrame(response)        # Create dataframe of respons
    df['HourDK'] = pd.to_datetime(df['HourDK']) # Make datetime and change timezone
    df.set_index('HourDK', inplace=True)  # Set index
    df = df.resample("15T").ffill()
    df.rename(columns={'aFRR_UpCapPriceEUR': 'capacity_price'}, inplace=True)
    return df
# EAM data
def EAM_data(grid_zone):
    url = f'https://api.energidataservice.dk/dataset/AfrrEnergyActivated?start={time_start}&end={time_end}&columns=ActivationTime, aFRR_UpActivatedPriceEUR&filter={{"PriceArea":["{grid_zone}"]}}' 
    response = requests.get(url).json()['records'] # Call endpoint
    df = pd.DataFrame(response) # Dataframe of response
    df['HourDK'] = pd.to_datetime(df['ActivationTime']) # Make datetime 
    df.set_index('HourDK', inplace=True)  # Set index
    df.index = df.index.tz_localize('UTC').tz_convert('CET').tz_localize(None) # Make timezone aware and change timezoned
    df = df[['aFRR_UpActivatedPriceEUR']].fillna(0)  # Filter and fill
    df_filtered = df[df.aFRR_UpActivatedPriceEUR > marginal_price] # Filter over for marginal price
    # EAM price
    EAM_price = df_filtered.resample('15T').mean().fillna(0) # Weighted average per 15 min
    EAM_price.rename(columns={'aFRR_UpActivatedPriceEUR': 'EAM_price'}, inplace=True)
    # EAM per hour activation per 15 min
    EAM_activation = df_filtered.resample('15T').count().fillna(0)/(60*60) # Per hour activation per 15 min
    EAM_activation.rename(columns={'aFRR_UpActivatedPriceEUR': 'EAM_activation'}, inplace=True)
    return EAM_price, EAM_activation
# Spot + Imbalance data
def spot_imbalance_data(grid_zone):
    url= f'https://api.energidataservice.dk/dataset/ImbalancePrice?start={time_start}&end={time_end}&columns=TimeDK,SpotPriceEUR,ImbalancePriceEUR&filter={{"PriceArea":["{grid_zone}"]}}'
    response = requests.get(url).json()['records'] # Call endpoint
    df = pd.DataFrame(response) # Dataframe of response
    df['TimeDK'] = pd.to_datetime(df['TimeDK']) # Make datetime and change timezone
    df.set_index('TimeDK', inplace=True)  # Set index
    df.rename(columns={'SpotPriceEUR': 'spot_price','ImbalancePriceEUR':'imbalance_price'}, inplace=True)
    df["spot_delta"] = df['imbalance_price'] - df['spot_price']
    return df
# Return single dataframe 
def aFRR_data(grid_zone):
    capacity_price = capacity_data(grid_zone=grid_zone)
    EAM_price, EAM_activation = EAM_data(grid_zone=grid_zone)
    spot_imbalance_price = spot_imbalance_data(grid_zone=grid_zone)
    df = pd.concat([capacity_price, EAM_price, EAM_activation, spot_imbalance_price], axis=1)
    return df

#%%
# Parameters
time_start = "2025-04-01T00:00"
time_end   = "2025-05-01T00:00"
marginal_price = 500
df = aFRR_data(grid_zone="DK1") # Get energy data
capacity_revenue = df['capacity_price'].resample('H').mean().sum()
EAM_revenue = (df['EAM_activation']*df["EAM_price"]).sum()
total_revenue = capacity_revenue+capacity_revenue
HGT_share = total_revenue*0.2
HGT_imbalance = (df['EAM_activation']*df["imbalance_price"]).sum()
BRP_imbalance = (df['EAM_activation']*df['spot_delta']).sum()
netto_revenue = total_revenue-HGT_share-HGT_imbalance-BRP_imbalance
