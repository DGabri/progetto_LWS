import matplotlib.pyplot as plt
import pandas as pd
import os

# constant to convert satoshis in btc
SATOSHIS_IN_BTC = 100000000
# get current working directory to locate the datasets folder where datasets are kept
current_dir = os.getcwd()

# open all datasets
transactions = pd.read_csv(current_dir + "/datasets/transactions.csv", names=["timestamp", "blockId", "txId", "isCoinbase", "fee"])
inputs       = pd.read_csv(current_dir + "/datasets/inputs.csv", names=["txId", "prevTxId", "prevTxPos"])
outputs      = pd.read_csv(current_dir + "/datasets/outputs.csv", names=["txId", "position", "addressId", "amount", "scriptType"])
mapping      = pd.read_csv(current_dir + "/datasets/mapAddr2Ids8708820.csv", names=["hash", "addressId"])

# count the occurrences of each blockId
tx_per_block_dist = transactions.groupby('blockId').size()

# Create a bar plot
plt.figure(figsize=(10, 6))
plt.plot(tx_per_block_dist.values, color="orange")
plt.xlabel("Block ID")
plt.ylabel("TXs count")
plt.title("Distribution of Txs per Block ID")
plt.show()

# get months list
months = pd.date_range(start="2009-01-01", end="2012-12-31", freq="M")
transactions['timestamp'] = pd.to_datetime(transactions["timestamp"], unit='s')

tx_dist_year = {"2009": [], "2010": [], "2011": [], "2012": []}
months_plot_label = ["Jan-Feb", "Mar-Apr", "May-Jun", "Jul-Aug", "Sept-Oct", "Nov-Dec"]

# iterate months and calculate mean block occupation
for i in range(0, len(months) - 1, 2):
    year = str(months[i].year)
    two_months_window = transactions[transactions["timestamp"].between(months[i], months[i+1])]
    two_months_window
    bimonthly_mean = two_months_window.groupby("blockId").size().mean()
    tx_dist_year[year].append(bimonthly_mean)

# Plot the data log scale
plt.figure(figsize=(10, 6))
plt.plot(months_plot_label, tx_dist_year["2009"], color="blue", label="2009")
plt.plot(months_plot_label, tx_dist_year["2010"], color="red", label="2010")
plt.plot(months_plot_label, tx_dist_year["2011"], color="purple", label="2011")
plt.plot(months_plot_label, tx_dist_year["2012"], color="green", label="2012")
plt.xlabel("Bimester")
plt.ylabel("TXs count")
plt.yscale("log")
plt.title("Bimonthly Distribution of Txs per Block")
plt.legend()
plt.show()

# decimal scale
plt.figure(figsize=(10, 6))
plt.plot(months_plot_label, tx_dist_year["2009"], color="blue", label="2009")
plt.plot(months_plot_label, tx_dist_year["2010"], color="red", label="2010")
plt.plot(months_plot_label, tx_dist_year["2011"], color="purple", label="2011")
plt.plot(months_plot_label, tx_dist_year["2012"], color="green", label="2012")
plt.xlabel("Bimester")
plt.ylabel("TXs count")
plt.title("Bimonthly Distribution of Txs per Block")
plt.legend()
plt.show()

# merge inputs and outputs to calculate utxo
merged = inputs.merge(outputs, left_on=["prevTxId", "prevTxPos"], right_on=["txId", "position"], how="inner")

# total output qty
total_outputs_btc_qty = outputs.amount.sum()
# total spent out qty
total_spent_btc_qty = merged.amount.sum()

# utxo is delta output and spent
utxo = total_outputs_btc_qty - total_spent_btc_qty

print(f"UTXO (satoshi): {utxo}")
print(f"UTXO (btc): {utxo/SATOSHIS_IN_BTC:.3f}")

spent_btc = transactions.merge(inputs, on="txId")
spent_btc.rename(columns={"timestamp": "ts_spent_btc"}, inplace=True)

created_btc = transactions.merge(outputs, on="txId")
created_btc.rename(columns={"timestamp": "ts_created_btc", "txId": "outputTxId"}, inplace=True)

time_delta = spent_btc.merge(created_btc, how="inner", left_on=["prevTxId", "prevTxPos"], right_on=["outputTxId", "position"])

# calculate time delta
time_delta["ts_delta"] = (time_delta.ts_spent_btc - time_delta.ts_created_btc).dt.days
# filter rows where delta >= 0
time_delta = time_delta[time_delta["ts_delta"] >= 0]

df = time_delta.groupby("ts_delta")["prevTxId"].count()

# no log
plt.figure(figsize=(10, 6))
plt.plot(df.index, df.values, color="orange")
plt.title("Time delta between spent and creation tx")
plt.ylabel("Outputs Count")
plt.xlabel("Days Difference")
plt.show()

#log scale 
plt.figure(figsize=(10, 6))
plt.plot(df.index, df.values, color="orange")
plt.yscale("log")
plt.title("Time delta between spent and creation tx")
plt.ylabel("Outputs Count")
plt.xlabel("Days Difference")
plt.show()

# get only these 4 columns and drop duplicates of the same block as the fee is the same for each line, keep the first
fees_df = transactions[["timestamp", "blockId", "txId","fee"]].drop_duplicates(subset="blockId", keep="first")
fees_df["timestamp"] = pd.to_datetime(fees_df["timestamp"], unit="s")

plt.figure(figsize=(10, 6))
plt.plot(fees_df.timestamp, fees_df.fee, color="orange")

# Title and xlabel
plt.title("Block Fees Since Genesis")
plt.ylabel("Fee Amount (btc)")
plt.xlabel("Timestamp")
plt.show()

fees_df["btc_fee"] = fees_df["fee"]/SATOSHIS_IN_BTC
plt.figure(figsize=(10, 6))
plt.hist(fees_df.btc_fee, color="orange")
plt.title("Fee amount distribution")
plt.yscale("log")
plt.ylabel("Count")
plt.xlabel("Fee Amount (btc)")
plt.show()

# convert series to dataframe, or join will fail
tx_per_block_dist = tx_per_block_dist.to_frame()
# create a df to store block count and fee value per block
fees_tx_dist = tx_per_block_dist.merge(fees_df, how="inner", on="blockId")

# fees dipendono dal numero di tx in blocco?
plt.figure(figsize=(10, 6))
plt.scatter(fees_tx_dist.blockId, fees_tx_dist.fee, color="orange")
plt.title("Fee correlation to tx/block")
plt.ylabel("Fee Amount (btc)")
plt.xlabel("TXs per block")
plt.show()

import networkx as nx

# number of inputs for each txId (look at prevTxId duplicates)
input_tx_merged = transactions.merge(inputs, how="inner", on="txId")
inputs_count_per_tx = input_tx_merged.groupby("txId")["prevTxId"].count()

# filter inputs > 1 being the real multi input txId
effective_multi_input = inputs.loc[inputs["txId"].isin(inputs_count_per_tx.loc[inputs_count_per_tx.values > 1].index)]

# merge multi inputs with outputs, leave txId, addressId and amount per tx
in_out_merged = effective_multi_input.merge(outputs, left_on=["prevTxId", "prevTxPos"], right_on=["txId", "position"], how="inner")

in_out_merged.rename(columns={"txId_x": "txId"}, inplace=True)
# drop columns that are not used
in_out_merged.drop(['txId_y', "position", "prevTxId", "prevTxPos", "amount", "scriptType"], axis=1, inplace=True)


multi_inputs = in_out_merged.groupby("txId")['addressId']


# create direct graph
tx_graph = nx.DiGraph()

#use addressId else address is too big 
tx_graph.add_nodes_from(mapping["addressId"])

# iterate each group (txId) and add edge for each address that took place in the transaction
for txId, addresses in multi_inputs:
    # get source addess
    src_addr = addresses.iloc[0]

    # get destination addresses
    for dst_addr in addresses:

            if (not (src_addr == dst_addr)):
                tx_graph.add_edge(src_addr, dst_addr)

# %%
weakly_conn_components = nx.weakly_connected_components(tx_graph)
weakly_conn_components = list(weakly_conn_components)
"""per ogni txId mettere un arco verso tutti gli addressId che collega"""

# take top 10 clusters by size
clusters_size = [len(cluster) for cluster in weakly_conn_components]
ordered_clusters = sorted(weakly_conn_components, key = len, reverse = True)
sorted_clusters_size = [len(cluster) for cluster in ordered_clusters]
top10_clusters = ordered_clusters[:10]

for i in range(0,10):
    print(f"Clusters {i+1} size: {sorted_clusters_size[i]}")

#dimensione media, minima e massima dei cluster, distribuzione delle loro dimensioni
clusters_count = len(sorted_clusters_size)
mean_cluster_size = sum(sorted_clusters_size)/clusters_count
min_cluster_size = sorted_clusters_size[-1]
max_cluster_size = sorted_clusters_size[0]

# print results 
print("\n")
print(f"Numero clusters: {clusters_count}")
print(f"Nodi medi per cluster: {mean_cluster_size:.3}")
print(f"Nodi nel cluster piu' grande: {max_cluster_size}")
print(f"Nodi nel cluster piu' piccolo: {min_cluster_size}")

x_ticks = [i for i in range(1,11)]
plt.figure(figsize=(10, 6))
plt.bar(x_ticks, sorted_clusters_size[:10], color="orange")
plt.title("Top 10 clusters size")
plt.ylabel("Nodes count/cluster")
plt.xticks(x_ticks)
plt.xlabel("Cluster")
plt.show()

# clusters size distribution
plt.figure(figsize=(10, 6))
plt.plot(clusters_size, color="orange")
plt.title("Cluster size distribution")
plt.ylabel("Nodes count/cluster")
plt.xlabel("Cluster")
plt.yscale("log")
plt.show()

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
import requests
import time
import bs4

# UBUNTU SERVER
#from selenium.webdriver.chrome.options import Options
#from selenium.webdriver.chrome.service import Service
#from webdriver_manager.chrome import ChromeDriverManager
 
import json

#options = Options()
#options.add_argument('--headless')
#options.add_argument('--no-sandbox')
#options.add_argument('--disable-dev-shm-usage')
#driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver = webdriver.Chrome()

# WALLET EXPLORER SCRAPER
header = {"User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}

# dicts to save clusters possible owners
wallet_expl_entities = {}

# WALLET EXPLORER SCRAPER

def get_wallet_expl(address):
    # wallet explorer url f string with address added
    wallet_expl_url = f"https://www.walletexplorer.com/?q={address}"
    retries = 0
    while (retries <= 5):
        retries += 1
        try:
            res = requests.get(wallet_expl_url, headers=header)

            if (res.status_code == 200):
                # read html with beautiful soup
                soup = bs4.BeautifulSoup(res.text, "html.parser")
                entity = soup.find("h2").text
                
                if (entity):
                    # entity is known if wallet is not inside [*]
                    if (not('[' in entity)):
                        return entity.split()[1]
        except:
            pass

        time.sleep(2)
    
    return None

# function to scrape all clusters
def scrape_wallet_expl(clusters):
    
    i = 0
    retries = 0
    for cluster in clusters:

        i+=1
        cluster_name = f"Cluster{i}"
        wallet_expl_entities[cluster_name] = "Not Found"

        for address_id in cluster:
            retries += 1
            if (retries == 30):
                retries = 0
                break

            addr_hash = mapping.loc[mapping['addressId'] == address_id]["hash"].values[0]
            # set owner to None, modify it if found
            wallet_owner = get_wallet_expl(addr_hash)
            
            # wallet owner found, break cycle, save to dict
            if (wallet_owner):
                wallet_expl_entities[cluster_name] = wallet_owner
                break

        print(f"{cluster_name}: {wallet_owner}")

scrape_wallet_expl(top10_clusters)

with open('wallet_expl.json', 'w') as f:
    data = json.dumps(wallet_expl_entities, indent = 1)
    f.write(data)

# BIT INFO CHARTS SCRAPER
bitinfo_entities = {}

def get_bitInfoCharts(address):
    # bitcoin info charts url f string with address added
    bitinfo_url = f"https://bitinfocharts.com/bitcoin/address/{address}"
    retries = 0

    while (retries <= 3):
        retries += 1
        # navigate to bitcoinInfoCharts
        driver.get(bitinfo_url)

        try:
            # condition is "wallet: " is in page
            wallet_condition = (By.XPATH, '//*[contains(text(), "wallet: ")]')

            # wait for "wallet: " to load on page
            wallet = WebDriverWait(driver, 10).until(EC.presence_of_element_located(wallet_condition)).text
            # Parse the value as needed
            if "wallet: " in wallet:
                entity = wallet.split(": ")[1]
                return entity

        except:
            pass

        time.sleep(2)
    
    return None

def scrape_bitInfo(clusters):

    i = 0
    for cluster in clusters:
        i+=1
        cluster_name = f"Cluster{i}"

        for address_id in cluster:
            addr_hash = mapping.loc[mapping['addressId'] == address_id]["hash"].values[0]
            # set owner to None, modify it if found
            bitinfo_entities[cluster_name] = "Not Found"
            wallet_owner = get_bitInfoCharts(addr_hash)

            # wallet owner found, break cycle, save to dict
            if ((wallet_owner != None) and (not wallet_owner.isnumeric())):
                bitinfo_entities[cluster_name] = wallet_owner
                break
            else:
                break

        print(f"{cluster_name}: {wallet_owner}")

scrape_bitInfo(top10_clusters)

with open('bitInfo.json', 'w') as f:
    data = json.dumps(bitinfo_entities, indent = 1)
    f.write(data)
