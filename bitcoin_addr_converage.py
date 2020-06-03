#sample code

import pymongo
from pprint import pprint
import pickle
import json
import time
import os
import datetime
import sys
import logging
import multiprocessing 
from collections import OrderedDict


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s-%(lineno)d: %(message)s")


invalid_addr = '#abnormal_addr'
mapping_addr_index = 0

def wash_data(txitem, mapping_set, reverse_idxs):
    global mapping_addr_index
    tx_idx = txitem['index']
    items = txitem['ins'] + txitem['outs']
    addr_s = set()
    for ele in items:
        addr = ele['addr']
        if addr == invalid_addr:
            continue 
            
        if addr in mapping_set:
            mapping_idx = mapping_set[addr]
        else:
            mapping_addr_index += 1
            mapping_set[addr] = mapping_addr_index
            mapping_idx = mapping_addr_index
            
        if mapping_idx in reverse_idxs:
            reverse_idxs[mapping_idx].add(tx_idx)   
        else:
            reverse_idxs[mapping_idx] = set([tx_idx])
        addr_s.add(mapping_idx)
    return tx_idx, addr_s
    

def produce_data():
    mapping_idx_addr = {}
    reverse_idxs = {}
    mapping_txidx_addr_set = {}
    for idx, txitem in enumerate(items):
        tx_idx, addr_s = wash_data(txitem, mapping_idx_addr, reverse_idxs)
        if addr_s:
            mapping_txidx_addr_set[tx_idx] = addr_s
    logging.info("wash done, washed data len:%s ", idx+1)
    logging.info("total addr count  %s",mapping_addr_index)
    ordered_reverse_idxs = OrderedDict(sorted(reverse_idxs.items(), key=lambda x: len(x[1])))
    return ordered_reverse_idxs, mapping_txidx_addr_set


def clean(reverse_idxs, chose_indexs, mapping_txidx_addr_set):
    
    chosed_addr_s = set()
    for idx in chose_indexs:
        chosed_addr_s.update(mapping_txidx_addr_set[idx])
    
    for addr in chosed_addr_s:
        if addr in reverse_idxs:
            reverse_idxs.pop(addr)
        
#     logging.info("del %s done", len(chosed_addr_s))
            

def make_choice(tx_idx_set, reverse_idxs):
    temp = {k:0 for k in tx_idx_set}
    for addr in reverse_idxs:
        s = reverse_idxs[addr]
        if not s:
            continue
        for a in (tx_idx_set & s):
            temp[a] = temp[a] + 1
    chose = None
    guard = 0
    for k in temp:
        c = temp[k]
        if c > guard:
            chose = k
            guard = c
    return chose
        

def query_coverage_index(reverse_idxs, mapping_txidx_addr_set):
    chose_indexs = set()
    run_round = 0
    while reverse_idxs:
#         logging.info("run_round:%s, left:%s", run_round, len(reverse_idxs))
        run_round += 1
        picked = False
        new_choice_set = set()
        for addr in reverse_idxs:
            tx_idx_set = reverse_idxs[addr]
            if len(tx_idx_set) == 1:
                tx_idx = tx_idx_set.pop()
                picked = True
                chose_indexs.add(tx_idx)
                new_choice_set.add(tx_idx)
        
#         logging.info("select 1 done; chose_indexs %s, left:%s", len(chose_indexs), len(reverse_idxs)) 
        
        if picked:
            clean(reverse_idxs, new_choice_set, mapping_txidx_addr_set)
#             logging.info("clean done; chose_indexs %s, left:%s", len(chose_indexs), len(reverse_idxs))
            
        if not picked:
            for addr in reverse_idxs:
                tx_idx_set = reverse_idxs[addr]
                if not tx_idx_set :
                    continue
                tx_idx = make_choice(tx_idx_set, reverse_idxs)
                chose_indexs.add(tx_idx)
                reverse_idxs[addr] = set()
                break
            clean(reverse_idxs, set([tx_idx]), mapping_txidx_addr_set)
#             logging.info("chose done; chose_indexs: %s, left:%s", len(chose_indexs), len(reverse_idxs)) 
            
    return chose_indexs

def get_data_from_db():
    logging.info("starting.....")

    myclient = pymongo.MongoClient('mongodb://smy-bsci-db:27017/')
    mydb = myclient['btcscan']
    mydb.authenticate("btcsaner", "SmUKLLc7dhn84GHkYso4cbh3SXUCV5")
    txscol = mydb['txs']
    logging.info("connected to mongo.....")
    # batch_txes = txscol.find({"block_height": {'$gte': 590000, '$lte': 593492}},{ '_id': 0})
    #
    #

    height_start = 590000
    height_diff = 10
    batch_txes = txscol.find({"block_height": {'$gte': height_start, '$lte': height_start + height_diff}},{ '_id': 0})


    logging.info("ready to read data.....")
    logging.info('height diff: %s', height_diff)

    items = []
    for txitem in batch_txes:
        items.append(txitem)
    logging.info("got from db, data len %s", len(items))
    return items


st = time.time()

items = get_data_from_db()
reverse_idxs, mapping_txidx_addr_set = produce_data()
chosed_idxs = query_coverage_index(reverse_idxs, mapping_txidx_addr_set)


with open('./min_txes_set.pkl', 'wb') as w:
    pickle.dump(chosed_idxs, w)

addr_sets = set()
for idx in chosed_idxs:
    addr_sets.update(mapping_txidx_addr_set[idx])
    
    
et = time.time()
logging.info("done.")

logging.info('total_choice_index: %s', len(chosed_idxs))
logging.info('before: %s, after:%s', mapping_addr_index, len(addr_sets))
logging.info("total time cost:%s", et-st)
