# Requirements:
# sudo pip install base58
# sudo pip install pyinstaller
# sudo pip install prettytable

from python_graphql_client import GraphqlClient
from string import Template
import logging
import requests
import sys
import base58
from prettytable import PrettyTable
import argparse
import math

#print(abis.GRAPH_REWARDS_ABI)
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)

ENDPOINT_ARBITRUM = 'https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-arbitrum'
ENDPOINT_ETHEREUM = 'https://api.thegraph.com/subgraphs/name/graphprotocol/graph-network-mainnet'
endpoint = {'a': ENDPOINT_ARBITRUM, 'e': ENDPOINT_ETHEREUM}
network = {'a': 'arbitrum-one', 'e': 'mainnet'}
indexers = {'a': {'id1': '0xfc842f81490dcb37e82d416b2d28327dfb24ba9a', 'id2': '0x0058223c6617cca7ce76fc929ec9724cd43d4542'},
            'e': {'id1': '0x45874192929530cd4e3dd0624df05bee3c13974f', 'id2': '0x720a98087160bfdb282f695abe6f9ac966b03d43'}
            }
sorting = {'k': 'k', 's': 'signaled'}

TOKEN = 1000000000000000000
MIN_SIGNAL = 10000
TOP_N = 10 
TOKEN_1M = 1000000
ISSUANCE = 821917.808219178
R_SHARE = {'a': 0.25, 'e': 0.75}
def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--indexer',
        dest='indexer',
        help='either id1 or id2',
        type=str,
        required=True)
    parser.add_argument('-n', '--network',
        dest='network',
        help='either a or e',
        type=str,
        required=True)
    parser.add_argument('-ms', '--min-signal',
        dest='min_signal',
        type=int,
        help='minimal signal',
        default=MIN_SIGNAL)
    parser.add_argument('-ma', '--max-allocation',
        dest='max_allocation',
        help='maximum alocations',
        type=int,
        default=TOP_N)
    parser.add_argument('-es', '--exclude-subs',
        dest='exclude_subs',
        help='list of subs to exclude, separated by space',
        nargs='+',
        default='')
    parser.add_argument('-is', '--include-subs',
        dest='include_subs',
        help='list of subs to exclude, separated by space',
        nargs='+',
        default='')
    parser.add_argument('-s', '--sorting',
        dest='srt',
        help='sorting rule either "k" = K or "s" = Signaled, default "k"',
        type=str,
        default = 'k')
    return parser.parse_args()

def parseExcludeSubs(subgraphs):
    s = subgraphs.split()
    print(s)
    return s

def getIPFS(id):
    return base58.b58encode(bytes.fromhex("1220"+id[2:])).decode('utf-8')

def getTotalSignal(endpoint: str):
    client = GraphqlClient(endpoint=endpoint)
    query = """
                query Signal {
                    graphNetworks{
                        totalTokensSignalled
                        }
                    }
                """
    try:
        data = client.execute(query=query)
    except requests.exceptions.RequestException as e:
        logging.error('Can\'t get Signal Data, check endpoint {}'.format(e))
        sys.exit()
        logging.info('Received Signal data: {}'.format(data))
    if data.get('errors'):
        logging.error('Can\'t get Signal, check query {}'.format(data))
        sys.exit()
    if len(data.get('data')) == 0:
        logging.error('Can\'t get Signal, check endpoint {}'.format(e))
        sys.exit()
    # print(int(data['data']['graphNetworks'][0]['totalTokensSignalled'])/TOKEN)
    return int(data['data']['graphNetworks'][0]['totalTokensSignalled'])/TOKEN

def getIndexer(indexer: str, endpoint: str):
    client = GraphqlClient(endpoint=endpoint)
    t = Template("""
                    query Indexer {
                        indexer(id: "$id") {
                            indexingRewardCut
                            delegatedTokens
                            stakedTokens
                        }
                    }
                """)
    query = t.substitute(id=indexer)
    try:
        data = client.execute(query=query)
    except requests.exceptions.RequestException as e:
        logging.error('Can\'t get Indexer Data, check endpoint {}'.format(e))
        sys.exit()
        logging.info('Received Indexer data: {}'.format(data))
    if data.get('errors'):
        logging.error('Can\'t get Indexer, check query {}'.format(data))
        sys.exit()
    if len(data.get('data')) == 0:
        logging.error('Can\'t get Indexer, check endpoint {}'.format(e))
        sys.exit()
    #print(data)
    return data['data']['indexer']

def getSubgraphs(endpoint: str):
    client = GraphqlClient(endpoint=endpoint)
    t = Template("""
                query {
                    subgraphs(where:{currentVersion_not:null}, first: $first, skip: $skip){
                        id
                        metadata {
                        displayName
                        image
                        }
                            currentVersion {
                                id
                                subgraphDeployment {
                                    id
                                    deniedAt
                                    signalAmount
                                    signalledTokens
                                    stakedTokens
                                    indexingRewardAmount
                                    queryFeesAmount
                                }
                            }
                    }
                }
                 """)
    i = 0
    result = []
    while True:
        query = t.substitute(first=1000, skip=1000*i)
        try:
            data = client.execute(query=query)
        except requests.exceptions.RequestException as e:
            logging.error('Can\'t get Indexer Data, check endpoint {}'.format(e))
            sys.exit()
            logging.info('Received Subgraphs data: {}'.format(data))
        #print(len(data['data']['subgraphs']))
        if data.get('errors'):
            break
        # print(data)
        result.extend(data['data']['subgraphs'])
        i = i + 1
    # print(len(result))
    return result

def formatSubgraphs(subgraphs):
    # TODO: remove dubs
    #subs_no_dubs = [i for n, i in enumerate(subgraphs) if i not in subgraphs[n + 1:]]
    # Some subs may have the same deployment ID but subgraph ID is different and name could be different, so it could appear multiple times in the list
    # We remove dublication based on deployment ID, other params signal and stake should be the same
    subs_no_dubs = []
    for n, sub in enumerate(subgraphs):
        s_deployments = [s['currentVersion']['subgraphDeployment'] for s in subgraphs[n+1:]]
        if sub['currentVersion']['subgraphDeployment'] not in s_deployments:
            subs_no_dubs.append(sub)
    formatted = []
    for sub in subs_no_dubs:
        # Some subs returns {'metadata': None}
        name = sub['metadata']['displayName'] if sub['metadata'] else ''
        id = sub['currentVersion']['subgraphDeployment']['id']
        ipfs = getIPFS(sub['currentVersion']['subgraphDeployment']['id'])
        status = True if sub['currentVersion']['subgraphDeployment']['deniedAt'] == 0 else False
        staked = int(sub['currentVersion']['subgraphDeployment']['stakedTokens']) / TOKEN
        signaled = int(sub['currentVersion']['subgraphDeployment']['signalledTokens']) / TOKEN
        k = round(signaled/staked*1000,1) if staked != 0 else 0
        formatted.append({'name': name, 'id': id, 'ipfs': ipfs, 'status': status, 'staked': staked, 'signaled': signaled, 'k': k})
    return formatted 

def filterSortSubgraphs(subgraphs, exclude_subgraphs, include_subgraphs, min_signal, srt):
    new_set = []
    for sub in subgraphs:
        if include_subgraphs:
            if sub['signaled'] > min_signal and sub['status'] and sub['ipfs'] not in exclude_subgraphs and sub['ipfs'] in include_subgraphs:
                new_set.append(sub)
        else:
            if sub['signaled'] > min_signal and sub['status'] and sub['ipfs'] not in exclude_subgraphs and sub['ipfs']:
                new_set.append(sub)
    s = sorted(new_set, key=lambda d: d[srt], reverse=True)
    #print(s)
    return s

def allocationDistribution(subgraphs, indexer_tokens, args):
    protocol_total_signal = getTotalSignal(endpoint[args.network])
    print('protocol_total_signal: ', protocol_total_signal)
    total_signal = 0
    indexer_tokens = int(indexer_tokens['delegatedTokens'])/TOKEN + int(indexer_tokens['stakedTokens'])/TOKEN
    total_stake = indexer_tokens
    for subgraph in subgraphs:
        total_signal += subgraph['signaled']
        total_stake += subgraph['staked']
    print('Stake: ', total_stake)
    print('Signal: ', total_signal)
    proposal_subgraphs = []
    for subgraph in subgraphs:
        signal_weight = subgraph['signaled']/total_signal
        allocation_tokens = signal_weight * total_stake - subgraph['staked']
        subgraph['allocation_tokens'] = allocation_tokens
        subgraph['k_calc'] = round(subgraph['signaled']/(subgraph['staked'] + allocation_tokens) *1000,1) if subgraph['staked'] != 0 else 0
        subgraph['rrpd_per1m'] = ISSUANCE * R_SHARE[args.network] / protocol_total_signal * subgraph['signaled'] / (subgraph['staked'] + allocation_tokens) * TOKEN_1M
        subgraph['rrpd'] = ISSUANCE * R_SHARE[args.network] / protocol_total_signal * subgraph['signaled'] / (subgraph['staked'] + allocation_tokens) * allocation_tokens
        subgraph['weight'] = signal_weight
        proposal_subgraphs.append(subgraph)
    return proposal_subgraphs

def _Print(proposal):
    rrpd = 0
    total_alloc = 0
    t = PrettyTable(['Name', 'ipfs', 'K', 'Staked', 'Signaled', 'Allocation', 'K_Calc', 'RRPD_1M', 'RRPD']) #'Weight',
    for allocation in proposal:
        t.add_row([allocation['name'],
                   allocation['ipfs'],
                   allocation['k'],
                   round(allocation['staked'], 1),
                   round(allocation['signaled'], 1),
                   # allocation['weight'],
                   round(allocation['allocation_tokens'], 1),
                   round(allocation['k_calc'], 1),
                   round(allocation['rrpd_per1m'], 1),
                   round(allocation['rrpd'], 1),
                   ])
        rrpd += round(allocation['rrpd'], 1)
        total_alloc += round(allocation['allocation_tokens'], 1)
        
    t.align="r"
    print(t)
    print('RRPD Total: {} GRT/DAY'.format(rrpd))
    print('Alloc Total: {} GRT \n'.format(total_alloc))

def _PrintCMDs(proposal, network):
    cmd = 'sudo docker exec -ti shell-tools-l2 graph indexer actions -n {network} queue allocate {sub} {amount}'
    print('To allocate: \n')
    for allocation in proposal:
        print(cmd.format(network=network, sub=allocation['ipfs'], amount=math.floor(allocation['allocation_tokens'])))
    print('sudo docker exec -ti shell-tools-l2 graph indexer actions approve queued')

def main():
    args = parseArguments()
    srt = sorting[args.srt]
    min_signal = args.min_signal
    max_allocation = args.max_allocation
    exclude_subgraphs = args.exclude_subs if args.exclude_subs else []
    include_subgraphs = args.include_subs if args.include_subs else []
    indexer_tokens = getIndexer(indexers[args.network][args.indexer], endpoint[args.network])
    print('\n')
    print('Indexer: ', indexers[args.network][args.indexer])
    print('Rewards cut: {}%'.format(indexer_tokens['indexingRewardCut']/10000))
    print('Tokens: ', int(indexer_tokens['delegatedTokens'])/TOKEN + int(indexer_tokens['stakedTokens'])/TOKEN)
    print('Sorted by: ', srt)
    print('Subs to exclude: ', exclude_subgraphs)
    print('Subs to include: ', include_subgraphs)
    subgraphs = getSubgraphs(endpoint[args.network])
    print(f'Total subgraphs count: {len(subgraphs)}')
    #print(subgraphs)
    formatted = formatSubgraphs(subgraphs)
    subgraphs_to_allocate = filterSortSubgraphs(formatted, exclude_subgraphs, include_subgraphs, min_signal, srt)[:max_allocation]
    proposal = allocationDistribution(subgraphs_to_allocate, indexer_tokens, args)
    #for i in proposal:
    #    print(i)
    _Print(proposal)
    _PrintCMDs(proposal, network[args.network])


if __name__ == "__main__":
    main()
