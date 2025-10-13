# address-index

Yet another blockchain indexer.

## Capability goals
To build an index that can:
- provide current list of UTXOs of any given locker script
  - this will allow consumers to be able to build transactions
- provide UTXO balances of any given locker script at any given block height
  - this will allow consumers to be able to know balances historically
- rewinding scanned blocks down to any target height
  - this will make the indexer reorg-proof by simply dropping recent blocks back to the fork then rescanning

## Delivery goals
To expose the data:
- via a readonly HTTP API using GraphQL
- by emitting events for async consumers

