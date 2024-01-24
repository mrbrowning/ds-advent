Kafka Challenge
===============

This one is incredibly straightforward: rather than keep topic-host mappings in a config store like kv-lin, use consistent hashing (or rather, rendezvous hashing with k = 1) to deterministically map topics to hosts in a coordination-free manner, and then each node proxies whichever requests or portions of requests that it doesn't own to the host that does, and replies to the external client once it's assembled all the neighbors' replies. Strictly speaking, consistent hashing doesn't buy us much here in the absence of partitions or the need to rebalance on cluster membership changes, but it's very simple to implement and using a naive mod hash feels bad. This approach seems to work pretty well:

```
 :net {:all {:send-count 64438,
             :recv-count 64438,
             :msg-count 64438,
             :msgs-per-op 3.8351386},
       :clients {:send-count 41436,
                 :recv-count 41436,
                 :msg-count 41436},
       :servers {:send-count 23002,
                 :recv-count 23002,
                 :msg-count 23002,
                 :msgs-per-op 1.3690037},
       :valid? true},
```

There's a decent amount of repetition in the forwarding logic for all of the message types other than `send`, in terms of determining topic placements on nodes and fanning out/waiting on requests, but not yet enough that trying to abstract it felt like a real win. It could be a function that takes closures for modifying/querying the local state, generating the RPC messages to send to other nodes, and merging their responses, but that feels less like a true abstraction and more like an ad hoc rewrite rule. I'll think about it some more.
