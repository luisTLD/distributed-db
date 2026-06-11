"""Distributed key-value database (PUC Minas - Computação Distribuída).

Core, transport-agnostic building blocks live in this package:

* :mod:`distdb.lamport`          -- Lamport logical clock
* :mod:`distdb.store`            -- durable KV store (WAL + snapshot) + key locks
* :mod:`distdb.coordinator`      -- Two-Phase Commit coordinator
* :mod:`distdb.election`         -- Bully leader election
* :mod:`distdb.failure_detector` -- heartbeat-based failure detector
* :mod:`distdb.config`           -- cluster topology

The gRPC wiring (servicer + background threads) lives in :mod:`distdb.node` and
the command-line client in :mod:`distdb.client`.
"""
