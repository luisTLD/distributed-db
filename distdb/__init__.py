"""Banco de dados chave-valor distribuído (PUC Minas — Computação Distribuída).

Pacote principal do projeto. Os blocos de lógica pura (independentes de
rede — testáveis isoladamente) são:

* distdb.lamport          -- relógio lógico de Lamport
* distdb.store            -- armazém chave-valor durável (WAL + snapshot) + locks
* distdb.coordinator      -- coordenador Two-Phase Commit + terminação
* distdb.election         -- eleição de líder (algoritmo do valentão)
* distdb.failure_detector -- detector de falhas por heartbeat
* distdb.config           -- topologia do cluster

A ligação com o gRPC (servidor + threads) fica em distdb.node e o cliente de
linha de comando em distdb.client.
"""
