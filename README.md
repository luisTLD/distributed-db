# Banco de Dados Distribuído (chave–valor) com gRPC, 2PC e Eleição de Líder

Banco de dados chave–valor replicado em vários nós, escrito em Python com gRPC.
As escritas são coordenadas por um líder eleito automaticamente e aplicadas de
forma atômica em todas as réplicas vivas pelo protocolo **Two-Phase Commit (2PC)**.
O sistema tolera falhas: detecta nós mortos por *heartbeat*, reelege o líder pelo
algoritmo **Bully**, recupera estado por **WAL** e por transferência de estado, e
resolve transações pendentes com um **protocolo de terminação**. Usa **quórum
majoritário** contra *split-brain* e **relógios de Lamport** para ordenar eventos.

Todos os algoritmos distribuídos foram implementados manualmente; o gRPC é usado
apenas como transporte. O cluster roda tanto **em várias máquinas de uma rede
local** quanto **simulado em um único computador** (vários processos em portas
diferentes), sem mudança de código.

Projeto acadêmico de Computação Distribuída (PUC Minas), tema "Sistema de Banco de
Dados Distribuído com replicação e consistência".

---

## O que está implementado

- **Comunicação RPC** com gRPC entre cliente e nós e entre os próprios nós.
- **Relógios lógicos de Lamport** carimbando todas as mensagens (ordem causal).
- **Two-Phase Commit** para escritas atômicas e replicadas (tudo ou nada).
- **Exclusão mútua** em dois níveis: serialização por chave no líder e *lock* por
  chave em cada participante durante o 2PC.
- **Eleição de líder (Bully)** com salvaguarda que impede um nó defasado de
  regredir o estado do banco.
- **Tratamento de falhas:** detecção por *heartbeat*, reeleição automática, *retry*
  de escrita excluindo réplicas mortas, WAL com `fsync` para recuperação após
  queda, transferência de estado (`SyncState`) no reingresso e protocolo de
  terminação cooperativa do 2PC.
- **Controle de réplicas e quórum majoritário (N/2+1)** contra *split-brain*.
- **Topologia configurável** por linha de comando (`--peers`): mesma base roda em
  1 máquina (loopback) ou em N máquinas da LAN.
- **Bateria de testes**, **benchmark** (distribuído × máquina única) e **teste de
  estresse** com verificação de consistência entre os nós.

---

## Como funciona

São N nós idênticos (os experimentos usam N = 3) e clientes. Todo nó roda o mesmo
programa; o papel (líder ou réplica) é decidido em tempo de execução pela eleição.
A topologia é uma malha completa: todo nó fala diretamente com todos.

```
            CLIENTE  (escrita -> líder | leitura -> qualquer nó)
                |
                v
         NÓ LÍDER (maior id vivo)   coordena o 2PC
           /                  \
   Prepare/Commit         Prepare/Commit
          v                      v
     RÉPLICA                RÉPLICA
     (vota no 2PC)          (vota no 2PC)

  Em paralelo, todos trocam: Heartbeat, Election/Announce e QueryDecision.
```

- **Escrita:** o cliente envia ao líder (se errar o nó, recebe `NOT_LEADER` e é
  redirecionado). O líder roda o 2PC sobre o *cohort* de réplicas vivas. Fase 1
  (Prepare): cada nó trava a chave, valida, grava a intenção no WAL e vota. Fase 2
  (Commit/Abort): se todos votaram SIM, aplica em todos atomicamente; senão,
  aborta em todos. Antes de tudo há checagem de quórum (N/2+1).
- **Leitura:** atendida localmente por qualquer nó, sem 2PC (barata).
- **Falha do líder:** as réplicas percebem o silêncio do *heartbeat* (timeout de
  3s), rodam o Bully e elegem o maior id vivo; o cliente é redirecionado sozinho.
- **Durabilidade:** WAL forçado ao disco (`fsync`) antes de alterar a memória; no
  restart, só transações com COMMIT são reaplicadas.
- **Coordenador morre no 2PC:** após 8s, o participante "em dúvida" pergunta aos
  pares a decisão (`QueryDecision`); se alguém viu COMMIT, confirma; senão, aborta
  por presunção (*presumed abort*).

A descrição completa, arquivo por arquivo e com o código, está no relatório
(`Relatorio/relatorio.pdf`).

---

## Stack

Python 3.11.9, gRPC e Protocol Buffers. Dependências em `requirements.txt`
(`grpcio`, `grpcio-tools`, `protobuf`). Sem banco externo: o armazenamento é um
dicionário em memória com WAL e *snapshots* em disco, implementados no projeto.

---

## Como rodar (mesma máquina, 3 nós)

A partir da raiz do projeto:

```bat
:: Windows
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts\generate_protos.py
scripts\run_cluster.bat
python -m distdb.client --demo
```

```bash
# Linux / macOS
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_protos.py
bash scripts/run_cluster.sh
python -m distdb.client --demo
bash scripts/stop_cluster.sh
```

Validar a lógica (sem rede, sem cluster no ar):
```bash
python tests/run_all.py        # imprime ALL TESTS PASSED
```

Coletar todos os logs de uma vez (testes, cluster, demo, benchmark, estresse):
```bat
scripts\collect_logs.bat       :: Windows  -> logs\coleta\TUDO.txt
bash scripts/collect_logs.sh   #  Linux/macOS -> logs/coleta/
```

## Como rodar (entre máquinas, rede local)

A mesma lista `--peers` em todas as máquinas; muda só o `--id`. Libere a porta no
firewall de cada máquina.

```bash
# máquina A (192.168.0.10):
python -m distdb.node --id 1 --peers "1=192.168.0.10:50051,2=192.168.0.11:50051,3=192.168.0.12:50051"
# máquina B: o mesmo, com --id 2 ; máquina C: --id 3
# cliente (de qualquer máquina): mesma lista --peers
```

Como as escritas exigem quórum majoritário, com 3 nós o sistema tolera a queda de
1 e continua escrevendo.

---

## Exemplo

Demonstração do cliente:
```
leader = node 3 (127.0.0.1:50053)
put user Luis       -> PUT committed (cohort=3)
update user Carlos  -> UPDATE committed (cohort=3)
update missing X    -> UPDATE aborted: node 3: key 'missing' does not exist
delete lang         -> DELETE committed (cohort=3)
```

Logs dos nós durante uma escrita (2PC em ação):
```
[node 3][LEADER]  2PC COMMIT tx=61efc933efa3 PUT user='Luis' votes=3/3 failed=[]
[node 1][REPLICA] PREPARE tx=61efc933efa3 PUT user -> YES (prepared)
[node 1][REPLICA] COMMIT tx=61efc933efa3 applied=True
```

Queda do líder com eleição automática:
```
[node 1][REPLICA] leader 3 appears down -> starting election
[node 2][LEADER]  won election -> I am the leader
[node 1][REPLICA] synced 2 keys from leader 2
[node 2][LEADER]  2PC COMMIT tx=c7949fac692b PUT c='3' votes=2/2 failed=[]
```

Mais saídas reais em `logs-exemplo/` (testes, demo, benchmark e estresse).

---

## Resultados

Medidos com os 3 nós e os clientes no mesmo computador (gRPC/TCP via loopback).

| Cenário | Escritas (ops/s) | Leituras (ops/s) |
|---|---:|---:|
| Máquina única (sem rede) | 471.698 | 3.636.359 |
| Distribuído, 3 nós (2PC) | 173,3 | 2.688,7 |
| Estresse: 8 clientes (carga mista) | 542,8 | 232,7 |

A diferença de mais de três ordens de grandeza nas escritas é o preço da
durabilidade e da replicação síncrona: cada escrita faz duas rodadas de RPC
(votação e decisão) e força o WAL ao disco em cada um dos 3 participantes,
enquanto o baseline só atualiza um dicionário em memória. As leituras pagam só uma
RPC local. No teste de estresse (8 clientes, 800 operações) o cluster concluiu 560
escritas confirmadas, sem nenhum aborto e nenhuma falha, e ao final os 3 nós
ficaram com dados idênticos (594 chaves).

---

## Testes

Rodam sem rede e sem `pytest`, exercitando a lógica distribuída em memória:

```bash
python tests/run_all.py
```

Cobrem o relógio de Lamport, o armazém (commit/abort, *locks*, recuperação por
WAL), o 2PC (atomicidade, abort, quórum, terminação), a eleição Bully com o
detector de falhas e um cenário de integração ponta a ponta (escrita → queda de
réplica → queda do líder → eleição → ressincronização).

---

## Estrutura

```
distributed-db/
├── proto/database.proto        # contrato gRPC (serviço e mensagens)
├── distdb/                     # pacote principal
│   ├── lamport.py              # relógio lógico de Lamport
│   ├── store.py                # armazém chave-valor com WAL/snapshot e locks
│   ├── coordinator.py          # coordenador 2PC e regra de terminação
│   ├── election.py             # algoritmo Bully
│   ├── failure_detector.py     # detecção de falhas por heartbeat
│   ├── config.py               # topologia do cluster (--peers)
│   ├── node.py                 # servidor gRPC e threads de fundo
│   ├── client.py               # cliente de linha de comando
│   └── generated/              # stubs gerados do .proto
├── scripts/                    # generate_protos, run_cluster, collect_logs, benchmark, stress_test
├── tests/                      # testes de unidade e integração
├── Relatorio/                  # relatório (PDF + LaTeX)
├── logs-exemplo/               # amostra de logs de uma execução real
└── requirements.txt
```

---

## Autores

Edson Pimenta de Almeida, Felipe Carvalho de Paula Silva, João Lucas de Melo
Quintão, Luís Augusto Starling Toledo, Juan Pablo Ramos de Oliveira, Luiz Gabriel
Milione Assis e Túlio Gomes Braga. PUC Minas, Instituto de Ciências Exatas e
Informática.
