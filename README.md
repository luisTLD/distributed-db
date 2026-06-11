# Banco de Dados Distribuído (chave-valor) com gRPC, 2PC e Eleição de Líder

Trabalho Prático de **Computação Distribuída** — PUC Minas.
Tema escolhido: **Sistema de Banco de Dados Distribuído** (opção 7 do enunciado),
um banco chave-valor replicado, com **replicação síncrona**, **consistência via
Two-Phase Commit**, **eleição de líder** e **tratamento de falhas**.

O sistema é um conjunto de nós idênticos que se comunicam por **RPC (gRPC)**.
Um nó atua como **líder** (coordenador das escritas) e os demais como **réplicas**
(participantes). Os papéis são dinâmicos: se o líder cair, os nós sobreviventes
elegem um novo líder automaticamente.

---

## 1. Visão geral do problema

Um banco de dados distribuído precisa manter os **mesmos dados em várias máquinas**
ao mesmo tempo, de forma que:

- uma escrita só seja considerada concluída se for aplicada de forma **atômica**
  em todas as réplicas vivas (ou em nenhuma);
- leituras possam ser atendidas por **qualquer** nó, sempre devolvendo dados
  consistentes;
- a queda de uma réplica — ou até do próprio líder — **não derrube o serviço**.

Nosso sistema implementa um banco **chave-valor** (como `PUT user = Luis`) e
resolve esses três desafios com algoritmos clássicos de sistemas distribuídos,V
implementados manualmente.

---

## 2. Requisitos do enunciado atendidos

### Entrega 1 (base) — pelo menos 2 dos requisitos. Implementamos 4:

| Requisito | Onde está |
|-----------|-----------|
| **RPC (gRPC)** | toda a comunicação entre nós e cliente (`proto/database.proto`, `distdb/node.py`) |
| **Relógios lógicos (Lamport)** | `distdb/lamport.py`, usado em todas as mensagens |
| **Eleição de líder (Bully)** | `distdb/election.py` + `distdb/node.py` |
| **Exclusão mútua / controle de concorrência** | locks por chave durante o 2PC (`distdb/store.py`) |

### Entrega 2 — tratamento de falhas (obrigatório) + requisitos de expansão. Implementamos os 3 opcionais:

| Requisito | Onde está |
|-----------|-----------|
| **Tratamento de falhas** (obrigatório) | heartbeat + detecção de falhas, re-eleição, *retry* de escrita, WAL para recuperação, *state transfer*, **protocolo de terminação do 2PC** (resolve transações em dúvida quando o coordenador cai no meio do protocolo) |
| **Controle de réplicas** | cohort de réplicas vivas, **quórum majoritário (N/2+1)** para escritas (proteção contra *split-brain*) + sincronização de estado ao reingressar (`SyncState`) |
| **Consistência em transações distribuídas (2PC)** | `distdb/coordinator.py` (Two-Phase Commit) |
| **Análise de desempenho** | `scripts/benchmark.py` (distribuído × máquina única) |
| **Alta concorrência** (apontado no feedback da entrega 1) | `scripts/stress_test.py` — N clientes simultâneos, chaves disputadas, falhas injetadas durante a carga e verificação de consistência entre os nós |

---

## 3. Arquitetura

### 3.1 Quem fala com quem

```
                         ┌─────────────────────┐
            (NOT_LEADER  │       CLIENTE        │
             redireciona │  distdb/client.py    │
             p/ o líder) └──────────┬──────────┘
                                    │ escrita (Put/Update/Delete)  -> sempre no LÍDER
                                    │ leitura (Get/List/...)       -> qualquer nó
                                    ▼
                         ┌─────────────────────┐
                         │   NÓ LÍDER (id=3)    │  coordena o 2PC
                         │   porta 50053        │
                         └─────┬─────────┬──────┘
              Prepare/Commit/  │         │  Prepare/Commit/Abort
                Abort (2PC)    ▼         ▼
                ┌──────────────────┐  ┌──────────────────┐
                │  RÉPLICA (id=1)  │  │  RÉPLICA (id=2)  │
                │  porta 50051     │  │  porta 50052     │
                └──────────────────┘  └──────────────────┘

   Em paralelo, TODOS os nós trocam continuamente:
     • Heartbeat   (estou vivo? quem é o líder?)
     • Election / Announce  (quando o líder cai)
     • QueryDecision  (resolução de transações em dúvida)
```

**Topologia: malha completa (*full mesh*).** Logicamente, todo nó conhece e se
comunica diretamente com todos os outros (N×N): o heartbeat é todos-para-todos,
o 2PC é líder→réplicas, a eleição Bully contata os ids maiores e o protocolo de
terminação consulta qualquer peer. Não há ponto único de roteamento — por isso
a queda de qualquer nó (inclusive o líder) não desconecta os demais. A
topologia é definida pela lista de peers (`distdb/config.py` ou `--peers`):
em produção cada nó fica em uma **máquina da rede local** (seção 6.7); para
desenvolvimento, a mesma malha é **simulada na própria máquina** com 3
processos em portas diferentes (`50051..50053`, seção 6.5) — a comunicação
continua sendo gRPC/TCP real, só que via interface de loopback.

### 3.2 Papel de cada entidade

- **Cliente** (`distdb/client.py`): envia operações. Mantém uma lista de todos os
  nós; manda escritas para o líder e, se contatar a réplica errada, é
  **redirecionado** (`NOT_LEADER`). Se um nó estiver fora, faz **failover** para o
  próximo.
- **Nó líder**: único que coordena escritas. Roda o **Two-Phase Commit** sobre as
  réplicas vivas, garantindo atomicidade. Responde heartbeats e pedidos de
  sincronização de estado.
- **Nó réplica (participante)**: guarda os dados, **vota** no 2PC, atende
  leituras e **vigia o líder**. Se o líder sumir, dispara uma eleição.

### 3.3 Principais mensagens (definidas em `proto/database.proto`)

- **API do cliente**: `Put`, `Update`, `Delete`, `Get`, `Exists`, `ListKeys`,
  `Size`, `GetAll`. A resposta de escrita inclui o campo `leader_address` para o
  redirecionamento.
- **Two-Phase Commit**: `Prepare` (fase 1, voto), `Commit` e `Abort` (fase 2,
  decisão) e `QueryDecision` (protocolo de terminação: "o que foi decidido
  sobre a transação X?").
- **Coordenação**: `Heartbeat`, `Election`, `Announce` (anúncio do novo líder),
  `WhoIsLeader`.
- **Recuperação**: `SyncState` (transferência do estado completo para um nó que
  reingressou).

Toda mensagem carrega um campo `timestamp` (relógio lógico de Lamport).

---

## 4. Como cada mecanismo funciona (passo a passo)

### 4.1 RPC com gRPC

O contrato do serviço está em `proto/database.proto`. Ele é compilado para código
Python (`scripts/generate_protos.py`) gerando os *stubs* em `distdb/generated/`.
Cada nó sobe um servidor gRPC (`grpc.server`) que implementa o serviço
`DatabaseService`; clientes e outros nós usam *stubs* para chamar os métodos
remotamente como se fossem funções locais.

### 4.2 Relógio lógico de Lamport (`distdb/lamport.py`)

Cada processo mantém um contador. As regras:

1. Antes de um evento local / enviar mensagem: `clock = clock + 1`.
2. Ao receber uma mensagem com timestamp `t`: `clock = max(clock, t) + 1`.

Isso garante a relação *happened-before*: se o evento A causou o evento B, então
`C(A) < C(B)`. Usamos o relógio para ordenar operações e datar transações. Todo
método do servidor chama `clock.update(request.timestamp)` ao receber uma chamada.

### 4.3 Escrita com Two-Phase Commit (`distdb/coordinator.py`)

Quando o líder recebe um `Put`/`Update`/`Delete`, ele coordena uma transação
distribuída em **duas fases** sobre o líder + réplicas vivas (o *cohort*).
Antes de começar, há uma checagem de **quórum**: se o cohort tem menos que a
**maioria do cluster (N/2+1)**, a transação é recusada (`no quorum`). Isso é o
controle de réplicas que impede *split-brain*: um líder isolado numa partição
minoritária da rede não consegue commitar escritas que a maioria nunca veria.

**Fase 1 — Votação (`Prepare`)**
Para cada participante, o coordenador envia `PREPARE(tx, op, chave, valor)`. Cada
participante:
- adquire um **lock na chave** (exclusão mútua — impede transações concorrentes
  na mesma chave);
- valida a operação (ex.: `UPDATE`/`DELETE` exigem chave existente);
- grava um registro `PREPARE` no **write-ahead log** (durabilidade);
- responde **SIM** (preparado) ou **NÃO**.

**Fase 2 — Decisão (`Commit`/`Abort`)**
- Se **todos** votaram SIM → o coordenador envia `COMMIT`. Cada nó aplica a
  operação e libera o lock. A escrita fica visível **atomicamente** em todos.
- Se **algum** votou NÃO (ou caiu) → envia `ABORT`. Ninguém aplica nada; os locks
  são liberados.

Resultado: **atomicidade** (tudo-ou-nada) e **isolamento** (locks entre o
*prepare* e a decisão).

### 4.4 Exclusão mútua / controle de concorrência

Funciona em **dois níveis** complementares:

1. **No líder (serialização de escritas)** — escritas concorrentes para a
   *mesma chave* são enfileiradas no coordenador (*striped locks* em
   `node.py:_coordinate_write`). Assim, sob alta concorrência, requisições
   simultâneas de clientes diferentes são atendidas uma após a outra em vez de
   se abortarem mutuamente. Chaves diferentes seguem em paralelo.
2. **Em cada participante (lock distribuído por chave)** — durante o `PREPARE`
   do 2PC, cada nó adquire um **lock na chave** dentro do seu `KeyValueStore`.
   Enquanto uma transação está *preparada*, qualquer outra transação que tente
   a mesma chave recebe voto NÃO e é abortada. É a garantia **distribuída**:
   protege os dados mesmo em cenários de troca de líder ou coordenadores
   concorrentes, quando a serialização do nível 1 não se aplica.

Chaves diferentes nunca se bloqueiam, preservando o paralelismo.

### 4.5 Detecção de falhas via Heartbeat (`distdb/failure_detector.py`)

Cada nó envia `Heartbeat` aos demais a cada ~1s. O detector registra o último
instante em que cada peer foi visto; se passar do *timeout* (3s), o peer é
considerado **suspeito/morto**. Isso alimenta duas decisões:
- o **líder** só inclui réplicas vivas no *cohort* do 2PC;
- as **réplicas** percebem quando o líder morre e disparam a eleição.

### 4.6 Eleição de líder — algoritmo Bully (`distdb/election.py`)

O nó com o **maior id vivo** deve ser o líder. Quando um nó percebe que o líder
caiu, ele inicia uma eleição:

1. Envia `ELECTION` para todos os nós de **id maior**.
2. Se **nenhum** responder → ele venceu: vira líder e envia `Announce`
   (COORDINATOR) para todos.
3. Se **algum id maior** responder ("estou vivo") → ele desiste e espera; o nó
   maior conduzirá a própria eleição e anunciará.

### 4.7 Tratamento de falhas (Entrega 2)

O sistema lida com falhas em várias camadas:

- **Queda de réplica durante a escrita**: o `Prepare`/`Commit` para o nó morto
  gera exceção; o coordenador **aborta** aquela tentativa, marca o nó como morto e
  **refaz a escrita** (retry) já excluindo o nó caído — então a escrita conclui no
  *cohort* restante. (`Node._coordinate_write`)
- **Queda do líder**: detectada por heartbeat; os sobreviventes rodam o **Bully** e
  elegem um novo líder, sem intervenção manual.
- **Durabilidade / recuperação de crash**: cada nó tem um **write-ahead log** e
  **snapshots** (`distdb/store.py`). Ao reiniciar, `recover()` reconstrói o estado
  reaplicando apenas transações que tiveram `COMMIT` registrado. Transações
  "em dúvida" (preparadas sem decisão) são descartadas com segurança.
- **Reingresso de réplica (state transfer / controle de réplicas)**: ao voltar, a
  réplica pede `SyncState` ao líder e substitui seu estado pelo *snapshot* atual,
  ficando em dia.
- **Eleição vencida por um nó defasado**: cada nó registra o `last_commit_ts`
  (timestamp de Lamport da última transação commitada). Ao **vencer uma eleição**,
  antes de se anunciar líder, o nó consulta o `SyncState` dos peers vivos e, se
  algum tiver `last_commit_ts` maior que o seu, **adota o estado mais recente**.
  Isso impede que um nó que ficou um tempo fora (ex.: o antigo líder de id mais
  alto reiniciando) assuma a liderança com dados antigos e os imponha às réplicas.
- **Coordenador morre no meio do 2PC (transação "em dúvida")**: o 2PC clássico é
  *bloqueante* — um participante que votou SIM e nunca recebeu a decisão ficaria
  com a chave travada para sempre. Implementamos o **protocolo de terminação
  cooperativa**: toda transação preparada há mais de `PENDING_TX_TIMEOUT` (8 s)
  sem decisão faz o nó perguntar aos peers (`QueryDecision`) o que foi decidido.
  Se **algum** peer registrou `COMMIT`, ele commita também; se ninguém viu uma
  decisão, **aborta por presunção** (*presumed abort* — seguro, pois o
  coordenador só envia COMMIT depois de todos os votos SIM, então se nenhum peer
  alcançável commitou, nenhum cliente recebeu confirmação). Em ambos os casos o
  lock da chave é liberado e o sistema destrava sozinho.
- **Partição de rede (split-brain)**: escritas exigem **quórum majoritário**
  (N/2+1 participantes vivos). Numa partição, só o lado com a maioria dos nós
  continua aceitando escritas; o lado minoritário responde `no quorum` (mas
  segue atendendo leituras do último estado commitado). Quando a partição se
  resolve, o lado minoritário se ressincroniza pelo `SyncState` — não existe a
  possibilidade de dois líderes commitarem escritas divergentes.
- **Cliente**: redireciona para o líder (`NOT_LEADER`) e faz **failover** entre nós
  quando algum está indisponível.

---

## 5. Estrutura do projeto

```
distributed-db/
├── proto/
│   └── database.proto          # contrato gRPC (serviço + mensagens)
├── distdb/                     # pacote principal
│   ├── lamport.py              # relógio lógico de Lamport
│   ├── store.py                # store chave-valor + WAL/snapshot + locks por chave
│   ├── coordinator.py          # coordenador Two-Phase Commit
│   ├── election.py             # algoritmo Bully (lógica de decisão)
│   ├── failure_detector.py     # detecção de falhas por heartbeat
│   ├── config.py               # topologia do cluster (ids + endereços)
│   ├── node.py                 # servidor gRPC + threads de coordenação
│   ├── client.py               # cliente de linha de comando (redirect + failover)
│   └── generated/              # stubs gerados do .proto (criados pelo script)
├── scripts/
│   ├── generate_protos.py/.sh  # compila o .proto
│   ├── run_cluster.bat/.sh     # sobe um cluster local de 3 nós
│   ├── stop_cluster.sh         # encerra o cluster (Linux/Mac)
│   ├── benchmark.py            # análise de desempenho (distribuído × único)
│   └── stress_test.py          # alta concorrência + checagem de consistência
├── tests/                      # testes (sem necessidade de pytest)
│   ├── test_lamport.py
│   ├── test_store.py           # commit/abort, locks, recuperação por WAL
│   ├── test_2pc.py             # atomicidade, abort, falha de participante
│   ├── test_election.py        # Bully + detector de falhas
│   ├── test_integration.py     # cenário completo de tolerância a falhas
│   └── run_all.py              # roda todos os testes
├── requirements.txt
└── README.md
```

A separação é proposital: toda a **lógica de sistemas distribuídos**
(`lamport`, `store`, `coordinator`, `election`, `failure_detector`) é
**independente do gRPC**, o que a torna fácil de testar isoladamente. O
`node.py` apenas "liga" essa lógica ao transporte gRPC.

---

## 6. Como usar

> **Resumo rápido (TL;DR) — mesma máquina, Windows:**
> ```bat
> cd C:\Trabalhos-puc\distributed-db
> python -m venv venv
> venv\Scripts\activate
> pip install -r requirements.txt
> python scripts\generate_protos.py
> scripts\run_cluster.bat
> python -m distdb.client --demo
> ```
> O passo a passo completo (e o modo entre máquinas) está detalhado abaixo.

### 6.1 Pré-requisitos

- **Python 3.11 ou superior.** Confira com:
  ```bash
  python --version
  ```
  (Se `python` não funcionar, tente `python3`. No Windows, na instalação do
  Python marque a opção **"Add Python to PATH"**.)
- Os arquivos do projeto na pasta `distributed-db`.

**Como abrir o terminal já dentro da pasta do projeto:**
- *Windows*: abra a pasta `distributed-db` no Explorer, clique na barra de
  endereço, digite `cmd` e tecle Enter — abre o Prompt de Comando já na pasta.
- *Linux/macOS*: `cd caminho/para/distributed-db`.

> Todos os comandos deste README são executados **a partir da raiz do projeto**
> (a pasta que contém o diretório `distdb` e o arquivo `requirements.txt`).

### 6.2 Criar o ambiente virtual e instalar as dependências

Faça isso **uma vez** por máquina. O ambiente virtual (`venv`) isola as
bibliotecas do projeto.

**Windows (CMD ou PowerShell):**
```bat
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**Linux / macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Depois de ativar, o terminal mostra `(venv)` no início da linha. **Toda vez que
abrir um terminal novo** para rodar um nó ou o cliente, ative o venv de novo
(`venv\Scripts\activate` no Windows, `source venv/bin/activate` no Linux/Mac).

As dependências instaladas são: `grpcio`, `grpcio-tools` e `protobuf`.

### 6.3 Gerar o código gRPC a partir do `.proto`

Faça isso **uma vez** (e novamente sempre que mexer no `proto/database.proto`):
```bash
python scripts/generate_protos.py
```
Isso cria `distdb/generated/database_pb2.py` e `distdb/generated/database_pb2_grpc.py`.
Sem esse passo os nós **não iniciam** (dá erro de import). Para conferir que deu
certo, verifique se os dois arquivos existem dentro de `distdb/generated/`.

### 6.4 Verificar que está tudo certo (testes que não usam rede)

Antes de subir o cluster, rode os testes automatizados. Eles validam toda a
lógica distribuída **sem precisar de rede nem dos nós no ar**:
```bash
python tests/run_all.py
```
Se aparecer **`ALL TESTS PASSED`**, o ambiente está pronto.

### 6.5 Rodar na MESMA máquina (3 nós em localhost)

Esta é a forma mais fácil de testar. Os 3 nós sobem em portas diferentes
(`50051`, `50052`, `50053`) no `localhost`. **Não precisa configurar nada.**

**Opção A — script automático**

*Windows* (abre cada nó em uma janela própria):
```bat
scripts\run_cluster.bat
```

*Linux / macOS* (roda em segundo plano; logs em `logs/`):
```bash
bash scripts/run_cluster.sh
tail -f logs/node*.log      # acompanhar os logs (Ctrl+C para sair do tail)
bash scripts/stop_cluster.sh   # para encerrar o cluster depois
```

**Opção B — manual (qualquer SO): um terminal por nó**

Abra **3 terminais**, ative o `venv` em cada um e rode (um comando por terminal):
```bash
python -m distdb.node --id 1
python -m distdb.node --id 2
python -m distdb.node --id 3
```
O nó de **id 3** começa como líder (tem o maior id). Cada nó escuta na porta
`50050 + id`. Nos logs você verá mensagens como `listening on 127.0.0.1:50053`
e `leader is now node 3`.

Abra então **mais um terminal** (com o `venv` ativo) para o cliente — veja 6.6.

### 6.6 Usar o cliente

**Modo interativo** (abre um prompt `db>` para digitar comandos):
```bash
python -m distdb.client
```
```
db> leader                 # mostra quem é o líder atual
db> put user Luis          # grava (replica em todas as réplicas via 2PC)
db> get user               # lê
db> update user Carlos     # atualiza uma chave existente
db> exists user
db> list                   # lista as chaves
db> size
db> all                    # mostra todos os pares chave=valor
db> delete user
db> quit
```

**Demonstração roteirizada** 
```bash
python -m distdb.client --demo
```

**Comando único** (sem entrar no modo interativo):
```bash
python -m distdb.client put cidade "Belo Horizonte"
python -m distdb.client get cidade
```

**Roteiro guiado — simulando TODAS as operações (rotas gRPC) na mão**

Suba o cluster (6.5), deixe os logs dos 3 nós visíveis, abra o cliente
(`python -m distdb.client`) e siga a sequência. A coluna "o que acontece por
trás" diz qual RPC é disparado e o que procurar nos logs dos nós.

| # | Comando no `db>` | Resposta esperada | O que acontece por trás |
|---|------------------|-------------------|--------------------------|
| 1 | `leader` | `leader = node 3 (127.0.0.1:50053)` | RPC `WhoIsLeader` em qualquer nó |
| 2 | `put user Luis` | `PUT committed (cohort=3)` | cliente acha o líder → líder roda o 2PC: nos logs, cada nó mostra `PREPARE ... -> YES` e `COMMIT`, e o líder `2PC COMMIT ... votes=3/3` |
| 3 | `get user` | `Luis` | RPC `Get` — atendido localmente por **qualquer** nó, sem 2PC (leitura barata) |
| 4 | `exists user` | `True` | RPC `Exists` |
| 5 | `put lang Python` | `PUT committed (cohort=3)` | outra transação 2PC completa |
| 6 | `list` | `['user', 'lang']` | RPC `ListKeys` |
| 7 | `size` | `2` | RPC `Size` |
| 8 | `all` | `user = Luis` / `lang = Python` | RPC `GetAll` |
| 9 | `update user Carlos` | `UPDATE committed (cohort=3)` | 2PC de novo; `get user` agora devolve `Carlos` |
| 10 | `update naoexiste X` | `UPDATE aborted: ... does not exist` | os participantes **votam NÃO** no `Prepare` (validação semântica) → líder manda `Abort`; logs mostram `PREPARE ... -> NO` e `2PC ABORT` |
| 11 | `delete lang` | `DELETE committed (cohort=3)` | 2PC; `exists lang` → `False` |
| 12 | `quit` | — | encerra o cliente (os nós continuam) |

Dois experimentos extras que mostram o roteamento e a replicação:

- **Redirecionamento (NOT_LEADER)**: rode um cliente apontando só para uma
  réplica: `python -m distdb.client --peers "1=127.0.0.1:50051" put k v`.
  A réplica responde `NOT_LEADER` com o endereço do líder; como esse cliente
  não conhece o líder, dá erro — agora repita com a lista completa e veja a
  escrita ser **redirecionada automaticamente** para o nó 3.
- **Leitura em réplica específica**: derrube o líder *depois* de gravar e leia
  numa réplica (`python -m distdb.client --peers "1=127.0.0.1:50051" get user`)
  — o dado está lá, provando que o 2PC replicou para todos.

Depois desse aquecimento, vá para a **seção 7** (simulações de falha: queda de
réplica, queda de líder, eleição, quórum, recuperação) e para a **seção 9**
(carga concorrente com `stress_test.py`).

### 6.7 Rodar em MÁQUINAS DIFERENTES (rede local)

Aqui cada nó roda em um computador diferente da mesma rede. A ideia: **todas as
máquinas usam a mesma lista de nós (`--peers`); muda apenas qual `--id` cada uma
executa.**

**Passo 1 — descobrir o IP de cada máquina (na LAN):**
- *Windows*: `ipconfig` → procure "Endereço IPv4" (algo como `192.168.0.10`).
- *Linux/macOS*: `ip addr` ou `hostname -I`.

**Passo 2 — montar a lista de peers** no formato
`id=ip:porta,id=ip:porta,...`. Exemplo com 3 máquinas:
```
1=192.168.0.10:50051,2=192.168.0.11:50051,3=192.168.0.12:50051
```

**Passo 3 — liberar a porta no firewall de cada máquina** (passo que mais
costuma travar o teste!). Use a porta que está na lista (ex.: `50051`):
- *Windows* (PowerShell **como Administrador**):
  ```powershell
  New-NetFirewallRule -DisplayName "distdb" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 50051
  ```
  (Ou, quando o Windows perguntar na primeira execução, marque **permitir** o
  Python em redes privadas.)
- *Linux* (ufw): `sudo ufw allow 50051/tcp`

**Passo 4 — iniciar o nó em cada máquina** (mesma lista, `--id` diferente):

Na máquina A (IP 192.168.0.10):
```bash
python -m distdb.node --id 1 --peers "1=192.168.0.10:50051,2=192.168.0.11:50051,3=192.168.0.12:50051"
```
Na máquina B (192.168.0.11): **o mesmo comando, mas** `--id 2`.
Na máquina C (192.168.0.12): **o mesmo comando, mas** `--id 3`.

**Passo 5 — rodar o cliente** (de qualquer máquina da rede), com a mesma lista:
```bash
python -m distdb.client --peers "1=192.168.0.10:50051,2=192.168.0.11:50051,3=192.168.0.12:50051"
```

**Dica para conferir a conectividade** antes: de uma máquina, teste se alcança a
porta da outra, por exemplo `ping 192.168.0.11`, ou no PowerShell
`Test-NetConnection 192.168.0.11 -Port 50051` (deve dar `TcpTestSucceeded: True`).

> Observação: o número de máquinas é flexível; basta ajustar a lista `--peers`
> em todos. Recomendamos **3 ou mais nós**: como as escritas exigem **quórum
> majoritário (N/2+1)**, com 3 nós o sistema tolera a queda de 1 e continua
> escrevendo; com apenas 2 nós o quórum é 2 e a queda de qualquer um bloqueia
> escritas (as leituras continuam) até ele voltar.

### 6.8 Solução de problemas (erros comuns)

| Sintoma | Causa provável | Como resolver |
|---------|----------------|----------------|
| `ModuleNotFoundError: No module named 'grpc'` | dependências não instaladas / `venv` não ativado | ative o `venv` e rode `pip install -r requirements.txt` |
| `ModuleNotFoundError: ... database_pb2` ou `distdb.generated` | stubs gRPC não gerados | rode `python scripts/generate_protos.py` |
| `ModuleNotFoundError: No module named 'distdb'` | rodou de dentro de uma subpasta | execute os comandos a partir da **raiz** do projeto (pasta com `distdb/`) |
| `Address already in use` / `Failed to bind` | já existe um nó usando a porta | feche o nó antigo ou use outra porta via `--peers` |
| Cliente: `node X unreachable` ou trava | nó fora do ar, IP errado ou **firewall** bloqueando | confira IPs, libere a porta no firewall (6.7, passo 3), veja se os nós estão rodando |
| Entre máquinas não conecta, mas na mesma máquina sim | firewall ou máquinas em redes diferentes | libere a porta e confirme que estão na mesma LAN (teste com `ping`/`Test-NetConnection`) |
| Quero "zerar" os dados | WAL/snapshots antigos em `data/` | apague a pasta `data/` (ou os arquivos `node*.wal` / `node*.snapshot`) e suba o cluster de novo |

---

## 7. Roteiro de demonstração da tolerância a falhas

Este é um passo a passo pronto para usar na apresentação. Suba o cluster de 3 nós
(seção 6.5) e deixe os logs dos nós visíveis — é neles que a "mágica" aparece.

**Cenário 1 — Escrita normal replicada (2PC)**
1. No cliente: `put user Luis` e depois `put lang Python`.
2. Olhe os logs: o **líder (nó 3)** mostra `2PC COMMIT ... votes=3/3` e cada
   réplica mostra `PREPARE ... -> YES` seguido de `COMMIT`. Ou seja, o dado foi
   gravado **atomicamente nos 3 nós**.
3. Confirme lendo de qualquer nó: `get user` → `Luis`.

**Cenário 2 — Queda de uma réplica (não o líder)**
1. Derrube o **nó 2** (feche a janela dele, ou `Ctrl+C` no terminal do nó 2).
2. No cliente: `put cidade BH`. A escrita **conclui normalmente** — o líder
   percebe que o nó 2 está fora, exclui ele do *cohort* e commita no que sobrou
   (`votes=2/2`). O serviço **não parou**.
3. **Reinicie** o nó 2 (`python -m distdb.node --id 2`). Nos logs dele aparece
   `synced N keys from leader` — ele se **sincroniza** e recupera o `cidade=BH`
   que tinha perdido. Confirme: pare os outros e `get cidade` direto nele, ou
   simplesmente veja o log de sync.

**Cenário 3 — Queda do líder + eleição automática**
1. Faça algumas escritas (`put a 1`, `put b 2`).
2. Derrube o **líder (nó 3)**.
3. Observe os logs dos nós 1 e 2: em poucos segundos aparece
   `leader 3 appears down -> starting election`, depois a eleição **Bully**, e o
   **nó 2 vira o novo líder** (`won election -> I am the leader`).
4. No cliente: `put c 3`. O cliente é **redirecionado** automaticamente para o
   novo líder (nó 2) e a escrita funciona. `leader` mostra o nó 2.
5. **Reinicie** o nó 3. Ele recupera seu estado do **WAL** (disco) ao iniciar e
   se **sincroniza** com o líder atual, voltando ao cluster.

**Cenário 4 — Durabilidade (recuperação por WAL)**
1. Faça escritas e derrube **todos** os nós.
2. Suba o cluster de novo. Como cada nó tem um **write-ahead log** em `data/`, os
   dados comprometidos **continuam lá** após o restart (`get` devolve os valores).
   Para começar do zero, apague a pasta `data/` antes de subir.

**Cenário 5 — Falha sob alta concorrência (o mais impressionante)**
1. Rode o teste de estresse: `python scripts/stress_test.py --clients 16 --ops 200`.
2. Enquanto a linha de progresso (`... N ops in the last second`) avança,
   **derrube uma réplica** — a vazão mal se altera (o líder encolhe o cohort).
3. Em outra rodada, derrube o **líder** no meio do teste: a vazão cai por
   ~3–5 s (detecção da falha + eleição), os clientes fazem failover/redirect
   sozinhos e a carga volta a fluir no novo líder.
4. Ao final, o script **confere a consistência**: lê o estado completo de cada
   nó vivo e mostra que todos têm **exatamente os mesmos dados**, mesmo após a
   falha no meio de centenas de transações concorrentes.
5. Bônus: se a queda do líder deixar alguma transação "em dúvida" numa réplica
   (votou SIM e não recebeu a decisão), em ~8 s aparece no log dela
   `termination protocol: in-doubt tx=... resolved -> COMMIT/ABORT` — o
   protocolo de terminação destravando a chave sozinho.

**Cenário 6 — Quórum majoritário (proteção contra split-brain)**
1. Com o cluster de 3 nós no ar, derrube **duas** réplicas (deixe só o líder).
2. No cliente: `put x 1` → a escrita é **recusada**: `aborted: no quorum: only 1
   live participant(s), majority of 2 required`. O líder sozinho se recusa a
   commitar algo que a maioria do cluster não veria.
3. `get` de chaves antigas continua funcionando (leituras não exigem quórum).
4. Religue **um** nó: assim que ele sincronizar, `put x 1` volta a funcionar
   (2/3 = maioria). É a escolha consistência > disponibilidade do sistema.

> O que observar nos logs (resumo): `PREPARE ... YES/NO`, `2PC COMMIT/ABORT
> votes=X/Y`, `leader ... appears down`, `starting Bully election`,
> `won election`, `synced N keys from leader`, `adopted fresher state from node X`
> (quando um nó que estava fora vence a eleição e puxa o estado mais novo dos
> peers antes de assumir).

---

## 8. Rodando os testes automatizados

**Não é necessário `pytest` nem subir o cluster** — os testes exercitam toda a
lógica distribuída em memória. A partir da raiz do projeto (com o `venv` ativo):

Rodar **todos** os testes de uma vez:
```bash
python tests/run_all.py
```
Saída esperada ao final: **`ALL TESTS PASSED`**.

Rodar um teste **individual** (cada arquivo roda sozinho):
```bash
python tests/test_lamport.py        # relógio de Lamport
python tests/test_store.py          # store: commit/abort, locks por chave, recuperação por WAL
python tests/test_2pc.py            # 2PC: atomicidade, abort por voto NÃO, falha de participante
python tests/test_election.py       # eleição Bully + detector de falhas
python tests/test_integration.py    # cenário completo de tolerância a falhas
```

O que cada um cobre:

- **test_lamport** — incremento monotônico e regra `max(local, recebido)+1`.
- **test_store** — escrita só fica visível após o `commit`; `abort` descarta e
  libera o lock; `update`/`delete` exigem chave existente; lock por chave bloqueia
  transações concorrentes; recuperação por WAL reaplica **só** o que teve commit;
  `last_commit_ts` acompanha o commit mais novo e sobrevive a snapshot + restart;
  o log de decisões (`COMMIT`/`ABORT` por transação) responde ao protocolo de
  terminação e também sobrevive a restart.
- **test_2pc** — commit replica em todos atomicamente; um voto NÃO aborta em todos;
  falha de participante é detectada e aborta sem expor dado parcial; *retry*
  excluindo o nó morto conclui a escrita; **sem quórum a escrita é recusada** sem
  tocar nos stores; regras do **protocolo de terminação** (qualquer COMMIT visto
  → commit; nada visto → *presumed abort*), incluindo o cenário ponta a ponta do
  coordenador que morre depois de commitar em só um participante.
- **test_election** — o maior id vivo vence; nó desiste se houver id maior vivo;
  vence se todos os maiores caíram; detector marca nó como morto após o *timeout*.
- **test_integration** — fluxo ponta a ponta: escrita normal → queda de réplica →
  queda do líder → eleição → recuperação por *state transfer*.

---

## 9. Análise de desempenho e alta concorrência

### 9.1 Distribuído × máquina única (`benchmark.py`)

Com o cluster no ar:
```bash
python scripts/benchmark.py --ops 500
```
Compara o custo de escritas/leituras no **cluster distribuído** (com 2PC e
replicação) contra um **baseline de máquina única** (um único store local, sem
rede). Como esperado, as escritas distribuídas pagam o custo de rede + 2PC,
enquanto leituras são baratas (atendidas localmente por qualquer nó). Para rodar
só o baseline (sem cluster): `python scripts/benchmark.py --baseline-only`.

### 9.2 Teste de estresse com requisições simultâneas (`stress_test.py`)

Avalia como o sistema se comporta com **muitos clientes concorrentes**:
```bash
python scripts/stress_test.py                      # 8 clientes x 100 ops
python scripts/stress_test.py --clients 16 --ops 200
python scripts/stress_test.py --hot-ratio 0.5      # mais disputa pela mesma chave
```
O que o teste faz:

- dispara **N threads-cliente ao mesmo tempo** (cada uma é um cliente gRPC
  independente), com carga mista: escritas em **chaves quentes** disputadas por
  todos (estressa a exclusão mútua), escritas em chaves únicas (paralelismo
  puro) e leituras;
- imprime **vazão por segundo** durante a execução — derrube um nó no meio para
  ver o tratamento de falhas sob carga (Cenário 5 acima);
- ao final, reporta **ops/s agregado**, latência **p50/p95/p99** de escrita e
  leitura e a contagem de transações **committed / aborted / failed**;
- por último, faz uma **verificação de consistência**: baixa o estado completo
  de cada nó vivo e confirma que todos são **idênticos**.

Resultados típicos a discutir no relatório: o custo de serializar escritas na
mesma chave (latência cresce com `--hot-ratio` alto, mas nada aborta), o ganho
de paralelismo em chaves distintas, e a janela de indisponibilidade de escrita
(~3–5 s) durante uma eleição — com leituras continuando a funcionar. Escritas
que caem nessa janela são **reexecutadas automaticamente** pelo cliente (retry)
e concluem no novo líder: nenhuma operação se perde, e o tempo de espera
aparece na latência p99.
