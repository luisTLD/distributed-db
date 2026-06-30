# FEATURES

Tudo da raiz do projeto, com o `.venv` ativo (`.venv\Scripts\activate`).

## Subir o cluster (4 terminais)
Abra 4 terminais. Nos 3 primeiros, um nó em cada (deixe as 3 janelas visíveis, é onde a "mágica" aparece):
```
python -m distdb.node --id 1
python -m distdb.node --id 2
python -m distdb.node --id 3
```
No 4º terminal, o cliente:
```
python -m distdb.client
```
O nó 3 (maior id) vira líder. Para zerar os dados antes: feche os nós e apague a pasta `data\`.

---

## Operações do banco (no prompt `db>`)
- `leader`: mostra o líder.
- `put <chave> <valor>`: grava (2PC em todas as réplicas).
- `update <chave> <valor>`: atualiza chave existente.
- `delete <chave>`: remove.
- `get <chave>`: lê.
- `exists <chave>`: True/False.
- `list` / `size` / `all`: chaves / quantidade / tudo.
- `quit`: sai.

Regras: chave é única (mesma chave sobrescreve); para vários registros use chaves diferentes (`user1`, `user2`); valor pode ter espaços.

---

## Demonstrar cada feature ao vivo

**1. RPC / quem é o líder**
```
db> leader
```
gRPC respondendo. Mostra `leader = node 3`.

**2. Two-Phase Commit (escrita atômica)**
```
db> put user Luis
```
Resposta `PUT committed (cohort=3)`. Aponte nos logs dos nós: réplicas mostram `PREPARE ... -> YES` e `COMMIT`; líder mostra `2PC COMMIT ... votes=3/3`.

**3. Leitura (qualquer nó, sem 2PC)**
```
db> get user
```

**4. Aborto por validação (voto NÃO)**
```
db> update naoexiste X
```
`UPDATE aborted: ... does not exist`. Logs: `PREPARE ... -> NO` e `2PC ABORT`.

**5. Lamport**
Nos logs, mostre o `[clock N]` em cada linha: só cresce e sincroniza entre os nós.

**6. Replicação real (ler de uma réplica específica)**
Em outro terminal:
```
python -m distdb.client --peers "1=127.0.0.1:50051" get user
```
O dado está no nó 1. Prova que o 2PC replicou.

**7. Queda de réplica (serviço não para)**
- Feche a janela do nó 2.
- `db> put cidade BH` → conclui com `cohort=2`.
- Reabra: `python -m distdb.node --id 2` → log mostra `synced N keys from leader`.

**8. Queda do líder + eleição**
- `db> put a 1` e `put b 2`.
- Feche a janela do nó 3.
- Logs dos nós 1 e 2: `leader 3 appears down`, eleição, `won election` no nó 2.
- `db> put c 3` → redirecionado ao novo líder, funciona. `leader` mostra o nó 2.

**9. Quórum (anti split-brain)**
- Feche DUAS réplicas (deixe só o líder).
- `db> put x 1` → `aborted: no quorum: only 1 live participant(s), majority of 2 required`.
- `db> get` de chave antiga ainda funciona. Reabra um nó → volta a escrever.

**10. Durabilidade (WAL)**
- Faça escritas, feche os 3 nós, suba os 3 de novo.
- `db> get <chave>` → o dado continua lá (veio do WAL em `data\`).

**11. Alta concorrência (sob carga)**
```
python scripts\stress_test.py --clients 16 --ops 200
```
Enquanto roda, feche um nó. No fim: muitas escritas confirmadas, `aborted=0`, e os 3 nós idênticos.

**12. Testes automatizados (sem rede)**
```
python tests\run_all.py
```
`ALL TESTS PASSED`.

---

## Entre máquinas
Ver `GUIA-TESTE-ENTRE-MAQUINAS.md`.
