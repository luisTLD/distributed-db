# Teste entre máquinas: passo a passo direto

Duas máquinas no mesmo Wi-Fi. Um nó em cada.

## 1. Pegar o IP de cada máquina
- Em cada PC: `ipconfig`
- Pegue o **Endereço IPv4** do adaptador **Wi-Fi** (ignore "Tailscale" e qualquer `169.254...`).
- Anote: máquina A e máquina B. Ex.: A = `10.84.96.46`, B = `10.84.96.83`.
- Os IPs mudam quando reconecta. Sempre confira o atual antes de rodar.

## 2. Ver se uma alcança a outra
- Da máquina A: `ping IP_B`
- Respondeu → segue. Deu "host inacessível" ou "tempo esgotado" → o Wi-Fi tem **isolamento de clientes** (normal em faculdade).
- Solução: liguem o **hotspot do celular** de um, conectem as duas máquinas nele, e refaçam o passo 1 (IPs novos, tipo `192.168.x.x`).

## 3. Liberar a porta no firewall: nas DUAS máquinas
PowerShell **como Administrador**:
```
New-NetFirewallRule -DisplayName "distdb" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 50051
```

## 4. Montar a lista de peers (IGUAL nas duas)
```
1=IP_A:50051,2=IP_B:50051
```
Troque IP_A e IP_B pelos reais. Essa lista é a mesma nas duas máquinas e no cliente.

## 5. Subir um nó em cada máquina: só muda o `--id`
Da raiz do projeto, com o `.venv` ativo:
- Máquina A:
```
python -m distdb.node --id 1 --peers "1=IP_A:50051,2=IP_B:50051"
```
- Máquina B:
```
python -m distdb.node --id 2 --peers "1=IP_A:50051,2=IP_B:50051"
```
Nos logs deve aparecer `listening on ...`, a eleição, e `leader is now node 2`.

## 6. Rodar o cliente (qualquer máquina, mesma lista)
```
python -m distdb.client --peers "1=IP_A:50051,2=IP_B:50051"
```
No prompt:
```
db> leader
db> put user Luis
db> get user
```

## 7. Provar que replicou entre as máquinas
Leia direto de cada nó:
```
python -m distdb.client --peers "1=IP_A:50051" get user
python -m distdb.client --peers "2=IP_B:50051" get user
```
O valor tem que aparecer nos dois.

## Aviso com 2 nós
Quórum = 2: os dois precisam estar vivos para escrever. Se derrubar um, escrita dá `no quorum` (leituras continuam).

## Para mostrar falha na escrita (3 nós com 2 máquinas)
- Máquina A roda 2 nós (portas 50051 e 50052), máquina B roda 1 (50051).
- Libere 50051 e 50052 no firewall da máquina A (passo 3, repetindo para 50052).
- Lista nas três: `1=IP_A:50051,2=IP_A:50052,3=IP_B:50051`
- Máquina A: dois terminais, `--id 1` e `--id 2`. Máquina B: `--id 3`.
- Agora quórum = 2, tolera a queda de 1 nó na escrita.
