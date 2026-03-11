# Diretiva: Buscar Deals do Pipedrive

## Objetivo

Consultar a API v1 do Pipedrive e extrair deals dos pipelines 36, 37 e 38, retornando somente campos personalizados selecionados em formato JSON.

## Inputs

- **Credenciais**: `PIPEDRIVE_API_TOKEN` e `PIPEDRIVE_BASE_URL` definidos no `.env`
- **Pipelines**: 36, 37, 38

## Script

`execution/fetch_pipedrive_deals.py`

## Como Rodar

```bash
cd c:\Users\Matheus\Documents\MeusProgramasPy\busca_api_pipedrive
pip install requests python-dotenv
python execution/fetch_pipedrive_deals.py
```

## Output

- JSON salvo em `.tmp/deals_pipedrive.json`
- JSON impresso no stdout
- Estrutura: `{ pipeline_36: [...], pipeline_37: [...], pipeline_38: [...], metadata: {...} }`

## Campos Extraídos

| Chave API | Nome Amigável |
|---|---|
| `title` | Título |
| `stage_id` | Etapa |
| `value` | Valor |
| `status` | Status |
| `5d6f2509ce...` | Percentual Concedido |
| `bc6e85bfd6...` | Data de Alteração de Funil (dd/mm/yyyy) |
| `fae8184ad9...` | Tipo do Plano |
| `93df664878...` | Finder (Origem da Fatura) |
| `b4233a3717...` | Lead (Origem) |
| `deb4d6ce97...` | Nº de Unidades |
| `9fe715b9c8...` | Média de Consumo (KWh) |
| `bba2ac4fe9...` | Consórcio |
| `ca61a683d1...` | kWh Contratado |
| `f6671d52cf...` | kWh Não Compensável |
| `f29736fac6...` | Cidade da Instalação |
| `stage_change_time` | Data da Ultima Alteração da Etapa |
| `6aa88ec119...` | Data de Assinatura (dd/mm/yyyy) |
| `add_time` | Negócio Criado Em (dd/mm/yyyy) |
| *(calculado)* | Colocação da Etapa (posição ordinal do stage no pipeline) |
| *(calculado)* | Data de Criação ou Alteração do Funil (fallback: se Data de Alteração de Funil = null, usa Negócio Criado Em) |
| *(calculado)* | Tipo de Finder (Tratamento Interno) — extrai tipo do finder; se Interno coexiste com outro, retorna o outro |
| *(calculado)* | Fatura Cheia (R$) = Média de Consumo (KWh) × 1,1150643 |
| *(calculado)* | Valor de Assinatura (R$) = kWh Contratado × 1,1150643 × (1 - percentual do plano) |

## Notas Técnicas

- **Paginação**: API limita 500 deals por requisição. O script itera automaticamente.
- **Parâmetro `everyone=1`**: busca deals de todos os usuários, não apenas do autenticado.
- **Rate Limits**: API v1 permite ~100 requests/segundo por empresa. Improvável atingir o limite.
- **Colocação da Etapa**: usa `order_nr` da API de stages para determinar a posição (1-based).
- **Campos de data**: Data de Assinatura, Data de Alteração de Funil e Negócio Criado Em são convertidos de ISO para dd/mm/yyyy.
- **Data de Criação ou Alteração do Funil**: campo calculado — usa Data de Alteração de Funil quando preenchido, caso contrário usa Negócio Criado Em como fallback.
- **Tipo de Finder (Tratamento Interno)**: extrai o tipo (segundo segmento entre traços, ex: "Interno", "Gold", "Plus"). Se houver dois finders e um for "Interno", prioriza o tipo do parceiro externo.
- **Constante TARIFA_KWH**: `1.1150643` — usada nos cálculos de Fatura Cheia e Valor de Assinatura.
- **Fatura Cheia (R$)**: `Média de Consumo (KWh) × TARIFA_KWH`. Retorna null se Média de Consumo for null.
- **Valor de Assinatura (R$)**: `kWh Contratado × TARIFA_KWH × (1 - percentual)`. Percentual extraído de "Tipo do Plano" (ex: "Plano 25%" → 0.25). Retorna null se kWh Contratado ou Tipo do Plano forem null.

## Erros Conhecidos

- Se `success=false`, o script emite aviso e continua para o próximo pipeline.
- Se `.env` não existir ou estiver incompleto, o script encerra com erro.