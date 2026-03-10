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
| `bc6e85bfd6...` | Data de Alteração de Funil |
| `fae8184ad9...` | Tipo do Plano |
| `93df664878...` | Finder (Origem da Fatura) |
| `b4233a3717...` | Lead (Origem) |
| `deb4d6ce97...` | Nº de Unidades |
| `9fe715b9c8...` | Média de Consumo (KWh) |
| `bba2ac4fe9...` | Consórcio |
| `ca61a683d1...` | kWh Contratado |
| `f6671d52cf...` | kWh Não Compensável |
| `f29736fac6...` | Cidade da Instalação |

## Notas Técnicas

- **Paginação**: API limita 500 deals por requisição. O script itera automaticamente.
- **Parâmetro `everyone=1`**: busca deals de todos os usuários, não apenas do autenticado.
- **Rate Limits**: API v1 permite ~100 requests/segundo por empresa. Improvável atingir o limite.

## Erros Conhecidos

- Se `success=false`, o script emite aviso e continua para o próximo pipeline.
- Se `.env` não existir ou estiver incompleto, o script encerra com erro.
