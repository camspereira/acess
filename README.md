#  Databricks Notebook: Desafio - Análise de Tabela
Este notebook demonstra como carregar um arquivo de logs de acesso (no formato TXT) no Databricks File System (DBFS), transformá-lo em uma tabela e realizar consultas sobre os dados. O arquivo **access_logs.txt** contém informações de acesso, e com esse notebook, você realiza algumas análises sobre o conteúdo deste arquivo.

## Descrição

Este notebook realiza as seguintes operações:

1. **Leitura de Dados**: Carrega o arquivo **access_logs.txt** armazenado no DBFS e o transforma em um DataFrame.
2. **Contagem de Acessos**: Filtra os dados para contar o número de visitas ao site "google.com".
3. **Contagem de Páginas Mais Visitadas**: Agrupa os dados por página e retorna as 5 páginas mais acessadas.
4. **Contagem de Acessos por Data**: Agrupa os dados por data e exibe a quantidade de acessos para cada data.
5. **Classificação de IP**: Classifica os endereços IP como "internos" ou "externos" e conta o número de acessos de cada tipo de IP.

## Tecnologias Usadas

- **Databricks**: Ambiente utilizado para o processamento de dados.
- **PySpark**: Usado para processamento distribuído dos dados no notebook.
- **Python**: Linguagem usada para implementar o código.

## Como Usar

1. **Carregue o notebook no Databricks**:
   - Faça login no Databricks e crie um novo notebook.
   - Copie e cole o código deste repositório no seu notebook.

2. **Prepare os Dados**:
   - Coloque o arquivo **access_logs.txt** no formato CSV dentro do DBFS.
   - Certifique-se de que o caminho do arquivo esteja correto (`/FileStore/tables/access_logs.txt` no código).

3. **Execute o Código**:
   - Após configurar o arquivo, execute o notebook no Databricks.
   - Os resultados serão mostrados conforme as análises realizadas no código.

## Resultados Esperados

- **Contagem de Acessos ao Google**: Número de vezes que o domínio "google.com" foi acessado.
- **Top 5 Páginas Mais Acessadas**: Lista das 5 páginas mais visitadas.
- **Contagem de Acessos por Data**: Exibição das datas e quantidades de acessos.
- **Acessos por Tipo de IP**: Quantidade de acessos classificando os IPs como internos ou externos.

## Arquivos

- `access_logs.txt`: Arquivo de logs de acesso no formato TXT com informações de páginas e IPs acessados.

---

*Criado por Camille Pereira da Costa (https://github.com/camspereira)*



