🚀 FIAP - Data Engineering - Projeto Final 🚀

Este repositório contém o projeto final da disciplina de Data Engineering da FIAP. O objetivo é construir um pipeline de ingestão, transformação e persistência de dados utilizando PySpark, aplicando boas práticas de engenharia de dados.

💡 Arquitetura do Pipeline

O pipeline processa os dados de pagamentos (JSON) e pedidos (CSV), realiza transformações e persiste o resultado em formato Parquet para consumo posterior.
    

⚙️ Pré-requisitos

Para executar o projeto, você precisa ter instalados:

Python 3.9+

PySpark (versão compatível com o seu ambiente Spark)

pytest para rodar os testes

Instalação

Instale as dependências listadas no requirements.txt:
 pip install -r requirements.txt

▶️ Como Executar o Pipeline

Configuração: Revise e ajuste os paths e opções de arquivos no arquivo de configuração: src/configs/config.json.

Execução: Execute o script principal para rodar o pipeline de ingestão e transformação:
 spark-submit src/main.py

🧪 Rodando os Testes

Os testes unitários garantem que os métodos de leitura e escrita do data_handler.py funcionem corretamente, simulando as operações em arquivos temporários.

Execute os testes com o seguinte comando: pytest -v

👨‍💻 Autores

Projeto desenvolvido por:

Pedro Henrique Cozzati Camillo RM361284 

Thomaz Colalillo Navajas RM364869 

Marcela Bento do Vale RM361949 

Yasmin Martins Vasconcellos RM363354 

FIAP - Data Engineering - Projeto Final
