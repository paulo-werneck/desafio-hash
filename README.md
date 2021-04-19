<h2> Desafio Hash </h2>

Repositório com soluções propostas para o desafio de engenheiro de dados

<br>

<h3>Tecnologias utilizadas: </h3>
* Linguagem: Python 3.7
* Framework: Spark 3.0.1


<h3>Tratamentos aplicados: </h3>

- padronizar a nomenclatura dos campos (headers) em minusculo;
- converter o campo "created_at" para timestamp;
- converter o campo "valor" para decimal;
- padronizar o campo "status";
- remover caracteres especiais do campo "documento";
- converter o campo "card_id" para bigint;
- converter o campo "card_brand" para int;
- padronizar o campo "nome_no_cartao";
- criação do campo "valor_por_parcela";

As conversões foram feitas a partir do arquivo types_mapping.json


<h3>Visões criadas: </h3>

- script: view_amount_per_brand.py
  - Montante (valor) transacionado por bandeira e status
    

- script: view_amount_per_iso_merchant.py
    - Montante (valor) transacionado pago por ISO / merchant, por mes