# WebScraper-Python-SQL-Azure

   Esse projeto tem objetivo apenas de aprendizado e faz coleta de dados em website especifico para coleta de dados 
via web-scraping.

   Meu objetivo era alcançar integração de varias ferramentas que hoje são essenciais na era Big Data.
    
   Tudo começa com a coleta de dados atraves da pratica de web-scraping, escrito em Python, de um site de livros 
(https://books.toscrape.com). Atraves do script faço a coleta dos nomes dos livros e das classificações atribuidas a cada um. 
Apos a obtenção das mesmas, faço a criação de duas listas, atraves das quais gero o Data Frame (DF). Se houvesse um grande 
volume de informações essa seria a hora de fazer a manipulação e enriquecimento dos dados antes da exportação. 

   Antes de iniciar o processo de exportação eu crio a sessão PySpark, que é uma interface alto nível que me permite acessar 
e usar o Spark por meio da linguagem Python. No processo de exportação dos dados eu inicialmente gero o DF em formato Spark  
e depois gravo a tabela no formato Delta, que é uma camada de armazenamento de software livre que traz confiabilidade para 
os data lakes. O Delta Lake fornece transações ACID, tratamento de metadados escalonáveis e unifica o processamento de dados 
de lote e streaming. 

   Apos a exportação para o Delta Lake, faço o upload do arquivo para o meu Azure Blob Storage e, na sequencia, insiro os dados 
no meu SQL Server On Premise (SQL Local), tudo diretamente do Python, demonstrando a integração entre as ferramentas.
A segunda parte do projeto corre no ambiente de Cloud Computing, onde utilizo o Azure Data Factory para fazer uma pesquisa nos 
meus dados do SQL Local com uma iteração condicional sobre a data dos dados. Quando certa condição é atendida, uma stored 
procedure é ativada, gerando um arquivo com os livro mais bem recomendados. Então o Data Factory salva essa recondação no meu 
Datalake e, atraves de um Logic App, me envia essa recomendação por e-mail.  
