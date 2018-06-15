import pandas as pd
import csv
import os
import errno
import time

#Dicionário com o relacionamento entre as tabelas do TPC-H:
tpch_relationship =  (  {'supplier.suppkey':0, 'partsupp.suppkey':1},
                        {'nation.nationkey':0, 'supplier.nationkey':3},
                        {'nation.nationkey':0, 'customer.nationkey':3},
                        {'customer.custkey':0, 'orders.custkey':1},
                        {'partsupp.partkey':0, 'lineitem.partkey':1},
                        {'partsupp.suppkey':1, 'lineitem.suppkey':2},
                        {'region.regionkey':0, 'nation.regionkey':2},
                        {'orders.orderkey':0, 'lineitem.orderkey':0},
                        {'part.partkey':0, 'partsupp.partkey':0} )

#

def adjust(id_col):
    '''
    Função para deixar a saída em bits do tamanho adequado.

    Caso queira alterar o tamanho da cadeia de bits, trocar todos os '12' presentes nessa
    função.
    '''
    b = ' '
    
    #Caso o  tamanho da numero binario for menor que 12
    if len(id_col) <= 12:
            b = 12-len(id_col)
            b ='{:0>12}'.format(id_col)
    #Caso o tamanho da numero binario for maior que 12
    if len(id_col) > 12:

            b = len(id_col) - 12
            b = id_col[b:]
    return b

#

def create_buckets(table_bigger, table_smaller, table_name='orders.csv'):
    '''
    Função que pega uma tabela no formato .csv e cria os seus respectivos buckets.

    '''

    with open(table_bigger) as csvfile:
        print("Start to build buckets...")
        
        read_table_bigger = csv.reader(csvfile)
        
        inicio = time.time()

        #Pegando qual a posição do atributo de junção a partir das tabelas 
        # que estão em Join:
        join_attribute_position_b, join_attribute_position_s = get_position_join_attribute(table_bigger=table_bigger, table_smaller=table_smaller)
        #join_attribute_position = get_position(table_bigger, table_smaller)


        #Para cada linha da leitura da tabela maior:
        for row in read_table_bigger:
            
            #Desconsiderando as linhas com esse atributo:
            if row[0] == 'id':
                continue
            

            #Pegando qual o atributo de junção da tabela para esse caso:
            join_attribute = row[join_attribute_position_b]

            #Levando o atributo de junção para o seu respectivo formato em binário:
            join_attribute_binary = "{0:b}".format(int(join_attribute))
            bucket_name = adjust(join_attribute_binary)
         
            
            #O endereço dos buckets com seus respectivos nomes:
            #Pegando o nome da tabela sem o '.csv'.
            bucket_subpath = table_name.split('.')[0]
            
            #Montando o caminho dos buckets:
            bucket_path = 'temp/{}/{}.csv'.format(bucket_subpath, bucket_name)
            
            #Se não existe o endereço desse bucket:
            if not os.path.exists( os.path.dirname(bucket_path) ):
                
                #Tentaremos criar:
                try:
                    os.makedirs(os.path.dirname(bucket_path))
                
                #Caso não tenha permissão para criar pasta/ arquivo:
                except OSError as exc: 
                    if exc.errno != errno.EEXIST:
                        raise
            #
            
            #Se foi criado com sucesso, e nenhuma exceção foi lançada, abriremos
            # esse bucket e começaremos a escrever:
            with open(bucket_path, 'a', encoding='utf-8') as filename:
                write = csv.writer(filename, lineterminator='\n')
                write.writerow(row) 
        #
    
    print("End of Create Buckets: %s sec --" % (time.time() - inicio))

#

def transform_tbl(table_name=None, path='TPCH/tpch-dbgen/'):
    '''
    Função para transformar um tabela em .tbl para .csv.

    '''

    #Caminho do .tbl:
    path = path + table_name

    #Pegando o nome da tabela:
    table_name = table_name.split('.')[0]

    #Abrindo o arquivo .tbl e o escrevendo para .csv:
    with open(path) as file:
        table = pd.read_table(file, sep='|', lineterminator='\n')
        result = open(table_name + '.csv','w')
        table.to_csv(result, index=False)

#

def exist_buckets(table_name=None):
    '''
    Função que checa se os Buckets para a 'table_name' já estão construídos.

    '''

    #Pegando o nome da tabela sem o '.csv'.
    bucket_subpath = table_name.split('.')[0]
    
    #Criando o caminho:
    bucket_path = os.path.dirname('temp/{}/'.format(bucket_subpath))

    #Retornando um boolean se existe esse caminho:
    return os.path.exists(bucket_path)

#

def table_sizes(table_1=None, table_2=None):
    '''
    Função para verificar qual tabela é maior entre as duas da entrada;

    Retorna a tabela maior e a tabela menor, respectivamente.
    
    '''

    try:
        #Verificando o tamanho em bytes das tabelas:
        table_1_size = os.stat(table_1).st_size
        table_2_size = os.stat(table_2).st_size
        
        #Verificando qual é a maior:
        if table_1_size > table_2_size:
            return table_1, table_2

        return table_2, table_1
        

    except Exception as error:
        raise error
    
#

def get_position_join_attribute(table_bigger, table_smaller):
    '''
    Lembro que, provavelmente, a tabela maior já estará com seus buckets criados. 
    
    Função retorna qual a posição de um atributo e sua FK para as tabelas dadas.
    
    '''

    #Pegando o nome sem o .csv:
    table_bigger = table_bigger.split('.')[0]
    table_smaller = table_smaller.split('.')[0]


    for fk in tpch_relationship:
        #No nosso caso, o fk é um dicionário armazenando a relação entre as tabelas:
        #print(fk)

        #Pegando as duas chaves do dicionário:
        fk_keys = list(fk.keys())
        #print(fk_keys)

        if table_bigger == fk_keys[0].split('.')[0]:
            if table_smaller == fk_keys[1].split('.')[0]:
                #print("First If: Success", fk[fk_keys[0]], fk[fk_keys[1]])
                
                return fk[fk_keys[0]], fk[fk_keys[1]]
        
        if table_bigger == fk_keys[1].split('.')[0]:
            if table_smaller == fk_keys[0].split('.')[0]:
                #print("Second If: Success", fk[fk_keys[1]], fk[fk_keys[0]])

                return fk[fk_keys[1]], fk[fk_keys[0]]
        
        
#

def match(table_smaller=None, table_bigger=None, attribute=None):
    
    inicio = time.time()

    #Abrindo o arquivo que contêm as tuplas: 
    with open(table_smaller) as csvfile:
        #count, maximum = 0,20
        print('Open smaller table: {}'.format(table_smaller))
        
        #Lendo todo o arquivo csv da tabela menor:
        read_table_smaller = csv.reader(csvfile)

        #Pegando qual a posição do atributo de junção a partir do relacionamento das
        # tabelas do tpch:
        join_attribute_position_b, join_attribute_position_s = get_position_join_attribute(table_bigger=table_bigger, table_smaller=table_smaller)

        #print("Position on bigger and smaller table: ", join_attribute_position_b, join_attribute_position_s)

        #Para cada linha da leitura da tabela menor:
        for row in read_table_smaller:
                       
            #Pegando o atributo de junção, à partir da linha, da tabela menor:
            join_attribute = row[join_attribute_position_s]  

            #Verificação de teste:
            if join_attribute == attribute:
                
                #Montando o atributo de junção em formato binário e com
                # a formatação adequada:
                join_attribute_binary = ("{0:b}".format(int(join_attribute)))
                join_attribute_binary = adjust(join_attribute_binary)

                #print("Attribute: ", join_attribute, " = Bucket { ", join_attribute_binary, " }")

                try:

                    #Montando o caminho de acesso ao bucket:
                    bucket_subpath = table_bigger.split('.')[0]
                    bucket_path = 'temp/{}/{}.csv'.format(bucket_subpath, join_attribute_binary)
                    
                    with open(bucket_path, 'r', encoding='utf-8') as bucket:
                        print('Entrando no Bucket: {}'.format(bucket_path))
                        
                        #Fazendo a leitura do CSV do Bucket e o colocando em memória,
                        # para poder ser iterado:
                        bucket = csv.reader(bucket)
                                                

                        #Percorrer cada linha do Bucket, procurando pelo atributo de junção:
                        row_count = 0

                        #Para cada linha do arquivo de bucket acessado:
                        for bucket_row in bucket:

                            #Pegando o atributo de junção do bucket:
                            join_attribute_bucket = bucket_row[join_attribute_position_b]

                            #Comparando com o atributo de junção da tabela menor:
                            if join_attribute == join_attribute_bucket:
                                '''
                                TODO
                                
                                + Adicionar essas informações em um arquivo de saída;
                                + Retirar a impressão do console;
                                '''
                                print(row_count, ' : ', row, " |", bucket_row)
                                print()

                            #Contador de linhas:    
                            row_count = row_count + 1
                    #
                
                except Exception:
                    print(Exception)

                    print("Caminho com Bucket não encontrado.")
                    return
                

    print("End of Match: %s sec --" % (time.time() - inicio))

#

def hash_join(table_1, table_2, join_attribute):
    '''
    Função de implementação do Hash Join.
    '''
      
    #Verificando qual a maior e qual a menor tabela:
    table_bigger, table_smaller = table_sizes(table_1=table_1, table_2=table_2)
    #print(table_smaller, table_bigger)

    #Se os buckets da tabela maior não estão construídos ainda, construí-los:
    if not exist_buckets(table_name=table_bigger):

        #Função de criação dos buckets:
        create_buckets(table_bigger=table_bigger, table_smaller=table_smaller)

    print("Start Hash Join...")

    #Fazendo o match dos valores:
    match(table_smaller=table_smaller, table_bigger=table_bigger, attribute=join_attribute)

#

