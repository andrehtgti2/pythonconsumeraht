import pika
import mysql.connector
#import MySQLdb # para o MySQL

connection = pika.BlockingConnection()
channel = connection.channel()



for method_frame, properties, body in channel.consume("LocacoesDiasRestantes"):    
    #print(method_frame, properties, body)
    print("Lendo da Queue LocacoesDiasRestantes:")
    print("Method_frame: ", method_frame)
    print("Properties: ", properties)
    print("Body: ", body)    
    channel.basic_ack(method_frame.delivery_tag)
    
    valor1 = body.decode("utf-8").split(",").pop(0)
    valor2 = body.decode("utf-8").split(",").pop(1)

    insert_lidos = ("INSERT INTO lidos_python "
               "(nome, dias) "
               "VALUES (%s, %s)")

    data_lidos = (valor1, valor2)

    cnx = mysql.connector.connect(user="root", password="admin", database="locadoraaht")
    cursor = cnx.cursor()

    print("Inserindo dados na tabela lidos_python: ", valor1, "|", valor2)
    cursor.execute(insert_lidos, data_lidos)

    cnx.commit()

    cursor.close()
    cnx.close()
    print("-------------------------------------------------")
    print("-------------------------------------------------")
    
    

    # if method_frame.delivery_tag == 10:
    #    break


requeued_messages = channel.cancel()
print('Requeued %i messages' % requeued_messages)
connection.close()
